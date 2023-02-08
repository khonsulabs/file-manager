use std::error::Error;
use std::fmt::Display;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, available_parallelism, JoinHandle};

use flume::{Receiver, Sender};

use crate::FileManager;

#[derive(Debug)]
pub struct FSyncManager<M>
where
    M: FileManager,
{
    data: Arc<Mutex<ThreadState<M::File>>>,
    _manager: PhantomData<M>,
}

impl<M> FSyncManager<M>
where
    M: FileManager,
{
    pub fn new(maximum_threads: usize) -> Self {
        Self {
            data: Arc::new(Mutex::new(ThreadState::Uninitialized { maximum_threads })),
            _manager: PhantomData,
        }
    }

    pub fn shutdown(&self) -> Result<(), FSyncError> {
        let mut data = self.data.lock()?;
        if let ThreadState::Running(ManagerThread {
            handle,
            command_sender,
        }) = std::mem::replace(&mut *data, ThreadState::Shutdown)
        {
            drop(command_sender);
            handle.join().map_err(|_| FSyncError::ThreadJoin)??;
        }
        Ok(())
    }

    fn with_running_thread<R>(
        &self,
        cb: impl FnOnce(&mut ManagerThread<M::File>) -> R,
    ) -> Result<R, FSyncError> {
        let mut data = self.data.lock()?;
        match &*data {
            ThreadState::Uninitialized { maximum_threads } => {
                let (command_sender, command_receiver) = flume::unbounded();
                let threads_to_spawn = maximum_threads.checked_sub(1).unwrap_or_default();
                let handle = thread::Builder::new()
                    .name(String::from("sediment-sync"))
                    .spawn(move || fsync_thread(command_receiver, threads_to_spawn))?;
                *data = ThreadState::Running(ManagerThread {
                    handle,
                    command_sender,
                });
            }
            ThreadState::Shutdown => return Err(FSyncError::Shutdown),
            ThreadState::Running(_) => {}
        }

        let ThreadState::Running(thread) = &mut *data else { unreachable!("initialized above")};
        Ok(cb(thread))
    }

    pub fn new_batch(&self) -> Result<FSyncBatch<M>, FSyncError> {
        let notify = Arc::new(FSyncNotify {
            remaining: Mutex::new(0),
            sync: Condvar::new(),
        });

        Ok(FSyncBatch {
            command_sender: self.with_running_thread(|t| t.command_sender.clone())?,
            notify,
            _manager: PhantomData,
        })
    }
}
impl<M> Clone for FSyncManager<M>
where
    M: FileManager,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _manager: PhantomData,
        }
    }
}

impl<M> Default for FSyncManager<M>
where
    M: FileManager,
{
    fn default() -> Self {
        Self::new(available_parallelism().map_or(4, |nonzero| nonzero.get()))
    }
}

enum SpawnStatus {
    CanSpawn,
    Spawned(JoinHandle<Result<(), FSyncError>>),
    AtLimit,
}

fn fsync_thread<F>(
    command_receiver: Receiver<FSync<F>>,
    threads_to_spawn: usize,
) -> Result<(), FSyncError>
where
    F: crate::File,
{
    let mut spawn_status = if threads_to_spawn > 0 {
        SpawnStatus::CanSpawn
    } else {
        SpawnStatus::AtLimit
    };

    while let Ok(fsync) = command_receiver.recv() {
        // Check if we should spawn a thread.
        if matches!(spawn_status, SpawnStatus::CanSpawn) && !command_receiver.is_empty() {
            let command_receiver = command_receiver.clone();
            let handle = thread::Builder::new()
                .name(String::from("sediment-sync"))
                .spawn(move || fsync_thread(command_receiver, threads_to_spawn))?;
            spawn_status = SpawnStatus::Spawned(handle);
        }

        if fsync.all {
            fsync.file.sync_all()?;
        } else {
            fsync.file.sync_data()?;
        }

        let mut remaining_syncs = fsync.notify.remaining.lock()?;
        *remaining_syncs -= 1;
        drop(remaining_syncs);
        fsync.notify.sync.notify_one();
    }

    if let SpawnStatus::Spawned(handle) = spawn_status {
        handle.join().map_err(|_| FSyncError::ThreadJoin)??;
    }

    Ok(())
}

#[derive(Debug)]
enum ThreadState<F> {
    Uninitialized { maximum_threads: usize },
    Running(ManagerThread<F>),
    Shutdown,
}

#[derive(Debug)]
struct ManagerThread<F> {
    handle: JoinHandle<Result<(), FSyncError>>,
    command_sender: Sender<FSync<F>>,
}

struct FSync<F> {
    all: bool,
    file: F,
    notify: Arc<FSyncNotify>,
}

#[derive(Debug)]
pub struct FSyncBatch<M>
where
    M: FileManager,
{
    command_sender: Sender<FSync<M::File>>,
    notify: Arc<FSyncNotify>,
    _manager: PhantomData<M>,
}

impl<M> FSyncBatch<M>
where
    M: FileManager,
{
    pub fn queue_fsync_all(&self, file: M::File) -> Result<(), FSyncError> {
        let mut remaining_syncs = self.notify.remaining.lock()?;
        *remaining_syncs += 1;
        drop(remaining_syncs);

        self.command_sender
            .send(FSync {
                all: true,
                file,
                notify: self.notify.clone(),
            })
            .map_err(|_| FSyncError::Shutdown)?;

        Ok(())
    }

    pub fn queue_fsync_data(&self, file: M::File) -> Result<(), FSyncError> {
        let mut remaining_syncs = self.notify.remaining.lock()?;
        *remaining_syncs += 1;
        drop(remaining_syncs);

        self.command_sender
            .send(FSync {
                all: false,
                file,
                notify: self.notify.clone(),
            })
            .map_err(|_| FSyncError::Shutdown)?;

        Ok(())
    }

    pub fn wait_all(self) -> Result<(), FSyncError> {
        let mut remaining_syncs = self.notify.remaining.lock()?;

        while *remaining_syncs > 0 {
            remaining_syncs = self.notify.sync.wait(remaining_syncs)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FSyncNotify {
    remaining: Mutex<usize>,
    sync: Condvar,
}

#[derive(Debug)]
pub enum FSyncError {
    Shutdown,
    ThreadJoin,
    InternalInconstency,
    Io(io::Error),
}

impl Error for FSyncError {}

impl Display for FSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FSyncError::Shutdown => f.write_str("fsync manager is not running"),
            FSyncError::ThreadJoin => f.write_str("error joining an fsync thread"),
            FSyncError::InternalInconstency => f.write_str("fsync manager mutex poisoned"),
            FSyncError::Io(io) => write!(f, "io error: {io}"),
        }
    }
}

impl From<io::Error> for FSyncError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl<T> From<PoisonError<T>> for FSyncError {
    fn from(_: PoisonError<T>) -> Self {
        Self::InternalInconstency
    }
}

impl From<FSyncError> for io::Error {
    fn from(error: FSyncError) -> Self {
        match error {
            FSyncError::Io(io) => io,
            other => Self::new(io::ErrorKind::Other, other),
        }
    }
}
