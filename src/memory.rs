use std::cmp::Ordering;
use std::collections::{hash_map, HashMap, HashSet};
use std::io::{self, Read, Seek, Write};
use std::num::TryFromIntError;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::fsync::FSyncManager;
use crate::{File, FileManager, OpenOptions, PathId};

#[derive(Clone, Debug)]
pub struct MemoryFileManager {
    /// The directory structure. If both directories and files need to be
    /// locked, lock directories first.
    directories: Arc<Mutex<HashMap<PathId, HashSet<PathId>>>>,
    /// Every file and directory known to this virtual file system.
    files: Arc<RwLock<HashMap<PathId, MemoryFile>>>,
    fsyncs: FSyncManager<Self>,
}

impl Default for MemoryFileManager {
    fn default() -> Self {
        let root = PathId::from(Path::new("/"));
        Self {
            files: Arc::new(RwLock::new(
                [(root.clone(), MemoryFile::new_directory(root.clone()))]
                    .into_iter()
                    .collect(),
            )),
            directories: Arc::new(Mutex::new([(root, HashSet::new())].into_iter().collect())),
            fsyncs: FSyncManager::default(),
        }
    }
}

impl FileManager for MemoryFileManager {
    type File = MemoryFile;

    fn open(&self, path: &PathId, options: OpenOptions) -> std::io::Result<Self::File> {
        check_path(path)?;
        let files = self.files.read().map_err(ToIo::to_io)?;
        if let Some(file) = files.get(path).map(MemoryFile::detach) {
            // TODO restrict from writing to a read-only file?
            Ok(file)
        } else if options.create {
            let Some(parent) = path.parent()
                else { unreachable!("/ is handled in the above condition, and all other paths return a parent") };

            // The file wasn't found, but we have the create flag. We need to
            // add the file to both files and directories, but to get files with
            // write permission, we must drop. This introduces a race condition,
            // which means we much check for the file again after reaquiring.
            let mut directories = self.directories.lock().map_err(ToIo::to_io)?;
            if let Some(parent) = directories.get_mut(&parent) {
                // The parent directory exists, so let's create the file.
                drop(files);
                let mut files = self.files.write().map_err(ToIo::to_io)?;
                match files.entry(path.clone()) {
                    // Another thread already created the file
                    hash_map::Entry::Occupied(file) => Ok(file.get().detach()),
                    hash_map::Entry::Vacant(empty) => {
                        // Record the directory entry.
                        parent.insert(path.clone());
                        // Create the file
                        Ok(empty.insert(MemoryFile::new(path.clone())).detach())
                    }
                }
            } else {
                Err(io::Error::from(io::ErrorKind::NotFound))
            }
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn exists(&self, path: &PathId) -> bool {
        if let Ok(files) = self.files.read() {
            return files.contains_key(path);
        }

        false
    }

    fn create_dir_all(&self, path: &PathId) -> std::io::Result<()> {
        check_path(path)?;
        let mut directories = self.directories.lock().map_err(ToIo::to_io)?;
        let mut files = self.files.write().map_err(ToIo::to_io)?;

        match files.get(path) {
            Some(file) if matches!(file.backing, FileBacking::Buffer { .. }) => Err(
                io::Error::new(io::ErrorKind::Unsupported, "path exists as a file"),
            ),
            // The directory already exists
            Some(_) => Ok(()),
            None => {
                // Find the first path along this path that exists. We need to
                // ensure that it's a directory, not a file.
                let mut paths_to_create = Vec::new();
                let Some(mut path_to_check) = path.parent() else { unreachable!("/ always exists in files") };
                loop {
                    match files.get(&path_to_check) {
                        Some(file) if matches!(file.backing, FileBacking::Directory) => break,
                        Some(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                        None => {
                            let Some(next_root) = path_to_check.parent() else { unreachable!("/ always is in files") };
                            paths_to_create.push(path_to_check);
                            path_to_check = next_root;
                        }
                    }
                }

                // We get here only if we fine a non-file directory that exists.
                // That means we can now create all of the directory entries
                // requested.
                let mut paths_to_create = paths_to_create.into_iter().peekable();
                while let Some(path_to_create) = paths_to_create.next() {
                    files.insert(
                        path_to_create.clone(),
                        MemoryFile::new_directory(path_to_create.clone()),
                    );

                    let mut files = HashSet::new();
                    if let Some(next_file) = paths_to_create.peek().cloned() {
                        files.insert(next_file);
                    }
                    directories.insert(path_to_create, files);
                }

                Ok(())
            }
        }
    }

    fn remove_dir_all(&self, path: &PathId) -> std::io::Result<()> {
        check_path(path)?;
        let mut directories = self.directories.lock().map_err(ToIo::to_io)?;
        let mut files = self.files.write().map_err(ToIo::to_io)?;

        if &**path == Path::new("/") {
            // No need to scan the structures when we're removing everything.
            directories.clear();
            directories.insert(path.clone(), HashSet::new());
            files.clear();
            files.insert(path.clone(), MemoryFile::new_directory(path.clone()));
        } else {
            let mut directories_to_scan = vec![path.clone()];
            while let Some(directory) = directories_to_scan.pop() {
                let Some(directory_files) = directories.remove(&directory)
                    else { return Err(io::Error::from(io::ErrorKind::NotFound)) };
                for file in directory_files {
                    let Some(file) = files.remove(&file) else { unreachable!("file missing") };
                    if let FileBacking::Directory = file.backing {
                        // This file was a directory itself. We need to remove its
                        // contents as well.
                        directories_to_scan.push(file.path);
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_file(&self, path: &PathId) -> std::io::Result<()> {
        check_path(path)?;
        let mut directories = self.directories.lock().map_err(ToIo::to_io)?;
        let mut files = self.files.write().map_err(ToIo::to_io)?;
        if let Some(parent) = path.parent() {
            if files.remove(path).is_some() {
                directories
                    .get_mut(&parent)
                    .expect("file exists without directory")
                    .remove(path);
                Ok(())
            } else {
                Err(io::Error::from(io::ErrorKind::NotFound))
            }
        } else {
            // This is a request to remove /, which is a directory.
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }

    fn new_fsync_batch(&self) -> std::io::Result<crate::FSyncBatch<Self>> {
        Ok(self.fsyncs.new_batch()?)
    }

    fn shutdown(&self) -> std::io::Result<()> {
        self.fsyncs.shutdown()?;
        Ok(())
    }

    fn list(&self, path: &PathId) -> io::Result<Vec<PathId>> {
        let directories = self.directories.lock().map_err(ToIo::to_io)?;
        if let Some(contents) = directories.get(path) {
            Ok(dbg!(contents.iter().cloned().collect()))
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn rename(&self, from: &PathId, to: PathId) -> io::Result<()> {
        check_path(from)?;
        check_path(&to)?;

        let Some(from_parent) = from.parent()
            else {
                return Err(io::Error::from(io::ErrorKind::Unsupported))
            };

        let mut directories = self.directories.lock().map_err(ToIo::to_io)?;
        let mut files = self.files.write().map_err(ToIo::to_io)?;
        if let Some(mut original) = files.remove(from) {
            original.path = to.clone();
            let directory = directories
                .get_mut(&from_parent)
                .expect("missing directory");
            directory.remove(from);
            directory.insert(to.clone());
            files.insert(to, original);

            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemoryFile {
    path: PathId,
    backing: FileBacking,
}

impl MemoryFile {
    pub fn new(path: PathId) -> Self {
        Self {
            path,
            backing: FileBacking::Buffer {
                buffer: Arc::default(),
                position: Arc::default(),
            },
        }
    }

    pub fn new_directory(path: PathId) -> Self {
        Self {
            path,
            backing: FileBacking::Directory,
        }
    }

    fn detach(&self) -> Self {
        Self {
            path: self.path.clone(),
            backing: match &self.backing {
                FileBacking::Directory => FileBacking::Directory,
                FileBacking::Buffer { buffer, .. } => FileBacking::Buffer {
                    position: Arc::default(),
                    buffer: buffer.clone(),
                },
            },
        }
    }
}

impl File for MemoryFile {
    type Manager = MemoryFileManager;

    fn path(&self) -> &PathId {
        &self.path
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        match &self.backing {
            FileBacking::Directory => Ok(0),
            FileBacking::Buffer { buffer, .. } => {
                let buffer = buffer.read().map_err(PoisonError::to_io)?;
                Ok(buffer.len() as u64)
            }
        }
    }

    fn set_len(&self, new_length: u64) -> io::Result<()> {
        match &self.backing {
            FileBacking::Directory => Err(io::Error::from(io::ErrorKind::Unsupported)),
            FileBacking::Buffer { buffer, position } => {
                let mut buffer = buffer.write().map_err(PoisonError::to_io)?;
                let new_length = new_length.try_into().map_err(ToIo::to_io)?;
                buffer.resize(new_length, 0);
                drop(buffer);
                let mut position = position.lock().map_err(PoisonError::to_io)?;
                if *position > new_length {
                    *position = new_length;
                }
                Ok(())
            }
        }
    }

    fn try_clone(&self) -> io::Result<Self> {
        Ok(self.clone())
    }
}

impl Read for MemoryFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &self.backing {
            FileBacking::Directory => Err(io::Error::from(io::ErrorKind::Unsupported)),
            FileBacking::Buffer { buffer, position } => {
                let mut position = position.lock().map_err(PoisonError::to_io)?;
                let buffer = buffer.read().map_err(PoisonError::to_io)?;

                if let Some(bytes_available) = buffer.len().checked_sub(*position) {
                    let bytes_to_read = bytes_available.min(buf.len());
                    let read_end = *position + bytes_to_read;
                    buf[..bytes_to_read].copy_from_slice(&buffer[*position..read_end]);
                    *position = read_end;
                    Ok(bytes_to_read)
                } else {
                    Ok(0)
                }
            }
        }
    }
}

impl Write for MemoryFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match &self.backing {
            FileBacking::Directory => Err(io::Error::from(io::ErrorKind::Unsupported)),
            FileBacking::Buffer { buffer, position } => {
                let mut position = position.lock().map_err(PoisonError::to_io)?;
                let mut buffer = buffer.write().map_err(PoisonError::to_io)?;
                let buffer_length = buffer.len();

                match position.cmp(&buffer_length) {
                    Ordering::Greater => {
                        // Writing beyond the end of the file, fill with 0s.
                        buffer.resize(*position, 0);
                        buffer.extend_from_slice(buf);
                        *position = buffer.len();
                        Ok(buf.len())
                    }
                    Ordering::Equal => {
                        // Writing at the end of the file, but no neeed to fill.
                        buffer.extend_from_slice(buf);
                        *position = buffer.len();
                        Ok(buf.len())
                    }
                    Ordering::Less => {
                        // Writing inside of the file.
                        let bytes_from_end = buffer_length - *position;
                        if bytes_from_end > 0 {
                            // Fill the buffer.
                            let bytes_to_write = bytes_from_end.min(buf.len());
                            let write_end = bytes_to_write + *position;
                            buffer[*position..write_end].copy_from_slice(&buf[..bytes_to_write]);
                            *position = write_end;
                            Ok(bytes_to_write)
                        } else {
                            buffer.extend_from_slice(buf);
                            *position += buf.len();
                            Ok(buf.len())
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for MemoryFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match &self.backing {
            FileBacking::Directory => Err(io::Error::from(io::ErrorKind::Unsupported)),
            FileBacking::Buffer { buffer, position } => {
                let mut position = position.lock().map_err(PoisonError::to_io)?;
                let new_position = match pos {
                    io::SeekFrom::Start(offset) => offset,
                    io::SeekFrom::End(from_end) => {
                        let buffer = buffer.read().map_err(PoisonError::to_io)?;
                        let current_length = buffer.len() as u64;
                        if from_end > 0 {
                            current_length + from_end as u64
                        } else if from_end == i64::MIN {
                            0
                        } else {
                            current_length - ((-from_end) as u64)
                        }
                    }
                    io::SeekFrom::Current(offset) => {
                        let current = *position as u64;
                        if offset > 0 {
                            current + offset as u64
                        } else if offset == i64::MIN {
                            0
                        } else {
                            current - ((-offset) as u64)
                        }
                    }
                };
                let as_usize = new_position.try_into().map_err(TryFromIntError::to_io)?;
                *position = as_usize;
                Ok(new_position)
            }
        }
    }
}

#[derive(Clone, Debug)]
enum FileBacking {
    Directory,
    Buffer {
        /// The position within the file's buffer. Always lock this before the
        /// buffer if both need to be locked.
        position: Arc<Mutex<usize>>,
        /// The file's buffer. Always lock this after the position, if both need
        /// to be locked.
        buffer: Arc<RwLock<Vec<u8>>>,
    },
}

trait ToIo {
    fn to_io(self) -> io::Error;
}

impl<T> ToIo for PoisonError<T> {
    fn to_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::Other, "lock poisoned")
    }
}

impl ToIo for TryFromIntError {
    fn to_io(self) -> io::Error {
        io::Error::new(
            io::ErrorKind::Other,
            "position too large for current platform",
        )
    }
}

fn check_path(path: &PathId) -> io::Result<()> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "memory file manager requires absolute paths ",
        ))
    }
}

#[test]
fn check_path_tests() {
    let absolute = PathId::from("/a-file");
    let relative = PathId::from("a-file");
    assert!(check_path(&absolute).is_ok());
    assert!(check_path(&relative).is_err());
}
