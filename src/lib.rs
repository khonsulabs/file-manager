pub mod fs;
mod fsync;
pub mod memory;
pub use fsync::{FSyncBatch, FSyncError};

use std::fmt::Debug;
use std::io::{self, Read, Seek, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use interner::global::{GlobalPath, GlobalPool};

static PATH_IDS: GlobalPool<PathBuf> = GlobalPool::new();

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct PathId(GlobalPath);

impl<'a> From<&'a str> for PathId {
    fn from(path: &'a str) -> Self {
        Self::from(Path::new(path))
    }
}

impl<'a> From<&'a Path> for PathId {
    fn from(path: &'a Path) -> Self {
        Self(PATH_IDS.get(path))
    }
}

impl From<PathBuf> for PathId {
    fn from(value: PathBuf) -> Self {
        Self(PATH_IDS.get(value))
    }
}

impl PathId {
    pub fn parent(&self) -> Option<Self> {
        self.0.parent().map(Self::from)
    }
}

impl Deref for PathId {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait FileManager: Debug + Clone + Send + Sync + 'static {
    type File: File<Manager = Self>;

    fn open(&self, path: &PathId, options: OpenOptions) -> io::Result<Self::File>;
    fn exists(&self, path: &PathId) -> bool;
    fn create_dir_all(&self, path: &PathId) -> io::Result<()>;
    fn remove_dir_all(&self, path: &PathId) -> io::Result<()>;
    fn remove_file(&self, path: &PathId) -> io::Result<()>;
    fn rename(&self, from: &PathId, to: PathId) -> io::Result<()>;
    fn sync_data(&self, path: &PathId) -> io::Result<()> {
        self.open(path, OpenOptions::new().read(true))?.sync_data()
    }
    fn sync_all(&self, path: &PathId) -> io::Result<()> {
        self.open(path, OpenOptions::new().read(true))?.sync_all()
    }
    fn new_fsync_batch(&self) -> io::Result<FSyncBatch<Self>>;
    fn shutdown(&self) -> io::Result<()>;
    fn list(&self, path: &PathId) -> io::Result<Vec<PathId>>;
}

pub trait File: Sized + Debug + Write + Read + Seek + Send + Sync + 'static {
    type Manager: FileManager<File = Self>;

    fn path(&self) -> &PathId;
    fn sync_all(&self) -> io::Result<()>;
    fn sync_data(&self) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }
    fn set_len(&self, new_length: u64) -> io::Result<()>;
    fn try_clone(&self) -> io::Result<Self>;
}

pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
}

impl OpenOptions {
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
        }
    }

    pub const fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    pub const fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    pub const fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    pub fn into_std(self) -> std::fs::OpenOptions {
        let mut options = std::fs::OpenOptions::new();

        if self.read {
            options.read(true);
        }

        if self.write {
            options.write(true);
        }

        if self.create {
            options.create(true);
        }

        options
    }
}

#[cfg(test)]
mod tests;
