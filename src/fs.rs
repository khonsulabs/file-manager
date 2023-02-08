use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};

use crate::fsync::FSyncManager;
use crate::{FileManager, OpenOptions, PathId};

#[derive(Clone, Debug, Default)]
pub struct StdFileManager {
    fsyncs: FSyncManager<Self>,
}

impl FileManager for StdFileManager {
    type File = StdFile;

    fn open(&self, path: &PathId, options: OpenOptions) -> io::Result<Self::File> {
        options.into_std().open(&**path).map(|file| StdFile {
            file,
            path: path.clone(),
        })
    }

    fn exists(&self, path: &PathId) -> bool {
        path.exists()
    }

    fn create_dir_all(&self, path: &PathId) -> io::Result<()> {
        std::fs::create_dir_all(&**path)
    }

    fn remove_dir_all(&self, path: &PathId) -> io::Result<()> {
        std::fs::remove_dir_all(&**path)
    }

    fn remove_file(&self, path: &PathId) -> io::Result<()> {
        std::fs::remove_file(&**path)
    }

    fn new_fsync_batch(&self) -> io::Result<crate::FSyncBatch<Self>> {
        Ok(self.fsyncs.new_batch()?)
    }

    fn shutdown(&self) -> io::Result<()> {
        self.fsyncs.shutdown()?;

        Ok(())
    }

    fn list(&self, path: &PathId) -> io::Result<Vec<PathId>> {
        let mut files = Vec::new();
        for file in fs::read_dir(&**path)? {
            let file = file?;
            files.push(PathId::from(file.path()));
        }
        Ok(files)
    }

    fn rename(&self, from: &PathId, to: PathId) -> io::Result<()> {
        fs::rename(&**from, &*to)
    }
}

#[derive(Debug)]
pub struct StdFile {
    file: File,
    path: PathId,
}

impl crate::File for StdFile {
    type Manager = StdFileManager;

    fn path(&self) -> &PathId {
        &self.path
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, new_length: u64) -> io::Result<()> {
        self.file.set_len(new_length)
    }

    fn try_clone(&self) -> io::Result<Self> {
        self.file.try_clone().map(|file| Self {
            file,
            path: self.path.clone(),
        })
    }
}

impl Read for StdFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Write for StdFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for StdFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
