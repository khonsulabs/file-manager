use crate::{fs::StdFileManager, memory::MemoryFileManager};
use crate::{FileManager, OpenOptions, PathId};

use std::io::{Read, Write};
use std::path::Path;

macro_rules! test_all_managers {
    ($fn_name:ident) => {{
        $fn_name(MemoryFileManager::default(), Path::new("/"));
        let dir = tempfile::tempdir().unwrap();
        $fn_name(StdFileManager::default(), dir.path())
    }};
}

#[test]
fn create_read_delete_file() {
    fn test<M: FileManager>(manager: M, path: &Path) {
        let file_path = PathId::from(path.join("a-file"));
        assert!(manager
            .open(&file_path, OpenOptions::new().read(true))
            .is_err());

        let mut file = manager
            .open(
                &file_path,
                OpenOptions::new().read(true).write(true).create(true),
            )
            .unwrap();
        let mut reader = manager
            .open(&file_path, OpenOptions::new().read(true))
            .unwrap();
        file.write_all(b"hello world").unwrap();
        let mut contents = Vec::new();
        reader.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"hello world");

        manager.remove_file(&file_path).unwrap();

        assert!(manager
            .open(&file_path, OpenOptions::new().read(true))
            .is_err());
    }
    test_all_managers!(test);
}
