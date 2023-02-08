use crate::{fs::StdFileManager, memory::MemoryFileManager};
use crate::{FileManager, OpenOptions, PathId};

use std::io::{Read, Write};
use std::path::Path;

fn create_read_delete_file<M: FileManager>(manager: M, path: &Path) {
    let file_path = PathId::from(path.join("a-file"));
    assert!(manager
        .open(&file_path, OpenOptions::new().read(true))
        .is_err());
    assert!(!manager.exists(&file_path));

    let mut file = manager
        .open(
            &file_path,
            OpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap();
    assert!(manager.exists(&file_path));
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

#[test]
fn create_read_delete_file_memory() {
    create_read_delete_file(MemoryFileManager::default(), Path::new("/"));
}

#[test]
fn create_read_delete_file_std() {
    let dir = tempfile::tempdir().unwrap();
    create_read_delete_file(StdFileManager::default(), dir.path());
}

fn create_dir_all<M: FileManager>(manager: M, path: &Path) {
    let path = PathId::from(path);
    let file_path = PathId::from(path.join("a-file"));

    // create a-file, which will be used to create an error.
    manager
        .open(
            &file_path,
            OpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap();

    // Creating the path we're given shouldn't do anything -- it already exists.
    manager.create_dir_all(&path).unwrap();
    // Trying to create against our file should be an error.
    manager.create_dir_all(&file_path).unwrap_err();
    // Creating a/b should work,
    let a = PathId::from(path.join("a"));
    let a_b = PathId::from(a.join("b"));
    manager.create_dir_all(&a_b).unwrap();
    assert!(manager.exists(&a));
    assert!(manager.exists(&a_b));
    // And we should be able to create a file within a/b.
    let sub_file = PathId::from(a_b.join("file"));
    manager
        .open(
            &sub_file,
            OpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap();

    // We shouldn't be able to use create_dir_all on top of our file
    manager
        .open(
            &PathId::from(file_path.join("bad-idea")),
            OpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap_err();

    // Finally, delete it all.
    manager.remove_dir_all(&path).unwrap();
    // Make sure nothing seems to exist still.
    assert!(!manager.exists(&file_path));
    assert!(!manager.exists(&a));
    assert!(!manager.exists(&a_b));
    assert!(!manager.exists(&sub_file));
}

#[test]
fn create_dir_all_memory() {
    create_dir_all(MemoryFileManager::default(), Path::new("/"));
}

#[test]
fn create_dir_all_std() {
    let dir = tempfile::tempdir().unwrap();
    create_dir_all(StdFileManager::default(), dir.path());
}
