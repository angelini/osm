use std::fs;
use std::io;
use std::path::PathBuf;

use crate::base::{ObjectKey, Partition, ToStdPath};
use crate::path::{DatasetPath, ObjectPath, PartitionPath};

#[derive(Debug)]
pub enum StoreError {
    Io(io::Error),
}

impl From<io::Error> for StoreError {
    fn from(error: io::Error) -> StoreError {
        StoreError::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub trait Store {
    fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<()>;
    fn list_partitions(&self, path: &DatasetPath) -> Result<Vec<Partition>>;
    fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectKey>>;
    fn remove_partition(&self, path: &PartitionPath) -> Result<()>;
}

pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub fn new(root: PathBuf) -> Self {
        FileStore { root }
    }

    fn fs_path(&self, path: PathBuf) -> PathBuf {
        let mut buf = self.root.clone();
        buf.push(path);
        buf
    }
}

impl Store for FileStore {
    fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<()> {
        let fs_source = self.fs_path(source.std_path());
        let fs_target = self.fs_path(target.std_path());
        let fs_target_part = self.fs_path(target.partition_path().std_path());

        fs::create_dir_all(fs_target_part)?;
        Ok(fs::rename(fs_source, fs_target)?)
    }

    fn list_partitions(&self, path: &DatasetPath) -> Result<Vec<Partition>> {
        let fs_path = self.fs_path(path.std_path());

        if !fs_path.is_dir() {
            return Err(StoreError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "not a directory",
            )));
        }

        //FIXME: Support depth > 1

        fs::read_dir(self.fs_path(path.std_path()))?
            .map(|dir_entry| {
                let path = dir_entry?.path();
                let file_name = match path.file_name() {
                    Some(f) => f.to_string_lossy().to_string(),
                    None => {
                        return Err(StoreError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid partition name",
                        )))
                    }
                };

                match (file_name.find('='), file_name.ends_with('=')) {
                    (Some(idx), false) => Ok(Partition::new(
                        file_name[0..idx].to_string(),
                        file_name[idx + 1..].to_string(),
                    )),
                    _ => Err(StoreError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid partition name",
                    ))),
                }
            })
            .collect()
    }

    fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectKey>> {
        let fs_path = self.fs_path(path.std_path());

        if !fs_path.is_dir() {
            return Err(StoreError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "not a directory",
            )));
        }

        fs::read_dir(fs_path)?
            .map(|dir_entry| match dir_entry?.path().file_name() {
                Some(f) => Ok(ObjectKey::from_os_str(f)),
                None => Err(StoreError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid partition name",
                ))),
            })
            .collect()
    }

    fn remove_partition(&self, path: &PartitionPath) -> Result<()> {
        Ok(fs::remove_dir(self.fs_path(path.std_path()))?)
    }
}
