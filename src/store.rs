use std::fs;
use std::io;
use std::path::PathBuf;

use anyhow::{Context, Error, Result};
use parquet::errors::ParquetError;
use thiserror::Error;

use crate::base::{Bytes, Format, ObjectKey, Partition, ToStdPath};
use crate::parquet::{combine_objects, read_object_state};
use crate::path::{DatasetPath, ObjectPath, PartitionPath};
use crate::state::ObjectState;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("IO: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Parquet(#[from] ParquetError),

    #[error("Cannot infer schema from path: {0}")]
    CannotInferSchema(ObjectPath),

    #[error("Invalid partition name: {0}")]
    InvalidPartition(String),
}

fn as_err<T, E: Into<StoreError>>(error: E) -> Result<T> {
    Err(Error::new(error.into()))
}

pub trait Store {
    fn read_object(&self, path: &ObjectPath) -> Result<ObjectState>;
    fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<()>;
    fn list_partitions(&self, path: &DatasetPath) -> Result<Vec<Partition>>;
    fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectKey>>;
    fn remove_partition(&self, path: &PartitionPath) -> Result<()>;
    fn remove_object(&self, path: &ObjectPath) -> Result<()>;
    fn rebalance_objects(
        &self,
        input_paths: &[ObjectPath],
        output_paths: &[ObjectPath],
        rows_per_file: usize,
    ) -> Result<Vec<ObjectState>>;
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
    fn read_object(&self, path: &ObjectPath) -> Result<ObjectState> {
        let fs_path = self.fs_path(path.std_path());
        let file = fs::File::open(fs_path)?;

        let state = match path.infer_format() {
            Some(Format::Csv) => ObjectState::new_csv(0, Bytes::new(0)),
            Some(Format::Parquet) => read_object_state(&file)?,
            None => return as_err(StoreError::CannotInferSchema(path.clone())),
        };

        Ok(state)
    }

    fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<()> {
        let fs_source = self.fs_path(source.std_path());
        let fs_target = self.fs_path(target.std_path());
        let fs_target_part = self.fs_path(target.partition_path().std_path());

        fs::create_dir_all(fs_target_part)
            .with_context(|| format!("cannot create partition: {}", target.partition_path()))?;
        fs::rename(fs_source, fs_target)
            .with_context(|| format!("cannot rename {} to {}", source, target))?;
        Ok(())
    }

    fn list_partitions(&self, path: &DatasetPath) -> Result<Vec<Partition>> {
        let fs_path = self.fs_path(path.std_path());

        if !fs_path.is_dir() {
            return as_err(io::Error::new(io::ErrorKind::NotFound, "not a directory"));
        }

        //FIXME: Support depth > 1

        let partitions = fs::read_dir(self.fs_path(path.std_path()))?
            .map(|dir_entry| {
                let path = dir_entry?.path();
                let file_name = match path.file_name() {
                    Some(f) => f.to_string_lossy().to_string(),
                    None => {
                        return as_err(StoreError::InvalidPartition("".to_string()));
                    }
                };

                let partition = match (file_name.find('='), file_name.ends_with('=')) {
                    (Some(idx), false) => Partition::new(
                        file_name[0..idx].to_string(),
                        file_name[idx + 1..].to_string(),
                    ),
                    _ => return as_err(StoreError::InvalidPartition(file_name)),
                };

                Ok(partition)
            })
            .collect::<Result<Vec<Partition>>>()?;

        Ok(partitions)
    }

    fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectKey>> {
        let fs_path = self.fs_path(path.std_path());

        if !fs_path.is_dir() {
            return as_err(StoreError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "not a directory",
            )));
        }

        fs::read_dir(fs_path)
            .with_context(|| format!("cannot list partition: {}", path))?
            .map(|dir_entry| match dir_entry?.path().file_name() {
                Some(f) => Ok(ObjectKey::from_os_str(f)),
                None => as_err(StoreError::InvalidPartition("".to_string())),
            })
            .collect::<Result<Vec<ObjectKey>>>()
    }

    fn remove_partition(&self, path: &PartitionPath) -> Result<()> {
        fs::remove_dir(self.fs_path(path.std_path()))
            .with_context(|| format!("partition to remove not found: {}", path))?;
        Ok(())
    }

    fn remove_object(&self, path: &ObjectPath) -> Result<()> {
        fs::remove_file(self.fs_path(path.std_path()))
            .with_context(|| format!("object to remove not found: {}", path))?;
        Ok(())
    }

    fn rebalance_objects(
        &self,
        input_paths: &[ObjectPath],
        output_paths: &[ObjectPath],
        rows_per_file: usize,
    ) -> Result<Vec<ObjectState>> {
        let input_files = input_paths
            .iter()
            .map(|path| {
                let file = fs::File::open(self.fs_path(path.std_path()))
                    .with_context(|| format!("rebalance input object not found: {}", path))?;
                Ok(file)
            })
            .collect::<Result<Vec<fs::File>>>()?;

        let output_files = output_paths
            .iter()
            .map(|path| {
                let file = fs::File::create(self.fs_path(path.std_path())).with_context(|| {
                    format!("failed to create rebalance output object: {}", path)
                })?;
                Ok(file)
            })
            .collect::<Result<Vec<fs::File>>>()?;

        combine_objects(input_files, output_files, rows_per_file)?;

        let states = output_paths
            .iter()
            .map(|path| {
                let file = fs::File::open(self.fs_path(path.std_path()))
                    .with_context(|| format!("rebalanced object not found: {}", path))?;
                Ok(read_object_state(&file)?)
            })
            .collect::<Result<Vec<ObjectState>>>()?;

        Ok(states)
    }
}
