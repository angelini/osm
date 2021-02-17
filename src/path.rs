use std::fmt;
use std::path::{PathBuf};

use crate::base::{Bucket, ObjectKey, Partition, ToStdPath};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DatasetPath {
    bucket: Bucket,
    path: PathBuf,
}

impl DatasetPath {
    pub fn new(bucket: Bucket, path: PathBuf) -> Self {
        DatasetPath { bucket, path }
    }

    pub fn partition_path(&self, partition: &Partition) -> PartitionPath {
        PartitionPath {
            dataset: self.clone(),
            partition: partition.clone(),
        }
    }

    pub fn object_path(&self, partition: &Partition, key: &ObjectKey) -> ObjectPath {
        ObjectPath {
            partition: PartitionPath {
                dataset: self.clone(),
                partition: partition.clone(),
            },
            key: key.clone(),
        }
    }
}

impl ToStdPath for DatasetPath {
    fn std_path(&self) -> PathBuf {
        let mut buf = PathBuf::new();
        buf.push(&self.bucket.name);
        buf.push(&self.path);
        buf
    }
}

impl fmt::Display for DatasetPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.bucket, self.path.to_string_lossy())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PartitionPath {
    pub dataset: DatasetPath,
    pub partition: Partition,
}

impl PartitionPath {
    pub fn new(dataset: DatasetPath, partition: Partition) -> Self {
        PartitionPath { dataset, partition }
    }

    pub fn object_path(&self, key: &ObjectKey) -> ObjectPath {
        ObjectPath {
            partition: self.clone(),
            key: key.clone(),
        }
    }
}

impl ToStdPath for PartitionPath {
    fn std_path(&self) -> PathBuf {
        let mut buf = self.dataset.std_path();
        buf.push(self.partition.std_path());
        buf
    }
}

impl fmt::Display for PartitionPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.dataset, self.partition)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ObjectPath {
    partition: PartitionPath,
    pub key: ObjectKey,
}

impl ObjectPath {
    pub fn dataset_path(&self) -> &DatasetPath {
        &self.partition.dataset
    }

    pub fn partition_path(&self) -> &PartitionPath {
        &self.partition
    }

    pub fn get_partition(&self) -> &Partition {
        &self.partition.partition
    }

    pub fn update_partition(&self, partition: &Partition) -> ObjectPath {
        ObjectPath {
            partition: PartitionPath::new(self.partition.dataset.clone(), partition.clone()),
            key: self.key.clone(),
        }
    }
}

impl ToStdPath for ObjectPath {
    fn std_path(&self) -> PathBuf {
        let mut buf = self.partition.std_path();
        buf.push(self.key.as_str());
        buf
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.partition, self.key)
    }
}
