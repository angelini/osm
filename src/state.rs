use std::fmt;

use anyhow::Result;
use arrow::datatypes::Schema;
use im::HashMap;
use parquet::schema::types::Type as ParquetType;
use thiserror::Error;

use crate::base::{Bytes, ObjectKey, Partition};
use crate::path::{DatasetPath, ObjectPath, PartitionPath};

#[derive(Error, Debug)]
pub enum StateError {
    #[error("Missing dataset: {0}")]
    MissingDataset(DatasetPath),

    #[error("Missing partition: {0}")]
    MissingPartition(Partition),

    #[error("Missing object: {0}")]
    MissingObject(ObjectKey),
}

#[derive(Debug, Clone)]
pub struct CsvFormatState {
    schema: Schema,
    delimiter: String,
}

impl CsvFormatState {
    pub fn new(schema: Schema, delimiter: String) -> Self {
        CsvFormatState { schema, delimiter }
    }
}

#[derive(Debug, Clone)]
pub struct ParquetFormatState {
    schema: ParquetType,
    num_rows: usize,
}

impl ParquetFormatState {
    pub fn new(schema: ParquetType, num_rows: usize) -> Self {
        Self { schema, num_rows }
    }
}

#[derive(Debug, Clone)]
pub enum FormatState {
    Csv(CsvFormatState),
    Parquet(ParquetFormatState),
}

impl FormatState {
    fn num_rows(&self) -> Option<usize> {
        match self {
            FormatState::Parquet(state) => Some(state.num_rows),
            _ => None,
        }
    }
}

impl fmt::Display for FormatState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FormatState::Csv(state) => write!(f, "Csv(delimiter: {})", state.delimiter),
            FormatState::Parquet(state) => write!(f, "Parquet(num_rows: {})", state.num_rows),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectState {
    pub format: FormatState,
    pub size: Bytes,
}

impl ObjectState {
    pub fn new_csv(format: CsvFormatState, size: Bytes) -> Self {
        Self {
            format: FormatState::Csv(format),
            size,
        }
    }

    pub fn new_parquet(format: ParquetFormatState, size: Bytes) -> Self {
        Self {
            format: FormatState::Parquet(format),
            size,
        }
    }

    pub fn num_rows(&self) -> Option<usize> {
        self.format.num_rows()
    }
}

impl fmt::Display for ObjectState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Object(size: {}, format: {})", self.size, self.format)
    }
}

#[derive(Debug, Default, Clone)]
pub struct PartitionState {
    objects: HashMap<ObjectKey, ObjectState>,
}

impl PartitionState {
    pub fn new(objects: HashMap<ObjectKey, ObjectState>) -> Self {
        PartitionState { objects }
    }

    fn get(&self, key: &ObjectKey) -> Result<&ObjectState> {
        Ok(self
            .objects
            .get(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))?)
    }

    fn get_mut(&mut self, key: &ObjectKey) -> Result<&mut ObjectState> {
        Ok(self
            .objects
            .get_mut(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))?)
    }

    pub fn size(&self) -> Bytes {
        self.objects
            .iter()
            .map(|(_, obj)| obj.size)
            .fold(Bytes::new(0), |acc, obj_size| acc + obj_size)
    }

    fn insert_object(&mut self, key: ObjectKey, state: ObjectState) {
        self.objects.insert(key, state);
    }

    fn remove_object(&mut self, key: &ObjectKey) -> Result<ObjectState> {
        Ok(self
            .objects
            .remove(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))?)
    }
}

#[derive(Debug, Clone)]
pub struct DatasetState {
    partitions: HashMap<Partition, PartitionState>,
}

impl DatasetState {
    pub fn new(partitions: HashMap<Partition, PartitionState>) -> Self {
        DatasetState { partitions }
    }

    fn get(&self, partition: &Partition) -> Result<&PartitionState> {
        Ok(self
            .partitions
            .get(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))?)
    }

    fn get_mut(&mut self, partition: &Partition) -> Result<&mut PartitionState> {
        Ok(self
            .partitions
            .get_mut(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))?)
    }

    fn list_objects(&self, partition: &Partition) -> Result<Vec<ObjectKey>> {
        self.get(partition)
            .map(|p_state| p_state.objects.keys().cloned().collect())
    }

    fn remove_object(&mut self, partition: &Partition, key: &ObjectKey) -> Result<ObjectState> {
        let partition = self.get_mut(partition)?;
        partition.remove_object(key)
    }

    fn remove_partition(&mut self, partition: &Partition) -> Result<PartitionState> {
        Ok(self
            .partitions
            .remove(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))?)
    }

    fn insert_partition(&mut self, partition: &Partition, state: PartitionState) {
        self.partitions.insert(partition.clone(), state);
    }
}

#[derive(Debug, Clone)]
pub struct State {
    datasets: HashMap<DatasetPath, DatasetState>,
}

impl State {
    pub fn new() -> Self {
        State {
            datasets: HashMap::new(),
        }
    }

    fn get(&self, path: &DatasetPath) -> Result<&DatasetState> {
        Ok(self
            .datasets
            .get(path)
            .ok_or_else(|| StateError::MissingDataset(path.clone()))?)
    }

    fn get_mut(&mut self, path: &DatasetPath) -> Result<&mut DatasetState> {
        Ok(self
            .datasets
            .get_mut(path)
            .ok_or_else(|| StateError::MissingDataset(path.clone()))?)
    }

    pub fn get_partition(&self, path: &PartitionPath) -> Result<&PartitionState> {
        self.get(&path.dataset)
            .and_then(|ds| ds.get(&path.partition))
    }

    pub fn get_object(&self, path: &ObjectPath) -> Result<&ObjectState> {
        self.get(&path.dataset_path())
            .and_then(|ds| ds.get(&path.get_partition()))
            .and_then(|pt| pt.get(&path.key))
    }

    pub fn contains_partition(&self, path: &PartitionPath) -> bool {
        match self.get(&path.dataset) {
            Ok(dataset) => dataset.get(&path.partition).is_ok(),
            Err(_) => false,
        }
    }

    pub fn contains_object(&self, path: &ObjectPath) -> bool {
        let dataset = match self.get(path.dataset_path()) {
            Ok(dataset) => dataset,
            Err(_) => return false,
        };

        match dataset.get(path.get_partition()) {
            Ok(partition) => partition.get(&path.key).is_ok(),
            Err(_) => false,
        }
    }

    pub fn list_partitions(&self, path: &DatasetPath) -> Result<Vec<PartitionPath>> {
        self.get(path)
            .map(|ds| ds.partitions.keys().collect::<Vec<&Partition>>())
            .map(|parts| parts.into_iter().map(|p| path.partition_path(p)).collect())
    }

    pub fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectPath>> {
        self.get(&path.dataset)
            .and_then(|ds| ds.list_objects(&path.partition))
            .map(|keys| keys.into_iter().map(|k| path.object_path(&k)).collect())
    }

    pub fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<Self> {
        let mut new_state = self.clone();
        let object_state;

        {
            let source_dataset = new_state.get_mut(source.dataset_path())?;
            let source_partition = source_dataset.get_mut(source.get_partition())?;
            object_state = source_partition.remove_object(&source.key)?;
        }

        {
            let target_dataset = new_state.get_mut(target.dataset_path())?;
            let target_partition = target_dataset
                .partitions
                .entry(target.get_partition().clone())
                .or_insert(PartitionState::default());
            target_partition
                .objects
                .insert(target.key.clone(), object_state);
        }

        Ok(new_state)
    }

    pub fn remove_partition(&self, path: &PartitionPath) -> Result<Self> {
        let mut new_state = self.clone();

        let dataset = new_state.get_mut(&path.dataset)?;
        dataset.remove_partition(&path.partition)?;

        Ok(new_state)
    }

    pub fn remove_object(&self, path: &ObjectPath) -> Result<Self> {
        let mut new_state = self.clone();

        let dataset = new_state.get_mut(&path.dataset_path())?;
        dataset.remove_object(&path.get_partition(), &path.key)?;

        Ok(new_state)
    }

    pub fn insert_dataset(&self, path: &DatasetPath, state: DatasetState) -> Result<Self> {
        let mut new_state = self.clone();

        new_state.datasets.insert(path.clone(), state);

        Ok(new_state)
    }

    pub fn insert_partition(&self, path: &PartitionPath, state: PartitionState) -> Result<Self> {
        let mut new_state = self.clone();

        let dataset = new_state.get_mut(&path.dataset)?;
        dataset.insert_partition(&path.partition, state);

        Ok(new_state)
    }

    pub fn insert_object(&self, path: &ObjectPath, state: ObjectState) -> Result<Self> {
        let mut new_state = self.clone();

        let dataset = new_state.get_mut(&path.dataset_path())?;
        let partition = dataset.get_mut(&path.get_partition())?;
        partition.insert_object(path.key.clone(), state);

        Ok(new_state)
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "State:")?;
        for (ds_path, ds_state) in &self.datasets {
            writeln!(f, "  - {}:", ds_path)?;
            for (part, pt_state) in &ds_state.partitions {
                writeln!(f, "    {}:", part)?;
                for (key, ob_state) in &pt_state.objects {
                    writeln!(f, "      {}: {}", key, ob_state)?;
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}
