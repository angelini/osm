use std::fmt;

use im::HashMap;
use parquet::schema::types::Type as ParquetType;

use crate::base::{Bytes, Compression, ObjectKey, Partition};
use crate::path::{DatasetPath, ObjectPath, PartitionPath};

#[derive(Debug)]
pub enum StateError {
    MissingDataset(DatasetPath),
    MissingPartition(Partition),
    MissingObject(ObjectKey),
}

pub type Result<T> = std::result::Result<T, StateError>;

#[derive(Debug, Clone)]
struct CsvFormatState {
    compression: Compression,
    delimiter: String,
}

impl CsvFormatState {
    fn default() -> Self {
        CsvFormatState {
            compression: Compression::None,
            delimiter: ",".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct ParquetFormatState {
    schema: ParquetType,
}

impl ParquetFormatState {
    fn new(schema: ParquetType) -> Self {
        Self { schema }
    }
}

#[derive(Debug, Clone)]
enum FormatState {
    Csv(CsvFormatState),
    Parquet(ParquetFormatState),
}

impl fmt::Display for FormatState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FormatState::Csv(_) => write!(f, "Csv"),
            FormatState::Parquet(_) => write!(f, "Parquet"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectState {
    format: FormatState,
    count: usize,
    size: Bytes,
}

impl ObjectState {
    pub fn new_csv(count: usize, size: Bytes) -> Self {
        ObjectState {
            format: FormatState::Csv(CsvFormatState::default()),
            count,
            size,
        }
    }

    pub fn new_parquet(count: usize, size: Bytes, type_: ParquetType) -> Self {
        ObjectState {
            format: FormatState::Parquet(ParquetFormatState::new(type_)),
            count,
            size,
        }
    }
}

impl fmt::Display for ObjectState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Object(count: {}, size: {}, format: {})",
            self.count, self.size, self.format
        )
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
        self.objects
            .get(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))
    }

    fn get_mut(&mut self, key: &ObjectKey) -> Result<&mut ObjectState> {
        self.objects
            .get_mut(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))
    }

    fn size(&self) -> Bytes {
        self.objects
            .iter()
            .map(|(_, obj)| obj.size)
            .fold(Bytes::new(0), |acc, obj_size| acc + obj_size)
    }

    fn remove_object(&mut self, key: &ObjectKey) -> Result<ObjectState> {
        self.objects
            .remove(key)
            .ok_or_else(|| StateError::MissingObject(key.clone()))
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
        self.partitions
            .get(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))
    }

    fn get_mut(&mut self, partition: &Partition) -> Result<&mut PartitionState> {
        self.partitions
            .get_mut(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))
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
        self.partitions
            .remove(partition)
            .ok_or_else(|| StateError::MissingPartition(partition.clone()))
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

    fn get(&self, dataset: &DatasetPath) -> Result<&DatasetState> {
        self.datasets
            .get(dataset)
            .ok_or_else(|| StateError::MissingDataset(dataset.clone()))
    }

    fn get_mut(&mut self, dataset: &DatasetPath) -> Result<&mut DatasetState> {
        self.datasets
            .get_mut(dataset)
            .ok_or_else(|| StateError::MissingDataset(dataset.clone()))
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

    pub fn get_partition_size(&self, path: &PartitionPath) -> Result<Bytes> {
        self.get(&path.dataset)
            .and_then(|ds| ds.get(&path.partition))
            .map(|partition| partition.size())
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
