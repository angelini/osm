use im::HashMap;

use parquet::schema::types::Type as ParquetType;

use crate::base::{Compression, ObjectKey, Partition};
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
    compression: Compression,
    num_rows: i64,
    schema: ParquetType,
}

#[derive(Debug, Clone)]
enum FormatState {
    Csv(CsvFormatState),
    Parquet(ParquetFormatState),
}

#[derive(Debug, Clone)]
pub struct ObjectState {
    format: FormatState,
    count: usize,
    size: usize,
}

impl ObjectState {
    pub fn new_csv(count: usize, size: usize) -> Self {
        ObjectState {
            format: FormatState::Csv(CsvFormatState::default()),
            count,
            size,
        }
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

    fn remove(&mut self, key: &ObjectKey) -> Result<ObjectState> {
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

    pub fn list_objects(&self, partition: &Partition) -> Result<Vec<ObjectKey>> {
        self.get(partition)
            .map(|p_state| p_state.objects.keys().cloned().collect())
    }
}

#[derive(Debug, Clone)]
pub struct State {
    datasets: HashMap<DatasetPath, DatasetState>,
}

impl State {
    pub fn new(datasets: HashMap<DatasetPath, DatasetState>) -> Self {
        State { datasets }
    }

    pub fn pretty_print(&self) -> String {
        let mut s = String::new();
        s.push_str("State:\n");
        for (ds_path, ds_state) in &self.datasets {
            s.push_str(&format!("  - {}:\n", ds_path));
            for (part, pt_state) in &ds_state.partitions {
                s.push_str(&format!("    {}:\n", part));
                for (key, ob_state) in &pt_state.objects {
                    s.push_str(&format!("      {}: {:?}\n", key, ob_state));
                }
            }
            s.push_str("\n");
        }
        s
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

    pub fn list_objects(&self, path: &PartitionPath) -> Result<Vec<ObjectPath>> {
        self.get(&path.dataset)
            .and_then(|ds| ds.list_objects(&path.partition))
            .map(|keys| {
                keys.into_iter()
                    .map(|k| path.object_path(&k))
                    .collect()
            })
    }

    pub fn move_object(&self, source: &ObjectPath, target: &ObjectPath) -> Result<Self> {
        let mut new_state = self.clone();
        let object_state;

        {
            let source_dataset = new_state.get_mut(source.dataset_path())?;
            let source_partition = source_dataset.get_mut(source.get_partition())?;
            object_state = source_partition.remove(&source.key)?;
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
        dataset.partitions.remove(&path.partition);

        Ok(new_state)
    }
}