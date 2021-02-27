use crate::path::DatasetPath;
use crate::state::{Result as StateResult, State};

pub trait View {
    fn render(&self, state: &State) -> StateResult<String>;
}

pub struct ListPartitions {
    path: DatasetPath,
}

impl ListPartitions {
    pub fn new(path: DatasetPath) -> Self {
        Self { path }
    }
}

impl View for ListPartitions {
    fn render(&self, state: &State) -> StateResult<String> {
        let mut out = format!("List Partitions for \"{}\":", self.path);

        for partition in state.list_partitions(&self.path)? {
            let objects = state.list_objects(&partition)?.len();
            let size = state.get_partition_size(&partition)?;
            out.push_str(&format!(
                "\n  - {} (objects: {}, size: {})",
                partition.partition, objects, size
            ))
        }

        Ok(out)
    }
}
