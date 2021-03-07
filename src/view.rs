use anyhow::Result;

use crate::path::{DatasetPath, PartitionPath};
use crate::state::State;

pub trait View {
    fn render(&self, state: &State) -> Result<String>;
}

pub struct ListPartitions {
    path: DatasetPath,
    with_objects: bool,
}

impl ListPartitions {
    pub fn new(path: DatasetPath, with_objects: bool) -> Self {
        Self { path, with_objects }
    }
}

impl View for ListPartitions {
    fn render(&self, state: &State) -> Result<String> {
        let mut out = format!("List Partitions for \"{}\":", self.path);

        for partition in state.list_partitions(&self.path)? {
            let objects = state.list_objects(&partition)?;
            let size = state.get_partition(&partition)?.size();
            out.push_str(&format!(
                "\n  - {} (objects: {}, size: {})",
                partition.partition, objects.len(), size
            ));

            if self.with_objects {
                for object_path in &objects {
                    let object = state.get_object(object_path)?;

                    out.push_str(&format!(
                        "\n    - {} (size: {}, format: {})",
                        object_path.key, object.size, object.format
                    ))
                }
            }
        }

        Ok(out)
    }
}

pub struct ListObjects {
    path: PartitionPath,
}

impl ListObjects {
    pub fn new(path: PartitionPath) -> Self {
        Self { path }
    }
}

impl View for ListObjects {
    fn render(&self, state: &State) -> Result<String> {
        let mut out = format!("List Objects for \"{}\":", self.path);

        for object_path in &state.list_objects(&self.path)? {
            let object = state.get_object(object_path)?;

            out.push_str(&format!(
                "\n  - {} (size: {}, format: {})",
                object_path.key, object.size, object.format
            ))
        }

        Ok(out)
    }
}
