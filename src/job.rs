use crate::action::{
    ActionTree, MoveAction, RebalanceAction, ReloadDatasetAction, RemoveObjectAction, RemovePartitionAction,
};
use crate::base::Bytes;
use crate::path::{DatasetPath, PartitionPath};
use crate::state::{Result as StateResult, State};

pub trait Job {
    fn actions(&self, state: &State) -> StateResult<ActionTree>;
}

pub struct ReloadDataset {
    path: DatasetPath,
}

impl ReloadDataset {
    pub fn new(path: DatasetPath) -> Self {
        ReloadDataset { path }
    }
}

impl Job for ReloadDataset {
    fn actions(&self, _: &State) -> StateResult<ActionTree> {
        let reload = ReloadDatasetAction::new(self.path.clone());
        Ok(ActionTree::single(Box::new(reload)))
    }
}

pub struct MovePartition {
    source: PartitionPath,
    target: PartitionPath,
}

impl MovePartition {
    pub fn new(source: PartitionPath, target: PartitionPath) -> Self {
        MovePartition { source, target }
    }
}

impl Job for MovePartition {
    fn actions(&self, state: &State) -> StateResult<ActionTree> {
        let mut actions = ActionTree::new();

        let remove_target_node = actions.add_node(&[]);

        if state.contains_partition(&self.target) {
            for object in state.list_objects(&self.target)? {
                actions.add_action(
                    remove_target_node,
                    Box::new(RemoveObjectAction::new(object)),
                )
            }
        }

        let copy_node = actions.add_node(&[remove_target_node]);

        for object in state.list_objects(&self.source)? {
            // FIXME: Object stores support copy and not move
            let target = object.update_partition(&self.target.partition);
            actions.add_action(copy_node, Box::new(MoveAction::new(object, target)))
        }

        let remove_partition_node = actions.add_node(&[copy_node]);
        actions.add_action(
            remove_partition_node,
            Box::new(RemovePartitionAction::new(self.source.clone())),
        );

        Ok(actions)
    }
}

pub struct RebalanceObjects {
    path: PartitionPath,
    target_size: Bytes,
}

impl RebalanceObjects {
    pub fn new(path: PartitionPath, target_size: Bytes) -> Self {
        Self { path, target_size }
    }
}

impl Job for RebalanceObjects {
    fn actions(&self, state: &State) -> StateResult<ActionTree> {
        let mut actions = ActionTree::new();
        let partition_size = state.get_partition(&self.path)?.size();

        if partition_size < self.target_size.grow(1.5) {
            return Ok(actions)
        }

        let objects = state.list_objects(&self.path)?;
        let target_count = partition_size.div(self.target_size);

        let rebalance_node = actions.add_node(&[]);
        actions.add_action(rebalance_node, Box::new(RebalanceAction::new(objects.clone(), target_count)));

        let delete_node = actions.add_node(&[]);
        for object in &objects {
            actions.add_action(delete_node, Box::new(RemoveObjectAction::new(object.clone())))
        }

        Ok(actions)
    }
}
