use crate::action::{Action, MoveAction, RemoveObjectAction, RemovePartitionAction};
use crate::path::PartitionPath;
use crate::state::{Result as StateResult, State};

type Actions = Vec<Box<dyn Action>>;

pub trait Operation {
    fn actions(&self, state: &State) -> StateResult<Actions>;
}

pub struct ReloadPartition {
    path: PartitionPath,
}

impl ReloadPartition {
    fn new(path: PartitionPath) -> Self {
        Self { path }
    }
}

impl Operation for ReloadPartition {
    fn actions(&self, state: &State) -> StateResult<Actions> {
        unimplemented!()
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

impl Operation for MovePartition {
    fn actions(&self, state: &State) -> StateResult<Actions> {
        let mut actions: Vec<Box<dyn Action>> = vec![];

        if state.contains_partition(&self.target) {
            actions.append(&mut state
                .list_objects(&self.target)?
                .into_iter()
                .map(RemoveObjectAction::new)
                .map(|action| Box::new(action) as Box<dyn Action>)
                .collect());
        }

        let mut move_actions = state
            .list_objects(&self.source)?
            .into_iter()
            .map(|path| {
                let target = path.update_partition(&self.target.partition);
                MoveAction::new(path, target)
            })
            .map(|action| Box::new(action) as Box<dyn Action>)
            .collect();

        actions.append(&mut move_actions);

        actions.push(Box::new(RemovePartitionAction::new(self.source.clone())));

        Ok(actions)
    }
}
