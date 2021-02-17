use crate::action::{Action, MoveAction, RemoveAction, RemovePartitionAction};
use crate::path::PartitionPath;
use crate::state::{Result as StateResult, State};

pub trait Transformer {
    fn actions(&self, state: &State) -> StateResult<Vec<Box<dyn Action>>>;
}

pub struct MovePartitionTransformer {
    source: PartitionPath,
    target: PartitionPath,
}

impl MovePartitionTransformer {
    pub fn new(source: PartitionPath, target: PartitionPath) -> Self {
        MovePartitionTransformer { source, target }
    }
}

impl Transformer for MovePartitionTransformer {
    fn actions(&self, state: &State) -> StateResult<Vec<Box<dyn Action>>> {
        let mut actions: Vec<Box<dyn Action>> = vec![];

        if state.contains_partition(&self.target) {
            actions.append(&mut state
                .list_objects(&self.target)?
                .into_iter()
                .map(RemoveAction::new)
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
