use std::fmt;

use im::HashMap;

use crate::base::{ObjectKey};
use crate::path::{ObjectPath, PartitionPath};
use crate::state::{ObjectState, State, StateError};
use crate::store::{Store, StoreError};

#[derive(Debug)]
pub enum ActionError {
    State(StateError),
    Store(StoreError),
}

impl From<StateError> for ActionError {
    fn from(error: StateError) -> Self {
        Self::State(error)
    }
}

impl From<StoreError> for ActionError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

type Result<T> = std::result::Result<T, ActionError>;

pub struct ActionEffects {
    creates: Vec<ObjectPath>,
    reads: Vec<ObjectPath>,
    updates: Vec<ObjectPath>,
    deletes: Vec<ObjectPath>,
}

impl ActionEffects {
    fn new() -> Self {
        Self {
            creates: vec![],
            reads: vec![],
            updates: vec![],
            deletes: vec![],
        }
    }

    fn create(&mut self, path: ObjectPath) {
        self.creates.push(path)
    }

    fn read(&mut self, path: ObjectPath) {
        self.reads.push(path)
    }

    fn update(&mut self, path: ObjectPath) {
        self.updates.push(path)
    }

    fn delete(&mut self, path: ObjectPath) {
        self.deletes.push(path)
    }
}

pub trait Action: fmt::Debug {
    fn key(&self) -> String;
    fn effects(&self, state: &State) -> ActionEffects;
    fn execute(&self, store: &dyn Store, state: &State) -> Result<State>;
}

#[derive(Debug)]
pub struct ReloadPartitionAction {
    path: PartitionPath,
}

impl ReloadPartitionAction {
    pub fn new(path: PartitionPath) -> Self {
        Self { path }
    }
}

impl Action for ReloadPartitionAction {
    fn key(&self) -> String {
        format!("reload({})", self.path)
    }

    fn effects(&self, state: &State) -> ActionEffects {
        // FIXME: Only considers object changes
        ActionEffects::new()
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let new_state = state.remove_partition(&self.path)?;
        // FIXME: Finish implementation
        // store.reload_partition(&self.path)?;

        let objects: HashMap<ObjectKey, ObjectState> = store
            .list_objects(&self.path)?
            .into_iter()
            .map(|key| (key, ObjectState::new_csv(0, 0)))
            .collect();

        // new_state.insert_partition(PartitionState::new(objects));

        Ok(new_state)
    }
}

#[derive(Debug)]
pub struct RemovePartitionAction {
    path: PartitionPath,
}

impl RemovePartitionAction {
    pub fn new(path: PartitionPath) -> Self {
        Self { path }
    }
}

impl Action for RemovePartitionAction {
    fn key(&self) -> String {
        format!("rm({}/)", self.path)
    }

    fn effects(&self, state: &State) -> ActionEffects {
        // FIXME: Only considers object changes
        ActionEffects::new()
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let new_state = state.remove_partition(&self.path)?;
        store.remove_partition(&self.path)?;

        Ok(new_state)
    }
}

#[derive(Debug)]
pub struct RemoveObjectAction {
    path: ObjectPath,
}

impl RemoveObjectAction {
    pub fn new(path: ObjectPath) -> Self {
        Self { path }
    }
}

impl Action for RemoveObjectAction {
    fn key(&self) -> String {
        format!("remove({})", self.path)
    }

    fn effects(&self, state: &State) -> ActionEffects {
        let mut effects = ActionEffects::new();
        effects.delete(self.path.clone());
        effects
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let new_state = state.remove_object(&self.path)?;
        store.remove_object(&self.path)?;

        Ok(new_state)
    }
}

#[derive(Debug)]
pub struct MoveAction {
    source: ObjectPath,
    target: ObjectPath,
}

impl MoveAction {
    pub fn new(source: ObjectPath, target: ObjectPath) -> Self {
        Self { source, target }
    }
}

impl Action for MoveAction {
    fn key(&self) -> String {
        format!("move({}, {})", self.source, self.target)
    }

    fn effects(&self, state: &State) -> ActionEffects {
        let mut effects = ActionEffects::new();
        effects.delete(self.source.clone());

        if state.contains_object(&self.target) {
            effects.update(self.target.clone());
        } else {
            effects.create(self.target.clone());
        }

        effects
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let new_state = state.move_object(&self.source, &self.target)?;
        store.move_object(&self.source, &self.target)?;

        Ok(new_state)
    }
}
