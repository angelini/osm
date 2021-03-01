use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::base::{ObjectKey, Partition};
use crate::path::{DatasetPath, ObjectPath, PartitionPath};
use crate::state::{DatasetState, ObjectState, PartitionState, State, StateError};
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

pub type Result<T> = std::result::Result<T, ActionError>;

pub trait Action: fmt::Debug {
    fn key(&self) -> String;
    fn execute(&self, store: &dyn Store, state: &State) -> Result<State>;
}

pub type Actions = Vec<Box<dyn Action>>;

#[derive(Clone, Debug)]
pub struct ReloadDatasetAction {
    path: DatasetPath,
}

impl ReloadDatasetAction {
    pub fn new(path: DatasetPath) -> Self {
        Self { path }
    }

    fn load_dataset(&self, store: &dyn Store) -> Result<DatasetState> {
        Ok(DatasetState::new(
            store
                .list_partitions(&self.path)?
                .into_iter()
                .map(|partition| {
                    let action = ReloadPartitionAction::new(self.path.partition_path(&partition));
                    Ok((partition, action.load_partition(store)?))
                })
                .collect::<Result<im::HashMap<Partition, PartitionState>>>()?,
        ))
    }
}

impl Action for ReloadDatasetAction {
    fn key(&self) -> String {
        format!("reload({})", self.path)
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        Ok(state.insert_dataset(&self.path, self.load_dataset(store)?)?)
    }
}

#[derive(Clone, Debug)]
pub struct ReloadPartitionAction {
    path: PartitionPath,
}

impl ReloadPartitionAction {
    pub fn new(path: PartitionPath) -> Self {
        Self { path }
    }

    fn load_partition(&self, store: &dyn Store) -> Result<PartitionState> {
        Ok(PartitionState::new(
            store
                .list_objects(&self.path)?
                .into_iter()
                .map(|key| {
                    let path = self.path.object_path(&key);
                    let state = store.read_object(&path)?;
                    Ok((key, state))
                })
                .collect::<Result<im::HashMap<ObjectKey, ObjectState>>>()?,
        ))
    }
}

impl Action for ReloadPartitionAction {
    fn key(&self) -> String {
        format!("reload({})", self.path)
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        Ok(state.insert_partition(&self.path, self.load_partition(store)?)?)
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

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let new_state = state.move_object(&self.source, &self.target)?;
        store.move_object(&self.source, &self.target)?;

        Ok(new_state)
    }
}

#[derive(Debug)]
pub struct RebalanceAction {
    paths: Vec<ObjectPath>,
    count: usize,
}

impl RebalanceAction {
    pub fn new(paths: Vec<ObjectPath>, count: usize) -> Self {
        Self { paths, count }
    }
}

impl Action for RebalanceAction {
    fn key(&self) -> String {
        let paths = self
            .paths
            .iter()
            .map(|p| format!("{}", p))
            .collect::<Vec<String>>();
        format!("rebalance({}, {})", paths.join(", "), self.count)
    }

    fn execute(&self, store: &dyn Store, state: &State) -> Result<State> {
        let total_rows: usize = self
            .paths
            .iter()
            .map(|path| state.get_object(path).map_or(0, |object| object.rows))
            .sum();
        let rows_per_file = total_rows / self.count;

        let output_paths = (0..self.count)
            .map(|idx| {
                self.paths[0]
                    .partition_path()
                    .object_path(&ObjectKey::new(format!("{}.parquet", idx)))
            })
            .collect::<Vec<ObjectPath>>();

        let object_states = store.rebalance_objects(self.paths.as_slice(), &output_paths, rows_per_file)?;

        let mut new_state = state.clone();
        for (path, object_state) in output_paths.iter().zip(object_states) {
            new_state = new_state.insert_object(path, object_state)?;
        }

        Ok(new_state)
    }
}

pub type Key = usize;
pub type Keys = HashSet<Key>;

#[derive(Debug)]
pub struct ActionTree {
    next_key: Key,
    roots: Keys,
    upstream: HashMap<Key, Keys>,
    actions: HashMap<Key, Actions>,
}

impl ActionTree {
    pub fn new() -> Self {
        Self {
            next_key: 1,
            roots: Keys::new(),
            upstream: HashMap::new(),
            actions: HashMap::new(),
        }
    }

    pub fn single(action: Box<dyn Action>) -> Self {
        let mut tree = Self::new();
        let key = tree.add_node(&[]);
        tree.add_action(key, action);
        tree
    }

    pub fn add_node(&mut self, dependencies: &[Key]) -> Key {
        let key = self.next_key;
        self.next_key += 1;

        for dependency in dependencies {
            let upstream = self.upstream.entry(key).or_insert_with(Keys::new);
            upstream.insert(*dependency);
        }

        if dependencies.is_empty() {
            self.roots.insert(key);
        }

        key
    }

    pub fn add_action(&mut self, key: Key, action: Box<dyn Action>) {
        let entry = self.actions.entry(key).or_insert_with(Vec::new);
        entry.push(action);
    }

    pub fn size(&self) -> usize {
        self.next_key - 1
    }

    pub fn next_batch(&self, completed: &Keys) -> Vec<(Key, Vec<&dyn Action>)> {
        if completed.is_empty() {
            return self
                .roots
                .iter()
                .map(|key| (*key, self.get_actions(key)))
                .collect();
        }

        self.upstream
            .iter()
            .filter(|(key, upstream_keys)| {
                !completed.contains(key)
                    && upstream_keys
                        .difference(completed)
                        .collect::<HashSet<&Key>>()
                        .is_empty()
            })
            .map(|(key, _)| (*key, self.get_actions(key)))
            .collect()
    }

    fn get_actions(&self, key: &Key) -> Vec<&dyn Action> {
        if self.actions.contains_key(key) {
            self.actions
                .get(key)
                .unwrap()
                .iter()
                .map(|action| action.as_ref())
                .collect()
        } else {
            Vec::new()
        }
    }
}
