mod action;
mod base;
mod generator;
mod operation;
mod path;
mod reader;
mod state;
mod store;
mod tree;
mod writer;

use std::path::PathBuf;

use arrow::datatypes::{DataType, Field, Schema};

use action::{Action, ActionError};
use base::{Bucket, Partition, Protocol};
use operation::{MovePartition, Operation};
use path::DatasetPath;
use state::{State, StateError};
use store::{FileStore, Store, StoreError};

#[derive(Debug)]
enum RuntimeError {
    State(StateError),
    Store(StoreError),
}

impl From<StateError> for RuntimeError {
    fn from(error: StateError) -> Self {
        Self::State(error)
    }
}

impl From<StoreError> for RuntimeError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<ActionError> for RuntimeError {
    fn from(error: ActionError) -> Self {
        match error {
            ActionError::Store(e) => RuntimeError::Store(e),
            ActionError::State(e) => RuntimeError::State(e),
        }
    }
}

type Result<T> = std::result::Result<T, RuntimeError>;

struct Runtime {
    store: Box<dyn Store>,
    passed: Vec<String>,
    failed: Vec<(String, ActionError)>,
}

impl Runtime {
    fn new(store: Box<dyn Store>) -> Self {
        Runtime {
            store,
            passed: vec![],
            failed: vec![],
        }
    }

    fn execute(
        &mut self,
        state: State,
        actions: Vec<Box<dyn Action>>,
    ) -> State {
        let mut current_state = state;

        for action in actions {
            match action.execute(self.store.as_ref(), &current_state) {
                Ok(new_state) => {
                    self.passed.push(action.key());
                    current_state = new_state;
                }
                Err(error) => self.failed.push((action.key(), error)),
            }
        }

        current_state
    }
}

fn main() -> Result<()> {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);

    let root = PathBuf::from("/tmp/osm-root");
    let store = FileStore::new(root);

    let bucket = Bucket::new(Protocol::File, "example".to_string());
    let path = DatasetPath::new(bucket, PathBuf::from("data/one"));

    let state = reader::read_state(&store, vec![path.clone()])?;

    print!("Initial {}", state.pretty_print());

    let mut runtime = Runtime::new(Box::new(store));

    let source = path.partition_path(&Partition::new("v", "1"));
    let target = path.partition_path(&Partition::new("v", "10"));

    let move_partition =
        MovePartition::new(
            source,
            target,
        );

    let actions = move_partition.actions(&state)?;
    for action in &actions {
        println!("action: {}", action.key());
    }

    let final_state = runtime.execute(state, actions);

    println!("passed: {:?}", runtime.passed);
    println!("failed: {:?}", runtime.failed);

    print!("\nFinal {}", final_state.pretty_print());

    Ok(())
}
