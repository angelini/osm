mod action;
mod base;
mod generator;
mod job;
mod path;
mod state;
mod store;
mod writer;

use std::path::PathBuf;

use arrow::datatypes::{DataType, Field, Schema};

use action::{ActionTree, ActionError, Result, Keys};
use base::{Bucket, Partition, Protocol};
use job::{Job, MovePartition, ReloadDataset};
use path::DatasetPath;
use state::State;
use store::{FileStore, Store};

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
        state: &State,
        actions: ActionTree,
    ) -> State {
        let mut current_state = state.clone();
        let mut completed = Keys::new();

        while completed.len() != actions.size() {
            let mut error_count = 0;

            for (key, actions) in actions.next_batch(&completed) {
                for action in actions {
                    match action.execute(self.store.as_ref(), &current_state) {
                        Ok(new_state) => {
                            self.passed.push(action.key());
                            current_state = new_state;
                        }
                        Err(error) => {
                            error_count += 1;
                            self.failed.push((action.key(), error))
                        },
                    }
                }
                completed.insert(key);
            }

            if error_count > 0 {
                return current_state
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

    let mut runtime = Runtime::new(Box::new(store));
    let mut state = State::new();
    println!("state-0: {}", state.pretty_print());

    let reload = ReloadDataset::new(path.clone());
    let move_partition =
        MovePartition::new(
            path.partition_path(&Partition::new("v", "1")),
            path.partition_path(&Partition::new("v", "10")),
        );

    state = runtime.execute(&state, reload.actions(&state)?);
    println!("state-1: {}", state.pretty_print());

    state = runtime.execute(&state, move_partition.actions(&state)?);
    println!("state-2: {}", state.pretty_print());

    println!("passed: {:?}", runtime.passed);
    println!("failed: {:?}", runtime.failed);

    Ok(())
}
