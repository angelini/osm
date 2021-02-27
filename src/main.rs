mod action;
mod base;
mod job;
mod parquet;
mod path;
mod state;
mod store;
mod view;

use std::path::PathBuf;

use action::{ActionError, ActionTree, Keys, Result};
use base::{Bucket, Partition, Protocol};
use job::{Job, MovePartition, ReloadDataset};
use path::DatasetPath;
use state::State;
use store::{FileStore, Store};
use view::{ListPartitions, View};

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

    fn execute(&mut self, state: &State, actions: ActionTree) -> State {
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
                        }
                    }
                }
                completed.insert(key);
            }

            if error_count > 0 {
                return current_state;
            }
        }

        current_state
    }
}

fn main() -> Result<()> {
    let store = FileStore::new(PathBuf::from("/tmp/osm-root"));

    let bucket = Bucket::new(Protocol::File, "example".to_string());
    let path = DatasetPath::new(bucket, PathBuf::from("nyc_taxis"));

    let mut runtime = Runtime::new(Box::new(store));
    let mut state = State::new();
    println!("state-0: {}", state);

    let reload = ReloadDataset::new(path.clone());
    let move_partition = MovePartition::new(
        path.partition_path(&Partition::new("date", "2020-01")),
        path.partition_path(&Partition::new("date", "2021-01")),
    );

    state = runtime.execute(&state, reload.actions(&state)?);
    println!("state-1: {}", state);
    println!("passed: {:#?}", runtime.passed);
    println!("failed: {:#?}", runtime.failed);
    println!("\n---\n");

    state = runtime.execute(&state, move_partition.actions(&state)?);
    println!("state-2: {}", state);
    println!("passed: {:#?}", runtime.passed);
    println!("failed: {:#?}", runtime.failed);
    println!("\n---\n");

    println!("{}", ListPartitions::new(path).render(&state)?);

    Ok(())
}
