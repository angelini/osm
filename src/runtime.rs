use std::fmt;

use anyhow::Error;

use crate::action::{ActionTree, Keys};
use crate::state::State;
use crate::store::Store;

pub struct Execution {
    pub state: State,
    passed: Vec<String>,
    failed: Vec<(String, Error)>,
}

impl Execution {
    fn new(state: State, passed: Vec<String>, failed: Vec<(String, Error)>) -> Self {
        Self { state, passed, failed }
    }

    pub fn has_errors(&self) -> bool {
        !self.failed.is_empty()
    }

    pub fn errors(self) -> Vec<Error> {
        self.failed.into_iter().map(|(_, error)| error).collect()
    }
}

impl fmt::Display for Execution {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "passed: {:#?}", self.passed)?;
        writeln!(f, "failed: {:#?}", self.failed)
    }
}

pub struct Runtime {
    store: Box<dyn Store>,
}

impl Runtime {
    pub fn new(store: Box<dyn Store>) -> Self {
        Runtime { store }
    }

    pub fn execute(&self, state: &State, actions: ActionTree) -> Execution {
        let mut passed = vec![];
        let mut failed = vec![];

        let mut current_state = state.clone();
        let mut completed = Keys::new();

        while completed.len() != actions.size() {
            let mut error_count = 0;

            for (key, actions) in actions.next_batch(&completed) {
                for action in actions {
                    match action.execute(self.store.as_ref(), &current_state) {
                        Ok(new_state) => {
                            passed.push(action.key());
                            current_state = new_state;
                        }
                        Err(error) => {
                            error_count += 1;
                            failed.push((action.key(), error))
                        }
                    }
                }
                completed.insert(key);
            }

            if error_count > 0 {
                return Execution::new(current_state, passed, failed);
            }
        }

        Execution::new(current_state, passed, failed)
    }
}
