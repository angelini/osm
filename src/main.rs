mod action;
mod base;
mod csv;
mod job;
mod parquet;
mod path;
mod runtime;
mod state;
mod store;
mod view;

use std::path::PathBuf;

use anyhow::Result;

use base::{Bucket, Bytes, Partition, Protocol};
use job::{Job, MovePartition, RebalanceObjects, ReloadDataset};
use path::DatasetPath;
use runtime::Runtime;
use state::State;
use store::FileStore;
use view::{ListPartitions, View};

fn execute_job(
    state: &State,
    runtime: &Runtime,
    path: &DatasetPath,
    job: &dyn Job,
) -> Result<State> {
    let view = ListPartitions::new(path.clone(), true);
    let execution = runtime.execute(&state, job.actions(&state)?);

    println!("{}", execution);
    println!("{}", view.render(&execution.state)?);
    println!("\n---\n");

    match execution.has_errors() {
        true => Err(execution.errors().remove(0)),
        false => Ok(execution.state),
    }
}

fn example(mut state: State, runtime: &Runtime, path: &DatasetPath) -> Result<State> {
    let reload = ReloadDataset::new(path.clone());

    let move_partition = MovePartition::new(
        path.partition_path(&Partition::new("date", "2020-01")),
        path.partition_path(&Partition::new("date", "2021-01")),
    );

    let rebalance = RebalanceObjects::new(
        path.partition_path(&Partition::new("date", "2020-03")),
        Bytes::new_in_mib(15),
    );

    state = execute_job(&state, runtime, path, &reload)?;
    state = execute_job(&state, runtime, path, &move_partition)?;
    state = execute_job(&state, runtime, path, &rebalance)?;

    Ok(state)
}

fn main() -> Result<()> {
    let store = FileStore::new(PathBuf::from("/tmp/osm-root"));

    let bucket = Bucket::new(Protocol::File, "example".to_string());
    let parquet_path = DatasetPath::new(bucket.clone(), PathBuf::from("nyc_taxis"));
    let csv_path = DatasetPath::new(bucket, PathBuf::from("nyc_taxis_csv"));

    let mut state = State::new();
    let runtime = Runtime::new(Box::new(store));

    state = example(state, &runtime, &parquet_path)?;
    state = example(state, &runtime, &csv_path)?;

    Ok(())
}
