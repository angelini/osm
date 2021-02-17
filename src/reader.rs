use im::HashMap;

use crate::base::Partition;
use crate::path::DatasetPath;
use crate::state::{DatasetState, ObjectState, PartitionState, State};
use crate::store::{Result as StoreResult, Store};

fn read_partition(
    store: &dyn Store,
    path: &DatasetPath,
    partition: &Partition,
) -> StoreResult<PartitionState> {
    let objects = store
        .list_objects(&path.partition_path(partition))?
        .into_iter()
        .map(|key| (key, ObjectState::new_csv(0, 0)))
        .collect();
    Ok(PartitionState::new(objects))
}

fn read_dataset(store: &dyn Store, path: &DatasetPath) -> StoreResult<DatasetState> {
    let partitions = store
        .list_partitions(path)?
        .into_iter()
        .map(|partition| {
            let state = read_partition(store, path, &partition)?;
            Ok((partition, state))
        })
        .collect::<StoreResult<HashMap<Partition, PartitionState>>>()?;
    Ok(DatasetState::new(partitions))
}

pub fn read_state(store: &dyn Store, datasets: Vec<DatasetPath>) -> StoreResult<State> {
    let states = datasets
        .into_iter()
        .map(|path| {
            let state = read_dataset(store, &path)?;
            Ok((path, state))
        })
        .collect::<StoreResult<HashMap<DatasetPath, DatasetState>>>()?;
    Ok(State::new(states))
}
