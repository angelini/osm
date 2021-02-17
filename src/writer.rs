use arrow::array::Array;
use arrow::datatypes::Schema;

use crate::base::{ObjectKey, Partition};
use crate::path::DatasetPath;

struct DatasetWriter {
    path: DatasetPath,
}

impl DatasetWriter {
    fn write(&self, partition: Partition, key: ObjectKey, schema: Schema, data: Vec<Box<dyn Array>>) {
        unimplemented!()
    }
}
