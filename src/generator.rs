use arrow::array::{Array, Int32Array};
use arrow::datatypes::Schema;

struct DatasetGenerator {
    schema: Schema,
}

impl DatasetGenerator {
    fn generate(&self, size: usize) -> Vec<Box<dyn Array>> {
        // FIXME: Build from schema & size
        let mut builder = Int32Array::builder(5);
        builder.append_slice(&[0, 1, 2, 3, 4]).unwrap();
        vec![Box::new(builder.finish())]
    }
}
