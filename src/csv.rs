use anyhow::Result;
use arrow::csv;
use std::io;

use crate::base::Bytes;
use crate::state::{CsvFormatState, ObjectState};

pub struct Csv {}

impl Csv {
    const BATCH_SIZE: usize = 2048 * 10;

    pub fn read_object_state<R: 'static + io::Read + io::Seek>(mut reader: R) -> Result<ObjectState> {
        let size = reader.seek(io::SeekFrom::End(0))?;
        reader.seek(io::SeekFrom::Start(0))?;

        let builder = csv::ReaderBuilder::new().infer_schema(Some(10));
        let csv_reader = builder.build(reader)?;
        let schema = csv_reader.schema();
        let format_state = CsvFormatState::new((*schema).clone(), ",".to_string());

        Ok(ObjectState::new_csv(format_state, Bytes::new(size as usize)))
    }

    pub fn combine_objects<R: 'static + io::Read + io::Seek, W: 'static + io::Write>(
        readers: Vec<R>,
        mut writers: Vec<W>,
        is_writer_full: Box<dyn Fn(usize) -> bool>,
    ) -> Result<()> {
        let mut writer_idx = 0;
        let mut writer = csv::Writer::new(writers.remove(0));

        for reader in readers {
            let csv_reader = csv::ReaderBuilder::new()
                .infer_schema(Some(Self::BATCH_SIZE))
                .with_batch_size(Self::BATCH_SIZE)
                .has_header(true)
                .build(reader)?;

            for batch_result in csv_reader {
                if is_writer_full(writer_idx) && !writers.is_empty() {
                    writer = csv::Writer::new(writers.remove(0));
                    writer_idx += 1;
                }

                let batch = batch_result?;
                writer.write(&batch)?;
            }
        }

        Ok(())
    }
}
