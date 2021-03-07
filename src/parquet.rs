use std::sync::Arc;

use anyhow::Result;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::file::footer;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, SerializedFileReader};
use parquet::file::writer::ParquetWriter;
use parquet::schema::types::Type as ParquetType;

use crate::base::Bytes;
use crate::state::{ObjectState, ParquetFormatState};

pub struct Parquet {}

impl Parquet {
    const BATCH_SIZE: usize = 2048 * 100;

    pub fn read_object_state<R: ChunkReader>(reader: &R) -> Result<ObjectState> {
        let meta = footer::parse_metadata(reader)?;
        let format_state =
            ParquetFormatState::new(Self::parquet_type(&meta), Self::row_count(&meta));
        Ok(ObjectState::new_parquet(
            format_state,
            Self::file_size(&meta),
        ))
    }

    pub fn combine_objects<R: 'static + ChunkReader, W: 'static + ParquetWriter>(
        mut readers: Vec<R>,
        mut writers: Vec<W>,
        target_rows: usize,
    ) -> Result<()> {
        let mut writer_count = 0;

        loop {
            let file_reader = SerializedFileReader::new(readers.remove(0))?;
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
            let record_reader = arrow_reader.get_record_reader(Self::BATCH_SIZE)?;

            let schema = record_reader.schema();
            let mut arrow_writer = ArrowWriter::try_new(writers.remove(0), schema.clone(), None)?;

            for batch_result in record_reader {
                if writer_count >= target_rows && !writers.is_empty() {
                    arrow_writer.close()?;
                    arrow_writer = ArrowWriter::try_new(writers.remove(0), schema.clone(), None)?;
                    writer_count = 0;
                }

                let batch = batch_result?;
                arrow_writer.write(&batch)?;
                writer_count += batch.num_rows();
            }

            if readers.is_empty() {
                arrow_writer.close()?;
                break;
            }
        }

        Ok(())
    }

    fn row_count(meta: &ParquetMetaData) -> usize {
        meta.file_metadata().num_rows() as usize
    }

    fn file_size(meta: &ParquetMetaData) -> Bytes {
        meta.row_groups()
            .iter()
            .map(|group| Bytes::new(group.total_byte_size() as usize))
            .fold(Bytes::new(0), |acc, bytes| acc + bytes)
    }

    fn parquet_type(meta: &ParquetMetaData) -> ParquetType {
        // FIXME: Handle empty files
        meta.row_groups()[0].schema_descr().root_schema().clone()
    }
}
