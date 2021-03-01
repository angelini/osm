use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader, ArrowWriter};
use parquet::errors::Result;
use parquet::file::footer;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, SerializedFileReader};
use parquet::file::writer::{ParquetWriter};
use parquet::schema::types::Type as ParquetType;

use crate::base::Bytes;
use crate::state::ObjectState;

const BATCH_SIZE: usize = 2048 * 100;

pub fn combine_objects<R: 'static + ChunkReader, W: 'static + ParquetWriter>(
    mut readers: Vec<R>,
    mut writers: Vec<W>,
    rows_per_file: usize,
) -> Result<()> {
    let mut writer_count = 0;

    let file_reader = SerializedFileReader::new(readers.remove(0))?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    loop {
        let record_reader = arrow_reader.get_record_reader(BATCH_SIZE)?;

        let schema = record_reader.schema();
        let mut arrow_writer = ArrowWriter::try_new(writers.remove(0), schema.clone(), None)?;

        for batch_result in record_reader {
            if writer_count >= rows_per_file {
                assert!(!writers.is_empty(), "record batches left with all full writers");
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

pub fn read_object_state<R: ChunkReader>(reader: &R) -> Result<ObjectState> {
    let meta = footer::parse_metadata(reader)?;
    Ok(ObjectState::new_parquet(
        row_count(&meta),
        file_size(&meta),
        parquet_type(&meta),
    ))
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
