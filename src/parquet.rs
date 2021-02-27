use parquet::errors::Result;
use parquet::file::footer;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;
use parquet::schema::types::Type as ParquetType;

use crate::base::Bytes;
use crate::state::ObjectState;

pub fn read_object_state<R: ChunkReader>(reader: &R) -> Result<ObjectState> {
    let meta = footer::parse_metadata(reader)?;
    Ok(ObjectState::new_parquet(row_count(&meta), file_size(&meta), parquet_type(&meta)))
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
