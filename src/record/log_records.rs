use std::{
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    ipc::{
        reader::StreamReader,
        writer::StreamWriter,
    },
};
use arrow_schema::SchemaRef;
use arrow_schema::{DataType as ArrowDataType, Field, Schema};
use byteorder::WriteBytesExt;
use byteorder::{ByteOrder, LittleEndian};
use crc32c::crc32c;
use tokio::io::AsyncReadExt;

use crate::{Result, metadata::DataType};

use super::{
    ChangeType, ScanRecord,
    row::ColumnarRow,
};

/// const for record batch
pub const BASE_OFFSET_LENGTH: usize = 8;
pub const LENGTH_LENGTH: usize = 4;
pub const MAGIC_LENGTH: usize = 1;
pub const COMMIT_TIMESTAMP_LENGTH: usize = 8;
pub const CRC_LENGTH: usize = 4;
pub const SCHEMA_ID_LENGTH: usize = 2;
pub const ATTRIBUTE_LENGTH: usize = 1;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const WRITE_CLIENT_ID_LENGTH: usize = 8;
pub const BATCH_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_LENGTH: usize = 4;

pub const BASE_OFFSET_OFFSET: usize = 0;
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const MAGIC_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const COMMIT_TIMESTAMP_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_OFFSET: usize = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
pub const SCHEMA_ID_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTES_OFFSET: usize = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const WRITE_CLIENT_ID_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BATCH_SEQUENCE_OFFSET: usize = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
pub const RECORDS_COUNT_OFFSET: usize = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

pub const RECORD_BATCH_HEADER_SIZE: usize = RECORDS_OFFSET;
pub const ARROW_CHANGETYPE_OFFSET: usize = RECORD_BATCH_HEADER_SIZE;
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

/// const for record
/// The "magic" values.
#[derive(Debug, Clone, Copy)]
pub enum LogMagicValue {
    V0 = 0,
}

pub const CURRENT_LOG_MAGIC_VALUE: u8 = LogMagicValue::V0 as u8;

/// Value used if writer ID is not available or non-idempotent.
pub const NO_WRITER_ID: i64 = -1;

/// Value used if batch sequence is not available.
pub const NO_BATCH_SEQUENCE: i32 = -1;

pub const BUILDER_DEFAULT_OFFSET: i64 = 0;

pub struct MemoryLogRecordsArrowBuilder<'a> {
    base_log_offset: i64,
    schema_id: i32,
    magic: u8,
    writer_id: i64,
    batch_sequence: i32,
    record_count: i32,
    arrow_record_batch: &'a RecordBatch,
}

impl<'a> MemoryLogRecordsArrowBuilder<'a> {
    pub fn new(schema_id: i32, arrow_record_batch: &'a RecordBatch) -> Self {
        MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic: CURRENT_LOG_MAGIC_VALUE,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            record_count: arrow_record_batch.num_rows() as i32,
            arrow_record_batch,
        }
    }

    pub fn build(&mut self) -> Result<Vec<u8>> {
        // serialize arrow batch
        let mut arrow_batch_bytes = vec![];
        let mut writer =
            StreamWriter::try_new(&mut arrow_batch_bytes, &self.arrow_record_batch.schema())?;
        // get header len
        let header = writer.get_ref().len();
        writer.write(self.arrow_record_batch)?;
        // get real arrow batch bytes
        let real_arrow_batch_bytes = &arrow_batch_bytes[header..];

        // now, write batch header and arrow batch
        let mut batch_bytes = vec![0u8; RECORD_BATCH_HEADER_SIZE + real_arrow_batch_bytes.len()];
        // write batch header
        self.write_batch_header(&mut batch_bytes[..])?;

        // write arrow batch byts
        let mut cursor = Cursor::new(&mut batch_bytes[..]);
        cursor.set_position(RECORD_BATCH_HEADER_SIZE as u64);
        cursor.write_all(real_arrow_batch_bytes)?;

        let calcute_crc_bytes = &cursor.get_ref()[SCHEMA_ID_OFFSET..];
        // then update crc
        let crc = crc32c(calcute_crc_bytes);
        cursor.set_position(CRC_OFFSET as u64);
        cursor.write_u32::<LittleEndian>(crc)?;

        Ok(batch_bytes.to_vec())
    }

    fn write_batch_header(&self, buffer: &mut [u8]) -> Result<()> {
        let total_len = buffer.len();
        let mut cursor = Cursor::new(buffer);
        cursor.write_i64::<LittleEndian>(self.base_log_offset)?;
        cursor
            .write_i32::<LittleEndian>((total_len - BASE_OFFSET_LENGTH - LENGTH_LENGTH) as i32)?;
        cursor.write_u8(self.magic)?;
        cursor.write_i64::<LittleEndian>(0)?; // timestamp placeholder
        cursor.write_u32::<LittleEndian>(0)?; // crc placeholder
        cursor.write_i16::<LittleEndian>(self.schema_id as i16)?;

        // todo: curerntly, always is append only
        let append_only = true;
        cursor.write_u8(if append_only { 1 } else { 0 })?;
        cursor.write_i32::<LittleEndian>(if self.record_count > 0 {
            self.record_count - 1
        } else {
            0
        })?;

        cursor.write_i64::<LittleEndian>(self.writer_id)?;
        cursor.write_i32::<LittleEndian>(self.batch_sequence)?;
        cursor.write_i32::<LittleEndian>(self.record_count)?;
        Ok(())
    }
}

pub struct LogRecordsBatchs<'a> {
    data: &'a [u8],
    current_pos: usize,
    remaining_bytes: usize,
}

impl<'a> LogRecordsBatchs<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let remaining_bytes: usize = data.len();
        Self {
            data,
            current_pos: 0,
            remaining_bytes,
        }
    }

    pub fn next_batch_size(&self) -> Option<usize> {
        if self.remaining_bytes < LOG_OVERHEAD {
            return None;
        }

        let batch_size_bytes =
            LittleEndian::read_i32(self.data.get(self.current_pos + LENGTH_OFFSET..).unwrap());
        Some(batch_size_bytes as usize + LOG_OVERHEAD)
    }
}

impl<'a> Iterator for &'a mut LogRecordsBatchs<'a> {
    type Item = LogRecordBatch<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch_size() {
            Some(batch_size) => {
                let data_slice = &self.data[self.current_pos..self.current_pos + batch_size];
                let record_batch = LogRecordBatch::new(data_slice);
                self.current_pos += batch_size;
                self.remaining_bytes -= batch_size;
                Some(record_batch)
            }
            None => None,
        }
    }
}

pub struct LogRecordBatch<'a> {
    data: &'a [u8],
}

impl<'a> LogRecordBatch<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        LogRecordBatch { data }
    }

    pub fn magic(&self) -> u8 {
        self.data[MAGIC_OFFSET]
    }

    pub fn commit_timestamp(&self) -> i64 {
        let offset = COMMIT_TIMESTAMP_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + COMMIT_TIMESTAMP_LENGTH])
    }

    pub fn writer_id(&self) -> i64 {
        let offset = WRITE_CLIENT_ID_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + WRITE_CLIENT_ID_LENGTH])
    }

    pub fn batch_sequence(&self) -> i32 {
        let offset = BATCH_SEQUENCE_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + BATCH_SEQUENCE_LENGTH])
    }

    pub fn ensure_valid(&self) -> Result<()> {
        // todo
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.size_in_bytes() >= RECORD_BATCH_HEADER_SIZE
            && self.checksum() == self.compute_checksum()
    }

    fn compute_checksum(&self) -> u32 {
        let start = SCHEMA_ID_OFFSET;
        let end = start + self.data.len();
        crc32c(&self.data[start..end])
    }

    fn attributes(&self) -> u8 {
        self.data[ATTRIBUTES_OFFSET]
    }

    pub fn next_log_offset(&self) -> i64 {
        self.last_log_offset() + 1
    }

    pub fn checksum(&self) -> u32 {
        let offset = CRC_OFFSET;
        LittleEndian::read_u32(&self.data[offset..offset + CRC_OFFSET])
    }

    pub fn schema_id(&self) -> i16 {
        let offset = SCHEMA_ID_OFFSET;
        LittleEndian::read_i16(&self.data[offset..offset + SCHEMA_ID_OFFSET])
    }

    pub fn base_log_offset(&self) -> i64 {
        let offset = BASE_OFFSET_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + BASE_OFFSET_LENGTH])
    }

    pub fn last_log_offset(&self) -> i64 {
        self.base_log_offset() + self.last_offset_delta() as i64
    }

    fn last_offset_delta(&self) -> i32 {
        let offset = LAST_OFFSET_DELTA_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LAST_OFFSET_DELTA_LENGTH])
    }

    pub fn size_in_bytes(&self) -> usize {
        let offset = LENGTH_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LENGTH_LENGTH]) as usize + LOG_OVERHEAD
    }

    pub fn record_count(&self) -> i32 {
        let offset = RECORDS_COUNT_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + RECORDS_COUNT_LENGTH])
    }

    pub fn records(&self, read_context: ReadContext) -> LogRecordIterator {
        let count = self.record_count();
        if count == 0 {
            return LogRecordIterator::empty();
        }

        // get arrow_metadata
        let arrow_metadata_bytes = read_context.to_arrow_metadata().unwrap();
        // arrow_batch_data
        let data = &self.data[RECORDS_OFFSET..];

        // need to combine arrow_metadata_bytes + arrow_batch_data
        let cursor = Cursor::new([&arrow_metadata_bytes, data].concat());
        let stream_reader = StreamReader::try_new(cursor, None).unwrap();

        let mut record_batch = None;
        for bath in stream_reader {
            record_batch = Some(bath.unwrap());
            // todo: check only one?
            break;
        }

        if record_batch.is_none() {
            return LogRecordIterator::empty();
        }

        let arrow_reader = ArrowReader::new(Arc::new(record_batch.unwrap()));
        LogRecordIterator::Arrow(ArrowLogRecordIterator {
            reader: arrow_reader,
            base_offset: self.base_log_offset(),
            timestamp: self.commit_timestamp(),
            row_id: 0,
            change_type: ChangeType::AppendOnly,
        })
    }
}

pub fn to_arrow_schema(fluss_schema: &DataType) -> SchemaRef {
    
    match &fluss_schema {
        DataType::Row(row_type) => {
            let fields: Vec<Field> = row_type
                .fields()
                .iter()
                .map(|f| {
                    
                    Field::new(
                        f.name(),
                        to_arrow_type(f.data_type()),
                        f.data_type().is_nullable(),
                    )
                })
                .collect();

            SchemaRef::new(Schema::new(fields))
        }
        _ => {
            panic!("must be row data tyoe.")
        }
    }
}

pub fn to_arrow_type(fluss_type: &DataType) -> ArrowDataType {
    match fluss_type {
        DataType::Boolean(_) => ArrowDataType::Boolean,
        DataType::TinyInt(_) => ArrowDataType::Int8,
        DataType::SmallInt(_) => ArrowDataType::Int16,
        DataType::BigInt(_) => ArrowDataType::Int64,
        DataType::Int(_) => ArrowDataType::Int32,
        DataType::Float(_) => ArrowDataType::Float32,
        DataType::Double(_) => ArrowDataType::Float64,
        DataType::Char(_) => ArrowDataType::Utf8,
        DataType::String(_) => ArrowDataType::Utf8,
        DataType::Decimal(_) => todo!(),
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(_) => todo!(),
        DataType::Timestamp(_) => todo!(),
        DataType::TimestampLTz(_) => todo!(),
        DataType::Bytes(_) => todo!(),
        DataType::Binary(_) => todo!(),
        DataType::Array(data_type) => todo!(),
        DataType::Map(data_type) => todo!(),
        DataType::Row(data_fields) => todo!(),
    }
}

pub struct ReadContext {
    arrow_schema: SchemaRef,
}

impl ReadContext {
    pub fn new(arrow_schema: SchemaRef) -> ReadContext {
        ReadContext {
            arrow_schema,
        }
    }

    pub fn to_arrow_metadata(&self) -> Result<Vec<u8>> {
        let mut arrow_schema_bytes = vec![];
        let writer = StreamWriter::try_new(&mut arrow_schema_bytes, &self.arrow_schema)?;
        Ok(arrow_schema_bytes)
    }
}

pub enum LogRecordIterator {
    Empty,
    Arrow(ArrowLogRecordIterator),
}

impl LogRecordIterator {
    pub fn empty() -> Self {
        LogRecordIterator::Empty
    }
}

impl Iterator for LogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LogRecordIterator::Empty => None,
            LogRecordIterator::Arrow(iter) => iter.next(),
        }
    }
}

pub struct ArrowLogRecordIterator {
    reader: ArrowReader,
    base_offset: i64,
    timestamp: i64,
    row_id: usize,
    change_type: ChangeType,
}

impl ArrowLogRecordIterator {
    fn new(reader: ArrowReader, base_offset: i64, timestamp: i64, change_type: ChangeType) -> Self {
        Self {
            reader,
            base_offset,
            timestamp,
            row_id: 0,
            change_type,
        }
    }
}

impl Iterator for ArrowLogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_id >= self.reader.row_count() {
            return None;
        }

        let columnar_row = self.reader.read(self.row_id);
        let scan_record = ScanRecord::new(
            columnar_row,
            self.base_offset + self.row_id as i64,
            self.timestamp,
            self.change_type,
        );
        self.row_id += 1;
        Some(scan_record)
    }
}

pub struct ArrowReader {
    record_batch: Arc<RecordBatch>,
}

impl ArrowReader {
    pub fn new(record_batch: Arc<RecordBatch>) -> Self {
        ArrowReader {
            record_batch,
        }
    }

    pub fn row_count(&self) -> usize {
        self.record_batch.num_rows()
    }

    pub fn read(&self, row_id: usize) -> ColumnarRow {
        ColumnarRow::new_with_row_id(self.record_batch.clone(), row_id)
    }
}
pub struct MyVec<T>(pub StreamReader<T>);

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use arrow::{
        array::record_batch,
        ipc::{
            reader::StreamReader,
            writer::StreamWriter,
        },
    };

    use crate::record::{
        log_records::{LogRecordBatch, ReadContext},
        row::InternalRow,
    };

    use super::MemoryLogRecordsArrowBuilder;

    #[test]
    pub fn t1() {
        let schema_id = 1;
        let arrow_record_batch =
            record_batch!(("c1", Int32, [1, 2]), ("c2", Utf8, ["a1", "a2"])).unwrap();
        let data = MemoryLogRecordsArrowBuilder::new(schema_id, &arrow_record_batch)
            .build()
            .unwrap();

        let log_record_batch = LogRecordBatch::new(&data);

        let read_context = ReadContext::new(arrow_record_batch.schema());

        let log_records = log_record_batch.records(read_context);
        for record in log_records {
            let row = record.row;
            println!("{}, {}", row.get_int(0), row.get_string(1))
        }
    }

    #[test]
    pub fn f2() {
        let arrow_record_batch =
            record_batch!(("c1", Int32, [1, 2]), ("c2", Utf8, ["a1", "a2"])).unwrap();
        let mut arrow_batch_bytes = vec![];
        let mut writer =
            StreamWriter::try_new(&mut arrow_batch_bytes, &arrow_record_batch.schema()).unwrap();
        // get header len
        let header = writer.get_ref().len();
        writer.write(&arrow_record_batch).unwrap();
        // get real arrow batch bytes
        let real_arrow_batch_bytes = &arrow_batch_bytes[header..];

        let mut arrow_schema_bytes = vec![];
        let writer =
            StreamWriter::try_new(&mut arrow_schema_bytes, &arrow_record_batch.schema()).unwrap();

        let cursor = Cursor::new([&arrow_schema_bytes, real_arrow_batch_bytes].concat());
        let stream_reader = StreamReader::try_new(cursor, None).unwrap();
    }
}
