use std::sync::Arc;

use arrow::array::{
    AsArray, BinaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
};

pub trait InternalRow {
    /// Returns the number of fields in this row
    fn get_field_count(&self) -> usize;

    /// Returns true if the element is null at the given position
    fn is_null_at(&self, pos: usize) -> bool;

    /// Returns the boolean value at the given position
    fn get_boolean(&self, pos: usize) -> bool;

    /// Returns the byte value at the given position
    fn get_byte(&self, pos: usize) -> i8;

    /// Returns the short value at the given position
    fn get_short(&self, pos: usize) -> i16;

    /// Returns the integer value at the given position
    fn get_int(&self, pos: usize) -> i32;

    /// Returns the long value at the given position
    fn get_long(&self, pos: usize) -> i64;

    /// Returns the float value at the given position
    fn get_float(&self, pos: usize) -> f32;

    /// Returns the double value at the given position
    fn get_double(&self, pos: usize) -> f64;

    /// Returns the string value at the given position with fixed length
    fn get_char(&self, pos: usize, length: usize) -> String;

    /// Returns the string value at the given position
    fn get_string(&self, pos: usize) -> String;

    // /// Returns the decimal value at the given position
    // fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Decimal;

    // /// Returns the timestamp value at the given position
    // fn get_timestamp_ntz(&self, pos: usize, precision: usize) -> TimestampNtz;

    // /// Returns the timestamp value at the given position
    // fn get_timestamp_ltz(&self, pos: usize, precision: usize) -> TimestampLtz;

    /// Returns the binary value at the given position with fixed length
    fn get_binary(&self, pos: usize, length: usize) -> Vec<u8>;

    /// Returns the binary value at the given position
    fn get_bytes(&self, pos: usize) -> Vec<u8>;
}

pub struct ColumnarRow {
    record_batch: Arc<RecordBatch>,
    row_id: usize,
}

impl ColumnarRow {
    pub fn new(batch: Arc<RecordBatch>) -> Self {
        ColumnarRow {
            record_batch: batch,
            row_id: 0,
        }
    }

    pub fn new_with_row_id(bach: Arc<RecordBatch>, row_id: usize) -> Self {
        ColumnarRow {
            record_batch: bach,
            row_id,
        }
    }

    pub fn set_row_id(&mut self, row_id: usize) {
        self.row_id = row_id
    }
}

impl InternalRow for ColumnarRow {
    fn get_field_count(&self) -> usize {
        self.record_batch.num_columns()
    }

    fn is_null_at(&self, pos: usize) -> bool {
        self.record_batch.column(pos).is_null(self.row_id)
    }

    fn get_boolean(&self, pos: usize) -> bool {
        self.record_batch
            .column(pos)
            .as_boolean()
            .value(self.row_id)
    }

    fn get_byte(&self, pos: usize) -> i8 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Expect byte array")
            .value(self.row_id)
    }

    fn get_short(&self, pos: usize) -> i16 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Expect short array")
            .value(self.row_id)
    }

    fn get_int(&self, pos: usize) -> i32 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expect int array")
            .value(self.row_id)
    }

    fn get_long(&self, pos: usize) -> i64 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expect long array")
            .value(self.row_id)
    }

    fn get_float(&self, pos: usize) -> f32 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Expect float32 array")
            .value(self.row_id)
    }

    fn get_double(&self, pos: usize) -> f64 {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Expect float64 array")
            .value(self.row_id)
    }

    fn get_char(&self, pos: usize, length: usize) -> String {
        let array = self
            .record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected fixed-size binary array for char type");

        let bytes = array.value(self.row_id);
        if bytes.len() != length {
            panic!(
                "Length mismatch for fixed-size char: expected {}, got {}",
                length,
                bytes.len()
            );
        }

        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
    }

    fn get_string(&self, pos: usize) -> String {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected String array.")
            .value(self.row_id)
            .to_string()
    }

    fn get_binary(&self, pos: usize, length: usize) -> Vec<u8> {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected binary array.")
            .value(self.row_id)
            .to_vec()
    }

    fn get_bytes(&self, pos: usize) -> Vec<u8> {
        self.record_batch
            .column(pos)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Expected bytes array.")
            .value(self.row_id)
            .to_vec()
    }
}
