use crate::record::row::InternalRow;
use std::time::{SystemTime, UNIX_EPOCH};

mod admin;
pub mod connection;
mod metadata;
pub mod table;
pub mod write;

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct GenericRow {
    pub values: Vec<i32>,
}

impl InternalRow for GenericRow {
    fn get_field_count(&self) -> usize {
        self.values.len()
    }

    fn is_null_at(&self, pos: usize) -> bool {
        false
    }

    fn get_boolean(&self, pos: usize) -> bool {
        todo!()
    }

    fn get_byte(&self, pos: usize) -> i8 {
        todo!()
    }

    fn get_short(&self, pos: usize) -> i16 {
        todo!()
    }

    fn get_int(&self, pos: usize) -> i32 {
        self.values[pos]
    }

    fn get_long(&self, pos: usize) -> i64 {
        todo!()
    }

    fn get_float(&self, pos: usize) -> f32 {
        todo!()
    }

    fn get_double(&self, pos: usize) -> f64 {
        todo!()
    }

    fn get_char(&self, pos: usize, length: usize) -> String {
        todo!()
    }

    fn get_string(&self, pos: usize) -> String {
        todo!()
    }

    fn get_binary(&self, pos: usize, length: usize) -> Vec<u8> {
        todo!()
    }

    fn get_bytes(&self, pos: usize) -> Vec<u8> {
        todo!()
    }
}
