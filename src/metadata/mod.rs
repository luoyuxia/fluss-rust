use core::fmt;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter},
    sync::Arc,
};

use linked_hash_map::VacantEntry;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::Result;
pub mod metadata_serde;
pub mod metadata_updater;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct TablePath {
    database: String,
    table: String,
}

impl TablePath {
    pub fn new(db: String, tbl: String) -> Self {
        return TablePath {
            database: db,
            table: tbl,
        };
    }

    #[inline]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[inline]
    pub fn table(&self) -> &str {
        &self.table
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean(BooleanType),
    TinyInt(TinyIntType),
    SmallInt(SmallIntType),
    Int(IntType),
    BigInt(BigIntType),
    Float(FloatType),
    Double(DoubleType),
    Char(CharType),
    String(StringType),
    Decimal(DecimalType),
    Date(DateType),
    Time(TimeType),
    Timestamp(TimestampType),
    TimestampLTz(TimestampLTzType),
    Bytes(BytesType),
    Binary(BinaryType),
    Array(ArrayType),
    Map(MapType),
    Row(RowType),
}

impl DataType {
    pub fn is_nullable(&self) -> bool {
        match self {
            DataType::Boolean(v) => v.nullable,
            DataType::TinyInt(v) => v.nullable,
            DataType::SmallInt(v) => v.nullable,
            DataType::Int(v) => v.nullable,
            DataType::BigInt(v) => v.nullable,
            DataType::Decimal(v) => v.nullable,
            DataType::Double(v) => v.nullable,
            DataType::Float(v) => v.nullable,
            DataType::Binary(v) => v.nullable,
            DataType::Char(v) => v.nullable,
            DataType::String(v) => v.nullable,
            DataType::Date(v) => v.nullable,
            DataType::TimestampLTz(v) => v.nullable,
            DataType::Time(v) => v.nullable,
            DataType::Timestamp(v) => v.nullable,
            DataType::Array(v) => v.nullable,
            DataType::Map(v) => v.nullable,
            DataType::Row(v) => v.nullable,
            DataType::Bytes(v) => v.nullable,
        }
    }

    fn as_non_nullable(&self) -> Self {
        match self {
            DataType::Boolean(v) => DataType::Boolean(v.as_non_nullable()),
            DataType::TinyInt(v) => DataType::TinyInt(v.as_non_nullable()),
            DataType::SmallInt(v) => DataType::SmallInt(v.as_non_nullable()),
            DataType::Int(v) => DataType::Int(v.as_non_nullable()),
            DataType::BigInt(v) => DataType::BigInt(v.as_non_nullable()),
            DataType::Decimal(v) => DataType::Decimal(v.as_non_nullable()),
            DataType::Double(v) => DataType::Double(v.as_non_nullable()),
            DataType::Float(v) => DataType::Float(v.as_non_nullable()),
            DataType::Binary(v) => DataType::Binary(v.as_non_nullable()),
            DataType::Char(v) => DataType::Char(v.as_non_nullable()),
            DataType::String(v) => DataType::String(v.as_non_nullable()),
            DataType::Date(v) => DataType::Date(v.as_non_nullable()),
            DataType::TimestampLTz(v) => DataType::TimestampLTz(v.as_non_nullable()),
            DataType::Time(v) => DataType::Time(v.as_non_nullable()),
            DataType::Timestamp(v) => DataType::Timestamp(v.as_non_nullable()),
            DataType::Array(v) => DataType::Array(v.as_non_nullable()),
            DataType::Map(v) => DataType::Map(v.as_non_nullable()),
            DataType::Row(v) => DataType::Row(v.as_non_nullable()),
            DataType::Bytes(v) => DataType::Bytes(v.as_non_nullable()),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
        // write!(f, "({}, {})", self.longitude, self.latitude)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BooleanType {
    nullable: bool,
}

impl BooleanType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TinyIntType {
    nullable: bool,
}

impl TinyIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SmallIntType {
    nullable: bool,
}

impl SmallIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IntType {
    nullable: bool,
}

impl IntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BigIntType {
    nullable: bool,
}

impl BigIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FloatType {
    nullable: bool,
}

impl FloatType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DoubleType {
    nullable: bool,
}

impl DoubleType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CharType {
    nullable: bool,
    length: u32,
}

impl CharType {
    pub fn new(length: u32) -> Self {
        Self::with_nullable(length, true)
    }

    pub fn with_nullable(length: u32, nullable: bool) -> Self {
        Self { nullable, length }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(self.length, false)
    }
}

impl Display for CharType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CHAR({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StringType {
    nullable: bool,
}

impl StringType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DecimalType {
    nullable: bool,
    precision: u32,
    scale: u32,
}

impl DecimalType {
    pub const MIN_PRECISION: u32 = 1;

    pub const MAX_PRECISION: u32 = 38;

    pub const DEFAULT_PRECISION: u32 = 10;

    pub const MIN_SCALE: u32 = 0;

    pub const DEFAULT_SCALE: u32 = 0;

    pub fn new(precision: u32, scale: u32) -> Self {
        Self::with_nullable(true, precision, scale)
    }

    pub fn with_nullable(nullable: bool, precision: u32, scale: u32) -> Self {
        DecimalType {
            nullable,
            precision,
            scale,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision, self.scale)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateType {
    nullable: bool,
}

impl DateType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimeType {
    nullable: bool,
    precision: u32,
}

impl TimeType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 0;

    pub fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimeType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampType {
    nullable: bool,
    precision: u32,
}

impl TimestampType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimestampType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampLTzType {
    nullable: bool,
    precision: u32,
}

impl TimestampLTzType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
    }

    pub fn new(precision: u32) -> Self {
        Self::with_nullable(true, precision)
    }

    pub fn with_nullable(nullable: bool, precision: u32) -> Self {
        TimestampLTzType {
            nullable,
            precision,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BytesType {
    nullable: bool,
}

impl BytesType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BinaryType {
    nullable: bool,
    length: usize,
}

impl BinaryType {
    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = usize::MAX;

    pub const DEFAULT_LENGTH: usize = 1;

    pub fn new(length: usize) -> Self {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: usize) -> Self {
        Self { nullable, length }
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.length)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrayType {
    nullable: bool,
    element_type: Box<DataType>,
}

impl ArrayType {
    pub fn new(element_type: DataType) -> Self {
        Self::with_nullable(true, element_type)
    }

    pub fn with_nullable(nullable: bool, element_type: DataType) -> Self {
        Self {
            nullable,
            element_type: Box::new(element_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            element_type: self.element_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct MapType {
    nullable: bool,
    key_type: Box<DataType>,
    value_type: Box<DataType>,
}

impl MapType {
    pub fn new(key_type: DataType, value_type: DataType) -> Self {
        Self::with_nullable(true, key_type, value_type)
    }

    pub fn with_nullable(nullable: bool, key_type: DataType, value_type: DataType) -> Self {
        Self {
            nullable,
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            key_type: self.key_type.clone(),
            value_type: self.value_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct RowType {
    nullable: bool,
    fields: Vec<DataField>,
}

impl RowType {
    pub const fn new(fields: Vec<DataField>) -> Self {
        Self::with_nullable(true, fields)
    }

    pub const fn with_nullable(nullable: bool, fields: Vec<DataField>) -> Self {
        Self { nullable, fields }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.fields.clone())
    }

    pub fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }
}

pub struct DataTypes;

impl DataTypes {
    pub fn binary(length: usize) -> DataType {
        DataType::Binary(BinaryType::new(length))
    }

    pub fn bytes() -> DataType {
        DataType::Bytes(BytesType::new())
    }

    pub fn boolean() -> DataType {
        DataType::Boolean(BooleanType::new())
    }

    pub fn int() -> DataType {
        DataType::Int(IntType::new())
    }

    /// Data type of a 1-byte signed integer with values from -128 to 127.
    pub fn tinyint() -> DataType {
        DataType::TinyInt(TinyIntType::new())
    }

    /// Data type of a 2-byte signed integer with values from -32,768 to 32,767.
    pub fn smallint() -> DataType {
        DataType::SmallInt(SmallIntType::new())
    }

    pub fn bigint() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    /// Data type of a 4-byte single precision floating point number.
    pub fn float() -> DataType {
        DataType::Float(FloatType::new())
    }

    /// Data type of an 8-byte double precision floating point number.
    pub fn double() -> DataType {
        DataType::Double(DoubleType::new())
    }

    pub fn char(length: u32) -> DataType {
        DataType::Char(CharType::new(length))
    }

    /// Data type of a variable-length character string.
    pub fn string() -> DataType {
        DataType::String(StringType::new())
    }

    /// Data type of a decimal number with fixed precision and scale `DECIMAL(p, s)` where
    /// `p` is the number of digits in a number (=precision) and `s` is the number of
    /// digits to the right of the decimal point in a number (=scale). `p` must have a value
    /// between 1 and 38 (both inclusive). `s` must have a value between 0 and `p` (both inclusive).
    pub fn decimal(precision: u32, scale: u32) -> DataType {
        DataType::Decimal(DecimalType::new(precision, scale))
    }

    pub fn date() -> DataType {
        DataType::Date(DateType::new())
    }

    /// Data type of a time WITHOUT time zone `TIME` with no fractional seconds by default.
    pub fn time() -> DataType {
        DataType::Time(TimeType::default())
    }

    /// Data type of a time WITHOUT time zone `TIME(p)` where `p` is the number of digits
    /// of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn time_with_precision(precision: u32) -> DataType {
        DataType::Time(TimeType::new(precision))
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP` with 6 digits of fractional
    /// seconds by default.
    pub fn timestamp() -> DataType {
        DataType::Timestamp(TimestampType::default())
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9
    /// (both inclusive).
    pub fn timestamp_with_precision(precision: u32) -> DataType {
        DataType::Timestamp(TimestampType::new(precision))
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE` with 6 digits of
    /// fractional seconds by default.
    pub fn timestamp_ltz() -> DataType {
        DataType::TimestampLTz(TimestampLTzType::default())
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn timestamp_ltz_with_precision(precision: u32) -> DataType {
        DataType::TimestampLTz(TimestampLTzType::new(precision))
    }

    /// Data type of an array of elements with same subtype.
    pub fn array(element: DataType) -> DataType {
        DataType::Array(ArrayType::new(element))
    }

    /// Data type of an associative array that maps keys to values.
    pub fn map(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Map(MapType::new(key_type, value_type))
    }

    /// Field definition with field name and data type.
    pub fn field(name: String, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
    }

    /// Field definition with field name, data type, and a description.
    pub fn field_with_description(
        name: String,
        data_type: DataType,
        description: String,
    ) -> DataField {
        DataField::new(name, data_type, Some(description))
    }

    /// Data type of a sequence of fields.
    pub fn row(fields: Vec<DataField>) -> DataType {
        DataType::Row(RowType::new(fields))
    }

    /// Data type of a sequence of fields with generated field names (f0, f1, f2, ...).
    pub fn row_from_types(field_types: Vec<DataType>) -> DataType {
        let fields = field_types
            .into_iter()
            .enumerate()
            .map(|(i, dt)| DataField::new(format!("f{}", i), dt, None))
            .collect();
        DataType::Row(RowType::new(fields))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataField {
    name: String,
    data_type: DataType,
    description: Option<String>,
}

impl DataField {
    pub fn new(name: String, data_type: DataType, description: Option<String>) -> DataField {
        DataField {
            name: name,
            data_type: data_type,
            description: description,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl fmt::Display for DataField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if let Some(desc) = &self.description {
            write!(f, " {}", desc)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    name: String,
    data_type: DataType,
    comment: Option<String>,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            comment: None,
        }
    }

    pub fn with_comment(mut self, comment: &str) -> Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn with_data_type(&self, data_type: DataType) -> Self {
        Self {
            name: self.name.clone(),
            data_type: data_type.clone(),
            comment: self.comment.clone(),
        }
    }

    // Getters...
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrimaryKey {
    constraint_name: String,
    column_names: Vec<String>,
}

impl PrimaryKey {
    pub fn new(constraint_name: &str, column_names: Vec<String>) -> Self {
        Self {
            constraint_name: constraint_name.to_string(),
            column_names,
        }
    }

    // Getters...
    pub fn constraint_name(&self) -> &str {
        &self.constraint_name
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
    // must be Row data type kind
    row_type: DataType,
}

impl Schema {
    pub fn empty() -> Self {
        Self::builder().build()
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// todo: custom from json
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        // serde::from_slice(bytes).map_err(Into::into)
        todo!()
    }

    /// todo: custom to json
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        // serde_json::to_vec(self).map_err(Into::into)
        todo!()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn primary_key(&self) -> Option<&PrimaryKey> {
        self.primary_key.as_ref()
    }

    pub fn row_type(&self) -> &DataType {
        &self.row_type
    }

    pub fn primary_key_indexes(&self) -> Vec<usize> {
        self.primary_key
            .as_ref()
            .map(|pk| {
                pk.column_names
                    .iter()
                    .filter_map(|name| self.columns.iter().position(|c| &c.name == name))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn primary_key_column_names(&self) -> Vec<&str> {
        self.primary_key
            .as_ref()
            .map(|pk| pk.column_names.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_row_type(mut self, row_type: &DataType) -> Self {
        match row_type {
            DataType::Row(row) => {
                for data_field in &row.fields {
                    self = self.column(&data_field.name, data_field.data_type.clone())
                }
                self
            }
            _ => {
                panic!("data type msut be row type")
            }
        }
    }

    pub fn column(mut self, name: &str, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type));
        self
    }

    pub fn from_columns(mut self, columns: Vec<Column>) -> Self {
        self.columns.extend_from_slice(columns.as_ref());
        self
    }

    pub fn with_comment(mut self, comment: &str) -> Self {
        if let Some(last) = self.columns.last_mut() {
            *last = last.clone().with_comment(comment);
        }
        self
    }

    pub fn primary_key(mut self, column_names: Vec<String>) -> Self {
        let constraint_name = format!("PK_{}", column_names.join("_"));
        self.primary_key_named(&constraint_name, column_names)
    }

    pub fn primary_key_named(mut self, constraint_name: &str, column_names: Vec<String>) -> Self {
        self.primary_key = Some(PrimaryKey::new(constraint_name, column_names));
        self
    }

    pub fn build(&mut self) -> Schema {
        let columns = Self::normalize_columns(&mut self.columns, self.primary_key.as_ref());

        let data_fields = columns
            .iter()
            .map(|c| DataField {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                description: c.comment.clone(),
            })
            .collect();

        Schema {
            columns: columns,
            primary_key: self.primary_key.clone(),
            row_type: DataType::Row(RowType::new(data_fields)),
        }
    }

    fn normalize_columns(columns: &mut [Column], primary_key: Option<&PrimaryKey>) -> Vec<Column> {
        let names: Vec<_> = columns.iter().map(|c| &c.name).collect();
        if let Some(duplicates) = Self::find_duplicates(&names) {
            panic!("Duplicate column names found: {:?}", duplicates);
        }

        let Some(pk) = primary_key else {
            return columns.to_vec();
        };

        let pk_set: HashSet<_> = pk.column_names.iter().collect();
        let all_columns: HashSet<_> = columns.iter().map(|c| &c.name).collect();
        if !pk_set.is_subset(&all_columns) {
            panic!("Primary key columns not found in schema");
        }

        columns
            .iter()
            .map(|col| {
                if pk_set.contains(&col.name) && col.data_type.is_nullable() {
                    col.with_data_type(col.data_type.as_non_nullable())
                } else {
                    col.clone()
                }
            })
            .collect()
    }

    fn find_duplicates<'a>(names: &'a [&String]) -> Option<HashSet<&'a String>> {
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();

        for name in names {
            if !seen.insert(name) {
                duplicates.insert(*name);
            }
        }

        if duplicates.is_empty() {
            None
        } else {
            Some(duplicates)
        }
    }
}

/// distribution of table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDistribution {
    bucket_count: Option<i32>,
    bucket_keys: Vec<String>,
}

impl TableDistribution {
    pub fn bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn bucket_count(&self) -> Option<i32> {
        self.bucket_count
    }
}

#[derive(Debug, Default)]
pub struct TableDescriptorBuilder {
    schema: Option<Schema>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
    partition_keys: Vec<String>,
    comment: Option<String>,
    table_distribution: Option<TableDistribution>,
}

impl TableDescriptorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn log_format(mut self, log_format: LogFormat) -> Self {
        self.properties
            .insert("table.log.format".to_string(), log_format.to_string());
        self
    }

    pub fn kv_format(mut self, kv_format: KvFormat) -> Self {
        self.properties
            .insert("table.kv.format".to_string(), kv_format.to_string());
        self
    }

    pub fn property<T: ToString>(mut self, key: &str, value: T) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties.extend(properties);
        self
    }

    pub fn custom_property(mut self, key: &str, value: &str) -> Self {
        self.custom_properties
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn custom_properties(mut self, custom_properties: HashMap<String, String>) -> Self {
        self.custom_properties.extend(custom_properties);
        self
    }

    pub fn partitioned_by(mut self, partition_keys: Vec<String>) -> Self {
        self.partition_keys = partition_keys;
        self
    }

    pub fn distributed_by(mut self, bucket_count: Option<i32>, bucket_keys: Vec<String>) -> Self {
        self.table_distribution = Some(TableDistribution {
            bucket_count,
            bucket_keys,
        });
        self
    }

    pub fn comment(mut self, comment: &str) -> Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn build(self) -> TableDescriptor {
        let schema = self.schema.expect("Schema must be set");
        let table_distribution = TableDescriptor::normalize_distribution(
            &schema,
            &self.partition_keys,
            self.table_distribution,
        );
        TableDescriptor {
            schema,
            comment: self.comment,
            partition_keys: self.partition_keys,
            table_distribution: table_distribution,
            properties: self.properties,
            custom_properties: self.custom_properties,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDescriptor {
    schema: Schema,
    comment: Option<String>,
    partition_keys: Vec<String>,
    table_distribution: Option<TableDistribution>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
}

impl TableDescriptor {
    pub fn builder() -> TableDescriptorBuilder {
        TableDescriptorBuilder::new()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn bucket_keys(&self) -> Vec<&str> {
        self.table_distribution
            .as_ref()
            .map(|td| td.bucket_keys.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn is_default_bucket_key(&self) -> bool {
        if self.schema.primary_key().is_some() {
            self.bucket_keys()
                == Self::default_bucket_key_of_primary_key_table(
                    self.schema(),
                    &self.partition_keys,
                )
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
        } else {
            self.bucket_keys().is_empty()
        }
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn has_primary_key(&self) -> bool {
        self.schema.primary_key().is_some()
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn table_distribution(&self) -> Option<&TableDistribution> {
        self.table_distribution.as_ref()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn replication_factor(&self) -> Result<i32> {
        self.properties
            .get("table.replication.factor")
            .ok_or_else(|| anyhow::anyhow!("Replication factor is not set"))
            .map_err(|e| {
                crate::Error::InvalidTableError("Replication factor is not set".to_string())
            })?
            .parse()
            .map_err(|e| {
                crate::Error::InvalidTableError(
                    "Replication factor can't be convert into int".to_string(),
                )
            })
    }

    pub fn with_properties(&self, new_properties: HashMap<String, String>) -> Self {
        Self {
            properties: new_properties,
            ..self.clone()
        }
    }

    pub fn with_replication_factor(&self, new_replication_factor: i32) -> Self {
        let mut properties = self.properties.clone();
        properties.insert(
            "table.replication.factor".to_string(),
            new_replication_factor.to_string(),
        );
        self.with_properties(properties)
    }

    pub fn with_bucket_count(&self, new_bucket_count: i32) -> Self {
        Self {
            table_distribution: Some(TableDistribution {
                bucket_count: Some(new_bucket_count),
                bucket_keys: self
                    .table_distribution
                    .as_ref()
                    .map(|td| td.bucket_keys.clone())
                    .unwrap_or_default(),
            }),
            ..self.clone()
        }
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        // serde_json::to_vec(self).map_err(Into::into)
        todo!()
    }

    /// 从JSON字节数组反序列化
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        // serde_json::from_slice(bytes).map_err(Into::into)
        todo!()
    }

    fn default_bucket_key_of_primary_key_table(
        schema: &Schema,
        partition_keys: &[String],
    ) -> Vec<String> {
        let mut bucket_keys = schema
            .primary_key()
            .expect("Primary key must be set")
            .column_names()
            .to_vec();

        bucket_keys.retain(|k| !partition_keys.contains(k));

        if bucket_keys.is_empty() {
            panic!(
                "Primary Key constraint {:?} should not be same with partition fields {:?}.",
                schema.primary_key().unwrap().column_names(),
                partition_keys
            );
        }

        bucket_keys
    }

    fn normalize_distribution(
        schema: &Schema,
        partition_keys: &[String],
        origin_distribution: Option<TableDistribution>,
    ) -> Option<TableDistribution> {
        if let Some(mut distribution) = origin_distribution {
            if distribution
                .bucket_keys
                .iter()
                .any(|k| partition_keys.contains(k))
            {
                panic!(
                    "Bucket key {:?} shouldn't include any column in partition keys {:?}.",
                    distribution.bucket_keys, partition_keys
                );
            }

            if let Some(pk) = schema.primary_key() {
                if distribution.bucket_keys.is_empty() {
                    return Some(TableDistribution {
                        bucket_count: distribution.bucket_count,
                        bucket_keys: Self::default_bucket_key_of_primary_key_table(
                            schema,
                            partition_keys,
                        ),
                    });
                } else {
                    let pk_columns: HashSet<_> = pk.column_names().iter().collect();
                    if !distribution
                        .bucket_keys
                        .iter()
                        .all(|k| pk_columns.contains(k))
                    {
                        panic!(
                            "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. \
                            The primary keys are {:?}, the partition keys are {:?}, but the user-defined bucket keys are {:?}.",
                            pk.column_names(),
                            partition_keys,
                            distribution.bucket_keys
                        );
                    }
                    return Some(distribution);
                }
            } else {
                return Some(distribution);
            }
        } else if schema.primary_key().is_some() {
            // 如果有主键但未设置分布，则设置默认桶键
            return Some(TableDistribution {
                bucket_count: None,
                bucket_keys: Self::default_bucket_key_of_primary_key_table(schema, partition_keys),
            });
        }

        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogFormat {
    ARROW,
    INDEXED,
}

impl Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::ARROW => {
                write!(f, "ARROW")?;
            }
            LogFormat::INDEXED => {
                write!(f, "INDEXED")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum KvFormat {
    INDEXED,
    COMPACTED,
}

impl Display for KvFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvFormat::COMPACTED => write!(f, "COMPACTED")?,
            KvFormat::INDEXED => write!(f, "INDEXED")?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_path: TablePath,
    pub table_id: i64,
    pub schema_id: i32,
    pub schema: Schema,
    pub row_type: DataType,
    pub primary_keys: Vec<String>,
    pub physical_primary_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub num_buckets: i32,
    pub properties: HashMap<String, String>,
    pub table_config: TableConfig,
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
    pub created_time: i64,
    pub modified_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableConfig {
    pub properties: HashMap<String, String>,
}

impl TableConfig {
    pub fn from_properties(properties: HashMap<String, String>) -> Self {
        TableConfig { properties }
    }
}

impl TableInfo {
    pub fn of(
        table_path: TablePath,
        table_id: i64,
        schema_id: i32,
        table_descriptor: TableDescriptor,
        created_time: i64,
        modified_time: i64,
    ) -> TableInfo {
        let TableDescriptor {
            schema,
            table_distribution,
            comment,
            partition_keys,
            properties,
            custom_properties,
        } = table_descriptor;
        let TableDistribution {
            bucket_count,
            bucket_keys,
        } = table_distribution.unwrap();
        TableInfo::new(
            table_path,
            table_id,
            schema_id,
            schema,
            bucket_keys,
            partition_keys,
            bucket_count.unwrap(),
            properties,
            custom_properties,
            comment,
            created_time,
            modified_time,
        )
    }

    pub fn new(
        table_path: TablePath,
        table_id: i64,
        schema_id: i32,
        schema: Schema,
        bucket_keys: Vec<String>,
        partition_keys: Vec<String>,
        num_buckets: i32,
        properties: HashMap<String, String>,
        custom_properties: HashMap<String, String>,
        comment: Option<String>,
        created_time: i64,
        modified_time: i64,
    ) -> Self {
        let row_type = schema.row_type.clone();
        let primary_keys = schema
            .primary_key_column_names()
            .iter()
            .map(|col| (*col).to_string())
            .collect();
        let physical_primary_keys =
            Self::generate_physical_primary_key(&primary_keys, &partition_keys);
        let table_config = TableConfig::from_properties(properties.clone());

        TableInfo {
            table_path,
            table_id,
            schema_id,
            schema,
            row_type,
            primary_keys,
            physical_primary_keys,
            bucket_keys,
            partition_keys,
            num_buckets,
            properties,
            table_config,
            custom_properties,
            comment,
            created_time,
            modified_time,
        }
    }

    pub fn get_table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id
    }

    pub fn get_schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_row_type(&self) -> &DataType {
        &self.row_type
    }

    pub fn has_primary_key(&self) -> bool {
        !self.primary_keys.is_empty()
    }

    pub fn get_primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn get_physical_primary_keys(&self) -> &[String] {
        &self.physical_primary_keys
    }

    pub fn has_bucket_key(&self) -> bool {
        !self.bucket_keys.is_empty()
    }

    pub fn is_default_bucket_key(&self) -> bool {
        if self.has_primary_key() {
            self.bucket_keys == self.physical_primary_keys
        } else {
            self.bucket_keys.is_empty()
        }
    }

    pub fn get_bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn is_auto_partitioned(&self) -> bool {
        self.is_partitioned() && todo!()
    }

    pub fn get_partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn get_num_buckets(&self) -> i32 {
        self.num_buckets
    }

    pub fn get_properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn get_table_config(&self) -> &TableConfig {
        &self.table_config
    }

    pub fn get_custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn get_comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn get_created_time(&self) -> i64 {
        self.created_time
    }

    pub fn get_modified_time(&self) -> i64 {
        self.modified_time
    }

    pub fn to_table_descriptor(&self) -> TableDescriptor {
        let mut builder = TableDescriptor::builder()
            .schema(self.schema.clone())
            .partitioned_by(self.partition_keys.clone())
            .distributed_by(Some(self.num_buckets), self.bucket_keys.clone())
            .properties(self.properties.clone())
            .custom_properties(self.custom_properties.clone());

        if let Some(comment) = &self.comment {
            builder = builder.comment(&comment.clone());
        }

        builder.build()
    }

    fn generate_physical_primary_key(
        primary_keys: &Vec<String>,
        partition_keys: &[String],
    ) -> Vec<String> {
        primary_keys
            .iter()
            .filter(|pk| !partition_keys.contains(*pk))
            .cloned()
            .collect()
    }
}

impl fmt::Display for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TableInfo{{ table_path={:?}, table_id={}, schema_id={}, schema={:?}, physical_primary_keys={:?}, bucket_keys={:?}, partition_keys={:?}, num_buckets={}, properties={:?}, custom_properties={:?}, comment={:?}, created_time={}, modified_time={} }}",
            self.table_path,
            self.table_id,
            self.schema_id,
            self.schema,
            self.physical_primary_keys,
            self.bucket_keys,
            self.partition_keys,
            self.num_buckets,
            self.properties,
            self.custom_properties,
            self.comment,
            self.created_time,
            self.modified_time
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TableBucket {
    table_id: i64,
    partition_id: Option<i64>,
    bucket: i32,
}

impl TableBucket {
    pub fn new(table_id: i64, bucket: i32) -> Self {
        TableBucket {
            table_id: table_id,
            partition_id: None,
            bucket: bucket,
        }
    }

    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    pub fn bucket_id(&self) -> i32 {
        self.bucket
    }

    pub fn partition_id(&self) -> Option<i64> {
        self.partition_id
    }
}
