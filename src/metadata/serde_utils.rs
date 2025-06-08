use serde::{Deserialize, Deserializer, Serialize, Serializer};


pub trait DataTypeName {
    const NAME: &'static str;
}

pub struct ARRAY;
    impl DataTypeName for ARRAY {
        const NAME: &'static str = "ARRAY";
    }

    pub struct BIGINT;
    impl DataTypeName for BIGINT {
        const NAME: &'static str = "BIGINT";
    }

    pub struct BOOLEAN;
    impl DataTypeName for BOOLEAN {
        const NAME: &'static str = "BOOLEAN";
    }

    pub struct BINARY;
    impl DataTypeName for BINARY {
        const NAME: &'static str = "BINARY";
    }

    pub struct CHAR;
    impl DataTypeName for CHAR {
        const NAME: &'static str = "CHAR";
    }

    pub struct VARCHAR;
    impl DataTypeName for VARCHAR {
        const NAME: &'static str = "VARCHAR";
    }

    pub struct DECIMAL;
    impl DataTypeName for DECIMAL {
        const NAME: &'static str = "DECIMAL";
    }

    pub struct DATE;
    impl DataTypeName for DATE {
        const NAME: &'static str = "DATE";
    }

    pub struct DOUBLE;
    impl DataTypeName for DOUBLE {
        const NAME: &'static str = "DOUBLE";
    }

    pub struct FLOAT;
    impl DataTypeName for FLOAT {
        const NAME: &'static str = "FLOAT";
    }

    pub struct INT;
    impl DataTypeName for INT {
        const NAME: &'static str = "INT";
    }

    pub struct SMALLINT;
    impl DataTypeName for SMALLINT {
        const NAME: &'static str = "SMALLINT";
    }

    pub struct TIME;
    impl DataTypeName for TIME {
        const NAME: &'static str = "TIME";
    }

    pub struct TIMESTAMP;
    impl DataTypeName for TIMESTAMP {
        const NAME: &'static str = "TIMESTAMP";
    }


    pub struct TimestampLTz;
    impl DataTypeName for TimestampLTz {
        const NAME: &'static str = "TIMESTAMPLTZ";
    }


    pub struct TINYINT;
    impl DataTypeName for TINYINT {
        const NAME: &'static str = "TINYINT";
    }

    pub struct MAP;
    impl DataTypeName for MAP {
        const NAME: &'static str = "MAP";
    }

    pub struct ROW;
    impl DataTypeName for ROW {
        const NAME: &'static str = "ROW";
    }

    pub struct NullableType<T: DataTypeName> {
        nullable: bool,
        value: PhantomData<T>,
    }

    impl<T: DataTypeName> From<bool> for NullableType<T> {
        fn from(value: bool) -> Self {
            Self {
                nullable: value,
                value: PhantomData,
            }
        }
    }
    impl<T: DataTypeName> From<NullableType<T>> for bool {
        fn from(value: NullableType<T>) -> Self {
            value.nullable
        }
    }

    impl<T: DataTypeName> Serialize for NullableType<T> {
        fn serialize<S>(&self, serializer: S) -> S
        where
            S: Serializer,
        {
            if self.nullable {
                serializer.serialize_str(T::NAME).unwrap()
            } else {
                serializer.serialize_str(&format!("{} NOT NULL", T::NAME)).unwrap()
            }
        }
    }

    impl<'de, T: DataTypeName> Deserialize<'de> for NullableType<T> {
        fn deserialize<D>(deserializer: D) -> Self
        where
            D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;

            let (name, nullable) = s.split_once(' ').unwrap_or((s.as_str(), ""));

            if name == T::NAME && nullable.is_empty() {
                NullableType::from(true)
            } else if name == T::NAME && nullable.contains("NOT NULL") {
                NullableType::from(true)
            } else {
                panic!("todo")
            }
        }
    }

