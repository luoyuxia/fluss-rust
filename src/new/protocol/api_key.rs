use crate::new::protocol::api_key::ApiKey::Unknown;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum ApiKey {
    CreateTable,
    ProduceLog,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        match key {
            1005 => ApiKey::CreateTable,
            1014 => ApiKey::ProduceLog,
            _ => Unknown(key),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::CreateTable => 1005,
            ApiKey::ProduceLog => 1014,
            Unknown(x) => x,
        }
    }
}
