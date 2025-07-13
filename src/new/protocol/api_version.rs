#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct ApiVersion(pub i16);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ApiVersionRange {
    min: ApiVersion,
    max: ApiVersion,
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ApiVersionRange {
    pub const fn new(min: ApiVersion, max: ApiVersion) -> Self {
        assert!(min.0 <= max.0);

        Self { min, max }
    }

    pub fn min(&self) -> ApiVersion {
        self.min
    }

    pub fn max(&self) -> ApiVersion {
        self.max
    }
}

impl std::fmt::Display for ApiVersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.min, self.max)
    }
}
