use crate::constants::{RESULT_FAILURE, RESULT_ONGOING};

pub type Result<T> = std::result::Result<T, DtmError>;

#[derive(thiserror::Error, Debug)]
pub enum DtmError {
    #[error("{message}")]
    Failure { message: String },

    #[error("{message}")]
    Ongoing { message: String },

    #[error("{message}")]
    Duplicated { message: String },

    #[error("invalid input: {message}")]
    InvalidInput { message: String },

    #[error("dtm http status {status}: {body}")]
    HttpStatus { status: u16, body: String },

    #[cfg(feature = "http")]
    #[error(transparent)]
    Http(Box<reqwest::Error>),

    #[cfg(feature = "grpc")]
    #[error(transparent)]
    GrpcStatus(Box<tonic::Status>),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "http")]
    #[error(transparent)]
    Url(#[from] url::ParseError),

    #[cfg(feature = "grpc")]
    #[error(transparent)]
    ProtoEncode(#[from] prost::EncodeError),

    #[cfg(feature = "grpc")]
    #[error(transparent)]
    ProtoDecode(#[from] prost::DecodeError),

    #[cfg(feature = "barrier-redis")]
    #[error(transparent)]
    Redis(Box<redis::RedisError>),

    #[error("{message}")]
    Other { message: String },
}

impl DtmError {
    pub fn failure(message: impl Into<String>) -> Self {
        Self::Failure {
            message: message.into(),
        }
    }

    pub fn ongoing(message: impl Into<String>) -> Self {
        Self::Ongoing {
            message: message.into(),
        }
    }

    pub fn duplicated(message: impl Into<String>) -> Self {
        Self::Duplicated {
            message: message.into(),
        }
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failure { .. })
    }

    pub fn is_ongoing(&self) -> bool {
        matches!(self, Self::Ongoing { .. })
    }

    pub fn is_duplicated(&self) -> bool {
        matches!(self, Self::Duplicated { .. })
    }

    pub fn from_dtm_result_string(message: &str) -> Option<Self> {
        if message.contains(RESULT_ONGOING) {
            Some(Self::ongoing(message.to_string()))
        } else if message.contains(RESULT_FAILURE) {
            Some(Self::failure(message.to_string()))
        } else {
            None
        }
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for DtmError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(Box::new(value))
    }
}

#[cfg(feature = "grpc")]
impl From<tonic::Status> for DtmError {
    fn from(value: tonic::Status) -> Self {
        Self::GrpcStatus(Box::new(value))
    }
}

#[cfg(feature = "barrier-redis")]
impl From<redis::RedisError> for DtmError {
    fn from(value: redis::RedisError) -> Self {
        Self::Redis(Box::new(value))
    }
}
