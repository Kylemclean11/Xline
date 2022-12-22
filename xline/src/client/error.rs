use thiserror::Error;

/// Error types of xline client
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// Errors of grpc
    #[error("Grpc error: {0} ")]
    Grpc(String),
    /// Propose error
    #[error("Propose error: {0} ")]
    Propose(#[from] curp::error::ProposeError),
}

impl From<tonic::Status> for ClientError {
    #[inline]
    fn from(status: tonic::Status) -> Self {
        ClientError::Grpc(status.to_string())
    }
}
