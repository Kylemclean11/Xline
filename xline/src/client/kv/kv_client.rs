use tonic::{transport::Channel, IntoRequest};

use crate::{
    client::{error::ClientError, CompactionOptions, DeleteRangeOptions, RangeOptions, Txn},
    rpc,
};

use super::opts::PutOptions;

/// Kv client
#[derive(Debug, Clone)]
pub struct KvClient {
    /// inner client
    inner: rpc::KvClient<Channel>,
}

impl KvClient {
    /// New `KvClient`
    pub(crate) fn new(channel: Channel) -> Self {
        Self {
            inner: rpc::KvClient::new(channel),
        }
    }

    /// Put the given key into the key-value store.
    /// # Errors
    /// Returns `ClientError` if the rpc call returns an error.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        opts: Option<PutOptions>,
    ) -> Result<rpc::PutResponse, ClientError> {
        self.inner
            .put(opts.unwrap_or_default().with_kv(key, value).into_request())
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }

    /// Get the value for the given key from the key-value store.
    /// # Errors
    /// Returns `ClientError` if the rpc call returns an error.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        opts: Option<RangeOptions>,
    ) -> Result<rpc::RangeResponse, ClientError> {
        self.inner
            .range(opts.unwrap_or_default().with_key(key).into_request())
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }

    /// Delete the given key from the key-value store.
    /// # Errors
    /// Returns `ClientError` if the rpc call returns an error.
    #[inline]
    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        opts: Option<DeleteRangeOptions>,
    ) -> Result<rpc::DeleteRangeResponse, ClientError> {
        self.inner
            .delete_range(opts.unwrap_or_default().with_key(key).into_request())
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }

    /// Send a transaction request to server.
    /// # Errors
    /// Returns `ClientError` if the rpc call returns an error.
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<rpc::TxnResponse, ClientError> {
        self.inner
            .txn(txn.into_request())
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }

    /// Compact the event history in server up to a given revision.
    /// # Errors
    /// Returns `ClientError` if the rpc call returns an error.
    #[inline]
    pub async fn compact(
        &mut self,
        revision: i64,
        opts: Option<CompactionOptions>,
    ) -> Result<rpc::CompactionResponse, ClientError> {
        self.inner
            .compact(
                opts.unwrap_or_default()
                    .with_revision(revision)
                    .into_request(),
            )
            .await
            .map(tonic::Response::into_inner)
            .map_err(Into::into)
    }
}
