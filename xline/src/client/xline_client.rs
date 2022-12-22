use std::collections::HashMap;

use curp::{client::Client as CurpClient, cmd::ProposeId};
use itertools::Itertools;
use tonic::transport::Channel;
use tower::discover::Change;
use uuid::Uuid;

use crate::{
    rpc::{self, RequestWithToken, RequestWrapper},
    server::command::{Command, KeyRange},
};

use super::{error::ClientError, DeleteRangeOptions, KvClient, PutOptions, RangeOptions, Txn};

/// Xline client
#[derive(Debug)]
pub struct Client {
    /// Curp client
    curp_client: CurpClient<Command>,
    /// Kv client
    kv_client: KvClient,
    /// Use curp client to send requests when true
    use_curp: bool,
}

impl Client {
    /// New `Client`
    /// # Panics
    /// Panics addrs is empty or contains invalid address
    #[inline]
    pub async fn new(endpoints: HashMap<String, String>, use_curp: bool) -> Self {
        let eps = endpoints
            .values()
            .map(|endpoint| {
                let addr = if endpoint.starts_with("http://") {
                    endpoint.clone()
                } else {
                    format!("http://{endpoint}")
                };
                Channel::builder(
                    addr.parse()
                        .unwrap_or_else(|e| panic!("Failed to parse endpoint: {}", e)),
                )
            })
            .collect_vec();

        assert!(!eps.is_empty(), "endpoints is empty");

        let (channel, tx) = Channel::balance_channel(64);
        for endpoint in eps {
            assert!(
                tx.send(Change::Insert(endpoint.uri().clone(), endpoint))
                    .await
                    .is_ok(),
                "send change failed"
            );
        }

        let kv_client = KvClient::new(channel);
        let curp_client = CurpClient::new(endpoints).await;
        Self {
            curp_client,
            kv_client,
            use_curp,
        }
    }

    /// Generate `Command` proposal from `RequestWrapper`
    fn command_from_request_wrapper(wrapper: RequestWithToken) -> Command {
        let propose_id = ProposeId::new(format!("client-{}", Uuid::new_v4()));
        #[allow(clippy::wildcard_enum_match_arm)]
        let key_ranges = match wrapper.request {
            RequestWrapper::RangeRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            RequestWrapper::PutRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: vec![],
            }],
            RequestWrapper::DeleteRangeRequest(ref req) => vec![KeyRange {
                start: req.key.clone(),
                end: req.range_end.clone(),
            }],
            RequestWrapper::TxnRequest(ref req) => req
                .compare
                .iter()
                .map(|cmp| KeyRange {
                    start: cmp.key.clone(),
                    end: cmp.range_end.clone(),
                })
                .collect(),
            _ => unreachable!("Other request should not be sent to this store"),
        };
        Command::new(key_ranges, wrapper, propose_id)
    }

    /// Put the given key into the key-value store.
    /// # Errors
    /// Returns `ClientError` if the propose or rpc call returns an error.
    #[inline]
    pub async fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        opts: Option<PutOptions>,
    ) -> Result<rpc::PutResponse, ClientError> {
        if self.use_curp {
            let cmd = Self::command_from_request_wrapper(RequestWithToken::new(
                rpc::PutRequest::from(opts.unwrap_or_default().with_kv(key, value)).into(),
            ));
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            self.kv_client.put(key, value, opts).await
        }
    }

    /// Get the given key from the key-value store.
    /// # Errors
    /// Returns `ClientError` if the propose or rpc call returns an error.
    #[inline]
    pub async fn get(
        &mut self,
        key: impl Into<Vec<u8>>,
        opts: Option<RangeOptions>,
    ) -> Result<rpc::RangeResponse, ClientError> {
        if self.use_curp {
            let cmd = Self::command_from_request_wrapper(RequestWithToken::new(
                rpc::RangeRequest::from(opts.unwrap_or_default().with_key(key)).into(),
            ));
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            self.kv_client.get(key, opts).await
        }
    }

    /// Delete the given key from the key-value store.
    /// # Errors
    /// Returns `ClientError` if the propose or rpc call returns an error.
    #[inline]
    pub async fn delete(
        &mut self,
        key: impl Into<Vec<u8>>,
        opts: Option<DeleteRangeOptions>,
    ) -> Result<rpc::DeleteRangeResponse, ClientError> {
        if self.use_curp {
            let cmd = Self::command_from_request_wrapper(RequestWithToken::new(
                rpc::DeleteRangeRequest::from(opts.unwrap_or_default().with_key(key)).into(),
            ));
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            self.kv_client.delete(key, opts).await
        }
    }

    /// Send a transaction request to the key-value store.
    /// # Errors
    /// Returns `ClientError` if the propose or rpc call returns an error.
    #[inline]
    pub async fn txn(&mut self, txn: Txn) -> Result<rpc::TxnResponse, ClientError> {
        if self.use_curp {
            let cmd = Self::command_from_request_wrapper(RequestWithToken::new(
                rpc::TxnRequest::from(txn).into(),
            ));
            let cmd_res = self.curp_client.propose(cmd).await?;
            Ok(cmd_res.decode().into())
        } else {
            self.kv_client.txn(txn).await
        }
    }
}
