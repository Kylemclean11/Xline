use tonic::IntoRequest;

use crate::{
    rpc::{self, SortOrder, SortTarget},
    server::command::KeyRange,
};

/// Option of `PutRequest`
#[derive(Debug, Default)]
pub struct PutOptions(rpc::PutRequest);

impl PutOptions {
    /// Creates a `PutOptions`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set key-value pair.
    #[inline]
    #[must_use]
    pub fn with_kv(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.0.key = key.into();
        self.0.value = value.into();
        self
    }

    /// Set lease ID.
    #[inline]
    #[must_use]
    pub fn with_lease(mut self, lease: i64) -> Self {
        self.0.lease = lease;
        self
    }

    /// Set prev kv flag.
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.0.prev_kv = prev_kv;
        self
    }

    /// Set ignore value flag.
    #[inline]
    #[must_use]
    pub fn with_ignore_value(mut self, ignore_value: bool) -> Self {
        self.0.ignore_value = ignore_value;
        self
    }

    /// Set ignore lease flag.
    #[inline]
    #[must_use]
    pub fn with_ignore_lease(mut self, ignore_lease: bool) -> Self {
        self.0.ignore_lease = ignore_lease;
        self
    }
}

impl From<PutOptions> for rpc::PutRequest {
    #[inline]
    #[must_use]
    fn from(options: PutOptions) -> Self {
        options.0
    }
}

impl IntoRequest<rpc::PutRequest> for PutOptions {
    #[inline]
    #[must_use]
    fn into_request(self) -> tonic::Request<rpc::PutRequest> {
        tonic::Request::new(self.into())
    }
}

/// Option of `RangeRequest`
#[derive(Debug, Default)]
pub struct RangeOptions {
    /// Inner request.
    inner: rpc::RangeRequest,
    /// All keys flag.
    all_keys: bool,
    /// Prefix flag.
    prefix: bool,
    /// From key flag.
    from_key: bool,
}

impl RangeOptions {
    /// Creates a `RangeOptions`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set key.
    #[inline]
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.inner.key = key.into();
        self
    }

    /// Set range end.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.all_keys = false;
        self.prefix = false;
        self.from_key = false;
        self.inner.range_end = range_end.into();
        self
    }

    /// Set limit.
    #[inline]
    #[must_use]
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.inner.limit = limit;
        self
    }

    /// Set revision.
    #[inline]
    #[must_use]
    pub fn with_revision(mut self, revision: i64) -> Self {
        self.inner.revision = revision;
        self
    }

    /// Set sort order.
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // safe cast
    pub fn with_sort_order(mut self, sort_order: SortOrder) -> Self {
        self.inner.sort_order = sort_order as i32;
        self
    }

    /// Set sort target.
    #[inline]
    #[must_use]
    #[allow(clippy::as_conversions)] // safe cast
    pub fn with_sort_target(mut self, sort_target: SortTarget) -> Self {
        self.inner.sort_target = sort_target as i32;
        self
    }

    /// Set serializable flag.
    #[inline]
    #[must_use]
    pub fn with_serializable(mut self, serializable: bool) -> Self {
        self.inner.serializable = serializable;
        self
    }

    /// Set keys only flag.
    #[inline]
    #[must_use]
    pub fn with_keys_only(mut self, keys_only: bool) -> Self {
        self.inner.keys_only = keys_only;
        self
    }

    /// Set count only flag.
    #[inline]
    #[must_use]
    pub fn with_count_only(mut self, count_only: bool) -> Self {
        self.inner.count_only = count_only;
        self
    }

    /// Set min mod revision.
    #[inline]
    #[must_use]
    pub fn with_min_mod_revision(mut self, revision: i64) -> Self {
        self.inner.min_mod_revision = revision;
        self
    }

    /// Set max mod revision.
    #[inline]
    #[must_use]
    pub fn with_max_mod_revision(mut self, revision: i64) -> Self {
        self.inner.max_mod_revision = revision;
        self
    }

    /// Set min create revision.
    #[inline]
    #[must_use]
    pub fn with_min_create_revision(mut self, revision: i64) -> Self {
        self.inner.min_create_revision = revision;
        self
    }

    /// Set max create revision.
    #[inline]
    #[must_use]
    pub fn with_max_create_revision(mut self, revision: i64) -> Self {
        self.inner.max_create_revision = revision;
        self
    }

    /// All keys.
    #[inline]
    #[must_use]
    pub fn with_all_keys(mut self) -> Self {
        self.all_keys = true;
        self.prefix = false;
        self.from_key = false;
        self
    }

    /// Keys with prefix.
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        self.all_keys = false;
        self.prefix = true;
        self.from_key = false;
        self
    }

    /// Keys with from key.
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        self.all_keys = false;
        self.prefix = false;
        self.from_key = true;
        self
    }
}

impl From<RangeOptions> for rpc::RangeRequest {
    #[inline]
    #[must_use]
    fn from(option: RangeOptions) -> Self {
        let mut request = option.inner;
        if option.all_keys {
            request.key = vec![0];
            request.range_end = vec![0];
        }
        if option.prefix {
            if request.key.is_empty() {
                request.key = vec![0];
                request.range_end = vec![0];
            } else {
                request.range_end = KeyRange::get_prefix(&request.key);
            }
        }
        if option.from_key {
            if request.key.is_empty() {
                request.key = vec![0];
            }
            request.range_end = vec![0];
        }
        request
    }
}

impl IntoRequest<rpc::RangeRequest> for RangeOptions {
    #[inline]
    #[must_use]
    fn into_request(self) -> tonic::Request<rpc::RangeRequest> {
        tonic::Request::new(self.into())
    }
}

/// Option for `DeleteRangeRequest`.
#[derive(Debug, Default)]
pub struct DeleteRangeOptions {
    /// Inner request.
    inner: rpc::DeleteRangeRequest,
    /// All keys flag.
    all_keys: bool,
    /// Prefix flag.
    prefix: bool,
    /// From key flag.
    from_key: bool,
}

impl DeleteRangeOptions {
    /// Create a default `DeleteRangeOption`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set key.
    #[inline]
    #[must_use]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.inner.key = key.into();
        self
    }

    /// Set range end.
    #[inline]
    #[must_use]
    pub fn with_range_end(mut self, range_end: impl Into<Vec<u8>>) -> Self {
        self.all_keys = false;
        self.prefix = false;
        self.from_key = false;
        self.inner.range_end = range_end.into();
        self
    }

    /// Set prev kv flag.
    #[inline]
    #[must_use]
    pub fn with_prev_kv(mut self, prev_kv: bool) -> Self {
        self.inner.prev_kv = prev_kv;
        self
    }

    /// All keys.
    #[inline]
    #[must_use]
    pub fn with_all_keys(mut self) -> Self {
        self.all_keys = true;
        self.prefix = false;
        self.from_key = false;
        self
    }

    /// Keys with prefix.
    #[inline]
    #[must_use]
    pub fn with_prefix(mut self) -> Self {
        self.all_keys = false;
        self.prefix = true;
        self.from_key = false;
        self
    }

    /// Keys with from key.
    #[inline]
    #[must_use]
    pub fn with_from_key(mut self) -> Self {
        self.all_keys = false;
        self.prefix = false;
        self.from_key = true;
        self
    }
}

impl From<DeleteRangeOptions> for rpc::DeleteRangeRequest {
    #[inline]
    #[must_use]
    fn from(option: DeleteRangeOptions) -> Self {
        let mut request = option.inner;
        if option.all_keys {
            request.key = vec![0];
            request.range_end = vec![0];
        }
        if option.prefix {
            if request.key.is_empty() {
                request.key = vec![0];
                request.range_end = vec![0];
            } else {
                request.range_end = KeyRange::get_prefix(&request.key);
            }
        }
        if option.from_key {
            if request.key.is_empty() {
                request.key = vec![0];
            }
            request.range_end = vec![0];
        }
        request
    }
}

impl IntoRequest<rpc::DeleteRangeRequest> for DeleteRangeOptions {
    #[inline]
    #[must_use]
    fn into_request(self) -> tonic::Request<rpc::DeleteRangeRequest> {
        tonic::Request::new(self.into())
    }
}

/// Option for `TxnRequest`.
#[derive(Debug, Default)]
pub struct Txn {
    /// Inner request
    inner: rpc::TxnRequest,
    /// Whether the txn has called when
    c_when: bool,
    /// Whether the txn has called and_then
    c_then: bool,
    /// Whether the txn has called or_else
    c_else: bool,
}

impl Txn {
    /// Create a default `Txn`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: rpc::TxnRequest {
                compare: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            },
            c_when: false,
            c_then: false,
            c_else: false,
        }
    }

    /// Set compares of the transaction.
    /// # Panics
    /// Panics if `when` is called twice. or `when` is not called before `and_then` or `or_else`.
    #[inline]
    #[must_use]
    pub fn when(mut self, compare: impl Into<Vec<rpc::Compare>>) -> Self {
        assert!(!self.c_when, "cannot call when twice");
        assert!(!self.c_then, "cannot call when after and_then");
        assert!(!self.c_else, "cannot call when after or_else");
        self.inner.compare = compare.into();
        self
    }

    /// Set success requests of the transaction.
    /// # Panics
    /// Panics if `and_then` is called twice. or `and_then` is called after `or_else`.
    #[inline]
    #[must_use]
    pub fn and_then(mut self, success: impl Into<Vec<rpc::RequestOp>>) -> Self {
        assert!(!self.c_then, "cannot call and_then twice");
        assert!(!self.c_else, "cannot call and_then after or_else");
        self.inner.success = success.into();
        self
    }

    /// Set failure requests of the transaction.
    /// # Panics
    /// Panics if `or_else` is called twice.
    #[inline]
    #[must_use]
    pub fn or_else(mut self, failure: impl Into<Vec<rpc::RequestOp>>) -> Self {
        assert!(!self.c_else, "cannot call or_else twice");
        self.inner.failure = failure.into();
        self
    }
}

impl From<Txn> for rpc::TxnRequest {
    #[inline]
    #[must_use]
    fn from(txn: Txn) -> Self {
        txn.inner
    }
}

impl IntoRequest<rpc::TxnRequest> for Txn {
    #[inline]
    #[must_use]
    fn into_request(self) -> tonic::Request<rpc::TxnRequest> {
        tonic::Request::new(self.into())
    }
}

/// Option for `CompactionRequest`.
#[derive(Debug, Default)]
pub struct CompactionOptions(rpc::CompactionRequest);

impl CompactionOptions {
    /// Create a default `CompactionOption`.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set revision.
    #[inline]
    #[must_use]
    pub fn with_revision(mut self, revision: i64) -> Self {
        self.0.revision = revision;
        self
    }

    /// Set physical flag.
    #[inline]
    #[must_use]
    pub fn with_physical(mut self) -> Self {
        self.0.physical = true;
        self
    }
}

impl From<CompactionOptions> for rpc::CompactionRequest {
    #[inline]
    #[must_use]
    fn from(option: CompactionOptions) -> Self {
        option.0
    }
}

impl IntoRequest<rpc::CompactionRequest> for CompactionOptions {
    #[inline]
    #[must_use]
    fn into_request(self) -> tonic::Request<rpc::CompactionRequest> {
        tonic::Request::new(self.into())
    }
}
