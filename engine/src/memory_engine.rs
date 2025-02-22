use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{Cursor, Seek},
    path::Path,
    sync::Arc,
};

use clippy_utilities::NumericCast;
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    engine_api::{SnapshotApi, StorageEngine, WriteOperation},
    error::EngineError,
};

/// A helper type to store the key-value pairs for the `MemoryEngine`
type MemoryTable = HashMap<Vec<u8>, Vec<u8>>;

/// Memory Storage Engine Implementation
#[derive(Debug, Default, Clone)]
pub struct MemoryEngine {
    /// The inner storage engine of `MemoryStorage`
    inner: Arc<RwLock<HashMap<String, MemoryTable>>>,
}

/// A snapshot of the `MemoryEngine`
#[derive(Debug, Default)]
pub struct MemorySnapshot {
    /// data of the snapshot
    data: Cursor<Vec<u8>>,
}

#[async_trait::async_trait]
impl SnapshotApi for MemorySnapshot {
    #[inline]
    fn size(&self) -> u64 {
        self.data.get_ref().len().numeric_cast()
    }

    #[inline]
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.data.read_exact(buf).await.map(drop)
    }

    #[inline]
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.data.write_all(buf).await
    }

    #[inline]
    fn rewind(&mut self) -> std::io::Result<()> {
        Seek::rewind(&mut self.data)
    }

    #[inline]
    async fn clean(&mut self) -> std::io::Result<()> {
        self.data.get_mut().clear();
        Ok(())
    }
}

impl MemoryEngine {
    /// New `MemoryEngine`
    ///
    /// # Errors
    ///
    /// Returns `EngineError` when DB create tables failed or open failed.
    #[inline]
    pub fn new(tables: &[&'static str]) -> Result<Self, EngineError> {
        let mut inner: HashMap<String, HashMap<Vec<u8>, Vec<u8>>> = HashMap::new();
        for table in tables {
            let _ignore = inner.entry((*table).to_owned()).or_insert(HashMap::new());
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemoryEngine {
    type Snapshot = MemorySnapshot;

    #[inline]
    fn get(&self, table: &str, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        Ok(table.get(&key.as_ref().to_vec()).cloned())
    }

    #[inline]
    fn get_multi(
        &self,
        table: &str,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;

        Ok(keys
            .iter()
            .map(|key| table.get(&key.as_ref().to_vec()).cloned())
            .collect())
    }

    #[inline]
    fn get_all(&self, table: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        let inner = self.inner.read();
        let table = inner
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
        let mut values = table
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        values.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        Ok(values)
    }

    #[inline]
    fn write_batch(&self, wr_ops: Vec<WriteOperation<'_>>, _sync: bool) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        for op in wr_ops {
            match op {
                WriteOperation::Put { table, key, value } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.insert(key, value);
                }
                WriteOperation::Delete { table, key } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    let _ignore = table.remove(key);
                }
                WriteOperation::DeleteRange { table, from, to } => {
                    let table = inner
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_owned()))?;
                    table.retain(|key, _value| {
                        let key_slice = key.as_slice();
                        match key_slice.cmp(from) {
                            Ordering::Less => true,
                            Ordering::Equal => false,
                            Ordering::Greater => match key_slice.cmp(to) {
                                Ordering::Less => false,
                                Ordering::Equal | Ordering::Greater => true,
                            },
                        }
                    });
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn get_snapshot(
        &self,
        _path: impl AsRef<Path>,
        _tables: &[&'static str],
    ) -> Result<Self::Snapshot, EngineError> {
        let inner_r = self.inner.read();
        let db = &*inner_r;
        let data = bincode::serialize(db).map_err(|e| {
            EngineError::UnderlyingError(format!("serialize memory engine failed: {e:?}"))
        })?;
        Ok(MemorySnapshot {
            data: Cursor::new(data),
        })
    }

    #[inline]
    fn apply_snapshot(
        &self,
        snapshot: Self::Snapshot,
        _tables: &[&'static str],
    ) -> Result<(), EngineError> {
        let mut inner = self.inner.write();
        let db = &mut *inner;
        let data = snapshot.data.into_inner();
        let new_db = bincode::deserialize(&data).map_err(|e| {
            EngineError::UnderlyingError(format!("deserialize memory engine failed: {e:?}"))
        })?;
        *db = new_db;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::iter::{repeat, zip};

    use super::*;

    const TESTTABLES: [&'static str; 3] = ["kv", "lease", "auth"];

    #[test]
    fn write_batch_into_a_non_existing_table_should_fail() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();

        let put = WriteOperation::new_put(
            "hello",
            "hello".as_bytes().to_vec(),
            "world".as_bytes().to_vec(),
        );
        assert!(engine.write_batch(vec![put], false).is_err());

        let delete = WriteOperation::new_delete("hello", b"hello");
        assert!(engine.write_batch(vec![delete], false).is_err());

        let delete_range = WriteOperation::new_delete_range("hello", b"hello", b"world");
        assert!(engine.write_batch(vec![delete_range], false).is_err());
    }

    #[test]
    fn write_batch_should_success() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let origin_set: Vec<Vec<u8>> = (1u8..=10u8)
            .map(|val| repeat(val).take(4).collect())
            .collect();
        let keys = origin_set.clone();
        let values = origin_set.clone();
        let puts = zip(keys, values)
            .map(|(k, v)| WriteOperation::new_put("kv", k, v))
            .collect::<Vec<WriteOperation<'_>>>();

        assert!(engine.write_batch(puts, false).is_ok());

        let res_1 = engine.get_multi("kv", &origin_set).unwrap();
        assert_eq!(res_1.iter().filter(|v| v.is_some()).count(), 10);

        let delete_key: Vec<u8> = vec![1, 1, 1, 1];
        let delete = WriteOperation::new_delete("kv", &delete_key);

        let res_2 = engine.write_batch(vec![delete], false);
        assert!(res_2.is_ok());

        let res_3 = engine.get("kv", &delete_key).unwrap();
        assert!(res_3.is_none());

        let delete_start: Vec<u8> = vec![2, 2, 2, 2];
        let delete_end: Vec<u8> = vec![5, 5, 5, 5];
        let delete_range = WriteOperation::new_delete_range("kv", &delete_start, &delete_end);
        let res_4 = engine.write_batch(vec![delete_range], false);
        assert!(res_4.is_ok());

        let get_key_1: Vec<u8> = vec![5, 5, 5, 5];
        let get_key_2: Vec<u8> = vec![3, 3, 3, 3];
        assert!(engine.get("kv", &get_key_1).unwrap().is_some());
        assert!(engine.get("kv", &get_key_2).unwrap().is_none());
    }

    #[test]
    fn get_operation_should_success() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let test_set = vec![("hello", "hello"), ("world", "world"), ("foo", "foo")];
        let batch = test_set.iter().map(|&(key, value)| {
            WriteOperation::new_put("kv", key.as_bytes().to_vec(), value.as_bytes().to_vec())
        });
        let res = engine.write_batch(batch.collect(), false);
        assert!(res.is_ok());

        let res_1 = engine.get("kv", "hello").unwrap();
        assert_eq!(res_1, Some("hello".as_bytes().to_vec()));
        let multi_keys = vec!["hello", "world", "bar"];
        let expected_multi_values = vec![
            Some("hello".as_bytes().to_vec()),
            Some("world".as_bytes().to_vec()),
            None,
        ];
        let res_2 = engine.get_multi("kv", &multi_keys).unwrap();
        assert_eq!(multi_keys.len(), res_2.len());
        assert_eq!(res_2, expected_multi_values);

        let mut res_3 = engine.get_all("kv").unwrap();
        let mut expected_all_values = test_set
            .into_iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();
        assert_eq!(res_3.sort(), expected_all_values.sort());
    }

    #[tokio::test]
    async fn snapshot_should_work() {
        let engine = MemoryEngine::new(&TESTTABLES).unwrap();
        let put = WriteOperation::new_put("kv", "key".into(), "value".into());
        assert!(engine.write_batch(vec![put], false).is_ok());

        let mut snapshot = engine.get_snapshot("", &TESTTABLES).unwrap();
        let put = WriteOperation::new_put("kv", "key2".into(), "value2".into());
        assert!(engine.write_batch(vec![put], false).is_ok());

        let mut buf = vec![0u8; snapshot.size().numeric_cast()];
        snapshot.read_exact(&mut buf).await.unwrap();

        let mut new_snapshot = MemorySnapshot {
            data: Cursor::new(Vec::new()),
        };
        new_snapshot.write_all(&buf).await.unwrap();

        let engine_2 = MemoryEngine::new(&TESTTABLES).unwrap();
        assert!(engine_2.apply_snapshot(new_snapshot, &TESTTABLES).is_ok());

        let value = engine_2.get("kv", "key").unwrap();
        assert_eq!(value, Some("value".into()));
        let value2 = engine_2.get("kv", "key2").unwrap();
        assert!(value2.is_none());
    }
}
