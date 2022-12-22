mod common;

use std::error::Error;

use xline::client::{DeleteRangeOptions, PutOptions, RangeOptions, SortOrder, SortTarget};

use crate::common::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_put() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: &'a str,
        value: &'a str,
        opts: Option<PutOptions>,
        want_err: bool,
    }

    let tests = [
        TestCase {
            key: "foo",
            value: "",
            opts: Some(PutOptions::new().with_ignore_value(true)),
            want_err: true,
        },
        TestCase {
            key: "foo",
            value: "bar",
            opts: None,
            want_err: false,
        },
        TestCase {
            key: "foo",
            value: "",
            opts: Some(PutOptions::new().with_ignore_value(true)),
            want_err: false,
        },
    ];

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    for test in tests {
        let res = client.put(test.key, test.value, test.opts).await;
        assert_eq!(res.is_err(), test.want_err);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_get() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<RangeOptions>,
        want_kvs: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let kvs = ["a", "b", "c", "c", "c", "foo", "foo/abc", "fop"];
    let want_kvs = ["a", "b", "c", "foo", "foo/abc", "fop"];
    let kvs_by_version = ["a", "b", "foo", "foo/abc", "fop", "c"];
    let reversed_kvs = ["fop", "foo/abc", "foo", "c", "b", "a"];

    let tests = [
        TestCase {
            key: "a",
            opts: None,
            want_kvs: &want_kvs[..1],
        },
        // TestCase {
        //     key: "a",
        //     opts: Some(RangeOptions::new().with_serializable(true)),
        //     want_kvs: &want_kvs[..1],
        // },
        TestCase {
            key: "a",
            opts: Some(RangeOptions::new().with_range_end("c")),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "",
            opts: Some(RangeOptions::new().with_prefix()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(RangeOptions::new().with_from_key()),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "a",
            opts: Some(RangeOptions::new().with_range_end("x")),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(RangeOptions::new().with_prefix().with_revision(4)),
            want_kvs: &want_kvs[..3],
        },
        TestCase {
            key: "a",
            opts: Some(RangeOptions::new().with_count_only(true)),
            want_kvs: &[],
        },
        TestCase {
            key: "foo",
            opts: Some(RangeOptions::new().with_prefix()),
            want_kvs: &["foo", "foo/abc"],
        },
        TestCase {
            key: "foo",
            opts: Some(RangeOptions::new().with_from_key()),
            want_kvs: &["foo", "foo/abc", "fop"],
        },
        TestCase {
            key: "",
            opts: Some(RangeOptions::new().with_prefix().with_limit(2)),
            want_kvs: &want_kvs[..2],
        },
        TestCase {
            key: "",
            opts: Some(
                RangeOptions::new()
                    .with_prefix()
                    .with_sort_target(SortTarget::Mod)
                    .with_sort_order(SortOrder::Ascend),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                RangeOptions::new()
                    .with_prefix()
                    .with_sort_target(SortTarget::Version)
                    .with_sort_order(SortOrder::Ascend),
            ),
            want_kvs: &kvs_by_version[..],
        },
        TestCase {
            key: "",
            opts: Some(
                RangeOptions::new()
                    .with_prefix()
                    .with_sort_target(SortTarget::Create)
                    .with_sort_order(SortOrder::None),
            ),
            want_kvs: &want_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                RangeOptions::new()
                    .with_prefix()
                    .with_sort_target(SortTarget::Create)
                    .with_sort_order(SortOrder::Descend),
            ),
            want_kvs: &reversed_kvs[..],
        },
        TestCase {
            key: "",
            opts: Some(
                RangeOptions::new()
                    .with_prefix()
                    .with_sort_target(SortTarget::Key)
                    .with_sort_order(SortOrder::Descend),
            ),
            want_kvs: &reversed_kvs[..],
        },
    ];

    for key in kvs {
        client.put(key, "bar", None).await?;
    }

    for test in tests {
        let res = client.get(test.key, test.opts).await?;
        assert_eq!(res.kvs.len(), test.want_kvs.len());
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_kvs.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_kv_delete() -> Result<(), Box<dyn Error>> {
    struct TestCase<'a> {
        key: &'a str,
        opts: Option<DeleteRangeOptions>,
        want_deleted: i64,
        want_keys: &'a [&'a str],
    }

    let mut cluster = Cluster::new(3).await;
    cluster.start().await;
    let client = cluster.client().await;

    let keys = ["a", "b", "c", "c/abc", "d"];

    let tests = [
        TestCase {
            key: "",
            opts: Some(DeleteRangeOptions::new().with_prefix()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "",
            opts: Some(DeleteRangeOptions::new().with_from_key()),
            want_deleted: 5,
            want_keys: &[],
        },
        TestCase {
            key: "a",
            opts: Some(DeleteRangeOptions::new().with_range_end("c")),
            want_deleted: 2,
            want_keys: &["c", "c/abc", "d"],
        },
        TestCase {
            key: "c",
            opts: None,
            want_deleted: 1,
            want_keys: &["a", "b", "c/abc", "d"],
        },
        TestCase {
            key: "c",
            opts: Some(DeleteRangeOptions::new().with_prefix()),
            want_deleted: 2,
            want_keys: &["a", "b", "d"],
        },
        TestCase {
            key: "c",
            opts: Some(DeleteRangeOptions::new().with_from_key()),
            want_deleted: 3,
            want_keys: &["a", "b"],
        },
        TestCase {
            key: "e",
            opts: None,
            want_deleted: 0,
            want_keys: &keys,
        },
    ];

    for test in tests {
        for key in keys {
            client.put(key, "bar", None).await?;
        }

        let res = client.delete(test.key, test.opts).await?;
        assert_eq!(res.deleted, test.want_deleted);

        let res = client
            .get("", Some(RangeOptions::new().with_all_keys()))
            .await?;
        let is_identical = res
            .kvs
            .iter()
            .zip(test.want_keys.iter())
            .all(|(kv, want)| kv.key == want.as_bytes());
        assert!(is_identical);
    }

    Ok(())
}
