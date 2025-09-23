use lsm_tree::multiget::{MultiGetConfig, MultiGetRequest};
use lsm_tree::prefix::FixedPrefixExtractor;
use lsm_tree::{AbstractTree, BlobTree, Config};
use std::sync::Arc;
use test_log::test;

#[test]
fn test_multiget_basic_functionality() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert test data
    tree.insert("key1", "value1", 0);
    tree.insert("key2", "value2", 1);
    tree.insert("key3", "value3", 2);
    tree.insert("key4", "value4", 3);

    // Test multiget with all keys
    let request = MultiGetRequest::from_strings(vec!["key1", "key2", "key3", "key4", "key5"], 4);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 5);

    // Verify found keys
    assert_eq!(
        response.results[0].value.as_ref().map(|v| v.as_ref()),
        Some(b"value1".as_ref())
    );
    assert_eq!(
        response.results[1].value.as_ref().map(|v| v.as_ref()),
        Some(b"value2".as_ref())
    );
    assert_eq!(
        response.results[2].value.as_ref().map(|v| v.as_ref()),
        Some(b"value3".as_ref())
    );
    assert_eq!(
        response.results[3].value.as_ref().map(|v| v.as_ref()),
        Some(b"value4".as_ref())
    );

    // Verify not found key
    assert_eq!(response.results[4].value, None);
    assert_eq!(response.results[4].error, None);

    Ok(())
}

#[test]
fn test_multiget_across_storage_layers() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert test data in memtable
    tree.insert("mem1", "memvalue1", 0);
    tree.insert("mem2", "memvalue2", 1);

    // Flush to create segments
    tree.flush_active_memtable(0)?;

    // Insert more data in new memtable
    tree.insert("new1", "newvalue1", 2);
    tree.insert("new2", "newvalue2", 3);

    // Test multiget across memtable and segments
    let request = MultiGetRequest::from_strings(vec!["mem1", "mem2", "new1", "new2", "missing"], 4);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 5);

    // Verify all found values
    assert_eq!(
        response.results[0].value.as_ref().map(|v| v.as_ref()),
        Some(b"memvalue1".as_ref())
    );
    assert_eq!(
        response.results[1].value.as_ref().map(|v| v.as_ref()),
        Some(b"memvalue2".as_ref())
    );
    assert_eq!(
        response.results[2].value.as_ref().map(|v| v.as_ref()),
        Some(b"newvalue1".as_ref())
    );
    assert_eq!(
        response.results[3].value.as_ref().map(|v| v.as_ref()),
        Some(b"newvalue2".as_ref())
    );

    // Verify missing key
    assert_eq!(response.results[4].value, None);

    Ok(())
}

#[test]
fn test_multiget_with_config() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert test data
    tree.insert("test1", "value1", 0);
    tree.insert("test2", "value2", 1);

    // Test with custom config
    let config = MultiGetConfig {
        max_batch_size: 10,
        enable_parallel_io: false,
        max_concurrent_ios: 1,
        use_io_uring: false,
    };

    let request = MultiGetRequest::from_strings(vec!["test1", "test2"], 2);
    let response = tree.multiget(request, Some(config))?;

    assert_eq!(response.results.len(), 2);
    assert!(response.results[0].value.is_some());
    assert!(response.results[1].value.is_some());

    Ok(())
}

#[test]
fn test_multiget_with_prefix_optimization() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    // Create tree with prefix extractor for optimized bloom filtering
    let tree = Config::new(temp_dir.path())
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
        .open()?;

    // Insert test data with common prefixes
    let prefixes = ["user", "item", "prod", "meta"];
    let mut all_keys = Vec::new();

    for prefix in &prefixes {
        for i in 0..25 {
            let key = format!("{}_key_{:03}", prefix, i);
            let value = format!("{}_value_{:03}", prefix, i);
            tree.insert(key.as_bytes(), value.as_bytes(), i as u64);
            all_keys.push(key);
        }
    }

    // Flush to create segments for bloom filter testing
    tree.flush_active_memtable(0)?;

    // Test multiget with mixed prefixes (should leverage prefix bloom filters)
    let test_keys = vec![
        "user_key_001",
        "user_key_010",
        "user_key_020",
        "item_key_005",
        "item_key_015",
        "prod_key_002",
        "prod_key_012",
        "meta_key_008",
        "nonexistent_key_001", // Should be quickly filtered out
    ];

    let request = MultiGetRequest::from_strings(test_keys.clone(), 100);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), test_keys.len());

    // Verify found keys (8 should be found, 1 should be missing)
    let found_count = response
        .results
        .iter()
        .filter(|r| r.value.is_some())
        .count();
    assert_eq!(found_count, 8);

    // Verify the nonexistent key is not found
    assert!(response.results.last().unwrap().value.is_none());

    Ok(())
}

#[test]
fn test_multiget_with_async_io_config() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert test data
    for i in 0..100 {
        let key = format!("async_key_{:03}", i);
        let value = format!("async_value_{:03}", i);
        tree.insert(key.as_bytes(), value.as_bytes(), i as u64);
    }

    // Flush to create segments
    tree.flush_active_memtable(0)?;

    // Test with async I/O configuration
    let config = MultiGetConfig {
        max_batch_size: 50,
        enable_parallel_io: true,
        max_concurrent_ios: 8,
        use_io_uring: true, // Will fall back to thread pool on non-Linux
    };

    let test_keys: Vec<String> = (0..50).map(|i| format!("async_key_{:03}", i)).collect();
    let request =
        MultiGetRequest::new(test_keys.iter().map(|k| k.as_bytes().into()).collect(), 101);

    let response = tree.multiget(request, Some(config))?;

    assert_eq!(response.results.len(), 50);

    // All keys should be found
    for result in &response.results {
        assert!(result.value.is_some());
        assert!(result.error.is_none());
    }

    Ok(())
}

#[test]
fn test_multiget_blob_tree() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree: BlobTree = Config::new(temp_dir.path())
        .blob_file_target_size(1024) // Small blob files
        .open_as_blob_tree()?;

    // Insert large values that will be stored as blobs
    for i in 0..25 {
        let key = format!("blob_key_{:03}", i);
        let value = "x".repeat(200); // Large value to ensure blob storage
        tree.insert(key.as_bytes(), value.as_bytes(), i as u64);
    }

    // Flush to create blob files
    tree.flush_active_memtable(0)?;

    // Test multiget with blob tree optimization
    let test_keys: Vec<String> = (0..25).map(|i| format!("blob_key_{:03}", i)).collect();
    let request =
        MultiGetRequest::new(test_keys.iter().map(|k| k.as_bytes().into()).collect(), 100);

    let config = MultiGetConfig {
        max_batch_size: 25,
        enable_parallel_io: true,
        max_concurrent_ios: 4,
        use_io_uring: false,
    };

    let response = tree.multiget(request, Some(config))?;

    assert_eq!(response.results.len(), 25);

    // All keys should be found with correct sizes
    for (i, result) in response.results.iter().enumerate() {
        assert!(
            result.value.is_some(),
            "Expected key {} to be found",
            test_keys[i]
        );
        if let Some(ref value) = result.value {
            assert_eq!(value.as_ref().len(), 200usize); // Verify value size
        }
    }

    Ok(())
}

#[test]
#[ignore] // TODO: Fix MVCC sequence number handling in multiget
fn test_multiget_mvcc_consistency() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert at different sequence numbers
    tree.insert("mvcc_key", "version_1", 0);
    tree.insert("mvcc_key", "version_2", 1);
    tree.insert("mvcc_key", "version_3", 2);
    tree.flush_active_memtable(2)?;

    // Test reading at different sequence numbers
    let request_v1 = MultiGetRequest::from_strings(vec!["mvcc_key"], 0);
    let response_v1 = tree.multiget(request_v1, None)?;
    assert_eq!(
        response_v1.results[0].value,
        Some("version_1".as_bytes().into())
    );

    let request_v2 = MultiGetRequest::from_strings(vec!["mvcc_key"], 1);
    let response_v2 = tree.multiget(request_v2, None)?;
    assert_eq!(
        response_v2.results[0].value,
        Some("version_2".as_bytes().into())
    );

    let request_v3 = MultiGetRequest::from_strings(vec!["mvcc_key"], 2);
    let response_v3 = tree.multiget(request_v3, None)?;
    assert_eq!(
        response_v3.results[0].value,
        Some("version_3".as_bytes().into())
    );

    Ok(())
}

#[test]
fn test_multiget_tombstones() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert and then delete
    tree.insert("key1", "value1", 0);
    tree.insert("key2", "value2", 0);
    tree.remove("key1", 1); // Create tombstone
    tree.flush_active_memtable(1)?;

    let request = MultiGetRequest::from_strings(vec!["key1", "key2"], 2);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 2);
    assert!(response.results[0].value.is_none()); // key1 deleted
    assert_eq!(response.results[1].value, Some("value2".as_bytes().into())); // key2 exists
    Ok(())
}

#[test]
fn test_multiget_performance_vs_individual_gets() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert test data
    let keys: Vec<String> = (0..1000).map(|i| format!("key_{:04}", i)).collect();
    let values: Vec<String> = (0..1000).map(|i| format!("value_{:04}", i)).collect();

    for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
        tree.insert(key.as_bytes(), value.as_bytes(), i as u64);
    }

    // Flush to create segments
    tree.flush_active_memtable(0)?;

    // Time individual gets
    let individual_start = std::time::Instant::now();
    let mut individual_results = Vec::new();
    for key in &keys[..100] {
        // Test first 100 keys
        if let Some(value) = tree.get(key.as_bytes(), 1000)? {
            individual_results.push(value);
        }
    }
    let individual_duration = individual_start.elapsed();

    // Time multiget
    let multiget_start = std::time::Instant::now();
    let request = MultiGetRequest::new(
        keys[..100].iter().map(|k| k.as_bytes().into()).collect(),
        1000,
    );
    let response = tree.multiget(request, None)?;
    let multiget_duration = multiget_start.elapsed();

    // Verify same results
    assert_eq!(individual_results.len(), 100);
    assert_eq!(response.results.len(), 100);

    // Verify all results are found
    for result in &response.results {
        assert!(result.value.is_some());
        assert!(result.error.is_none());
    }

    println!("Individual gets took: {:?}", individual_duration);
    println!("MultiGet took: {:?}", multiget_duration);

    Ok(())
}

#[test]
fn test_multiget_performance_scaling() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;

    // Create tree with prefix extractor for better bloom filter performance
    let tree = Config::new(temp_dir.path())
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(6)))
        .open()?;

    // Insert a large dataset with prefixed keys
    let batch_sizes = [10, 50, 100, 500];
    let key_count = 2000;

    for i in 0..key_count {
        let prefix = format!("batch{:02}", i % 10); // 10 different prefixes
        let key = format!("{}_key_{:04}", prefix, i);
        let value = format!("value_{:04}", i);
        tree.insert(key.as_bytes(), value.as_bytes(), i as u64);
    }

    // Flush to create segments
    tree.flush_active_memtable(0)?;

    // Test different batch sizes to verify scaling
    for &batch_size in &batch_sizes {
        let test_keys: Vec<String> = (0..batch_size)
            .map(|i| {
                let prefix = format!("batch{:02}", i % 10);
                format!("{}_key_{:04}", prefix, i)
            })
            .collect();

        let start_time = std::time::Instant::now();

        let request = MultiGetRequest::new(
            test_keys.iter().map(|k| k.as_bytes().into()).collect(),
            key_count as u64 + 1,
        );

        let config = MultiGetConfig {
            max_batch_size: batch_size,
            enable_parallel_io: true,
            max_concurrent_ios: 4,
            use_io_uring: false, // Use thread pool for cross-platform compatibility
        };

        let response = tree.multiget(request, Some(config))?;
        let duration = start_time.elapsed();

        assert_eq!(response.results.len(), batch_size);

        // All keys should be found
        let found_count = response
            .results
            .iter()
            .filter(|r| r.value.is_some())
            .count();
        assert_eq!(found_count, batch_size);

        println!(
            "Batch size {}: {:?} ({:.2} μs per key)",
            batch_size,
            duration,
            duration.as_micros() as f64 / batch_size as f64
        );
    }

    Ok(())
}

#[test]
fn test_multiget_empty_request() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    let request = MultiGetRequest::new(vec![], 0);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 0);
    Ok(())
}

#[test]
fn test_multiget_single_key() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    tree.insert("key1", "value1", 0);
    tree.flush_active_memtable(0)?;

    let request = MultiGetRequest::from_strings(vec!["key1"], 1);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].value, Some("value1".as_bytes().into()));
    Ok(())
}

#[test]
fn test_multiget_all_missing() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    // Insert some data but query different keys
    tree.insert("existing1", "value1", 0);
    tree.insert("existing2", "value2", 0);
    tree.flush_active_memtable(0)?;

    let request = MultiGetRequest::from_strings(vec!["missing1", "missing2", "missing3"], 1);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 3);
    for result in &response.results {
        assert!(result.value.is_none());
        assert!(result.error.is_none());
    }
    Ok(())
}

#[test]
fn test_multiget_duplicate_keys() -> lsm_tree::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let tree = Config::new(temp_dir.path()).open()?;

    tree.insert("key1", "value1", 0);
    tree.flush_active_memtable(0)?;

    let request = MultiGetRequest::from_strings(vec!["key1", "key1", "key1"], 1);
    let response = tree.multiget(request, None)?;

    assert_eq!(response.results.len(), 3);
    for result in &response.results {
        assert_eq!(result.value, Some("value1".as_bytes().into()));
    }
    Ok(())
}
