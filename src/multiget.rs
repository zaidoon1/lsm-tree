// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! High-performance MultiGet implementation optimized for batch key lookups
//!
//! This module provides efficient batch key lookup operations that significantly
//! outperform individual get() calls by:
//! - Batching bloom filter operations to pipeline cache misses
//! - Reducing virtual function call overhead
//! - Enabling parallel I/O operations using io_uring
//! - Optimizing block cache usage patterns
//! - Prefix-aware batching for better bloom filter utilization
//! - Enhanced BlobTree optimization with value log batching

use crate::{InternalValue, SeqNo, UserKey, UserValue};
use std::collections::HashMap;

pub mod async_io;
pub mod blob_optimization;
pub mod prefix_batching;

pub use async_io::{AsyncIoRequest, AsyncIoResult, AsyncIoScheduler};
pub use blob_optimization::BlobTreeMultiget;
pub use prefix_batching::{PrefixBatch, PrefixBatchOrganizer, PrefixBloomOptimizer};

/// A batch request for multiple keys
#[derive(Debug, Clone)]
pub struct MultiGetRequest {
    /// The keys to lookup
    pub keys: Vec<UserKey>,
    /// The sequence number for MVCC consistency
    pub seqno: SeqNo,
}

/// Result of a single key lookup in a multiget operation
#[derive(Debug)]
pub struct MultiGetResult {
    /// The key that was looked up
    pub key: UserKey,
    /// The value if found, None if not found
    pub value: Option<UserValue>,
    /// Any error that occurred during lookup
    pub error: Option<String>, // Use String instead of Error for cloning
}

/// Batch result containing all lookup results
#[derive(Debug)]
pub struct MultiGetResponse {
    /// Results for each key in the same order as the request
    pub results: Vec<MultiGetResult>,
}

/// Internal structure for batching keys by segment
#[derive(Debug)]
pub(crate) struct SegmentBatch {
    /// Segment ID for this batch
    pub segment_id: u64,
    /// Keys that need to be looked up in this segment
    pub keys: Vec<KeyWithIndex>,
    /// Pre-computed hashes for bloom filter operations
    pub key_hashes: Vec<u64>,
}

/// Internal structure linking a key to its position in the original request
#[derive(Debug, Clone)]
pub(crate) struct KeyWithIndex {
    /// The key to lookup
    pub key: UserKey,
    /// Index in the original request array
    pub index: usize,
}

/// Optimized multiget state machine for pipelining operations
#[derive(Debug)]
pub(crate) struct MultiGetContext {
    /// Original request
    pub request: MultiGetRequest,
    /// Results array (initially empty)
    pub results: Vec<Option<UserValue>>,
    /// Error tracking per key
    pub errors: Vec<Option<String>>,
    /// Keys still needing lookup (not found in memtables)
    pub remaining_keys: Vec<KeyWithIndex>,
    /// Pre-computed bloom filter hashes
    pub key_hashes: HashMap<UserKey, u64>,
}

impl MultiGetRequest {
    /// Create a new multiget request
    pub fn new(keys: Vec<UserKey>, seqno: SeqNo) -> Self {
        Self { keys, seqno }
    }

    /// Create a multiget request with string keys (convenience method)
    pub fn from_strings(keys: Vec<&str>, seqno: SeqNo) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.as_bytes().into()).collect(),
            seqno,
        }
    }
}

impl MultiGetContext {
    /// Initialize a new multiget context
    pub fn new(request: MultiGetRequest) -> Self {
        let key_count = request.keys.len();
        let mut key_hashes = HashMap::with_capacity(key_count);
        let mut remaining_keys = Vec::with_capacity(key_count);

        // Pre-compute all bloom filter hashes
        for (index, key) in request.keys.iter().enumerate() {
            let hash = crate::segment::filter::standard_bloom::Builder::get_hash(key);
            key_hashes.insert(key.clone(), hash);
            remaining_keys.push(KeyWithIndex {
                key: key.clone(),
                index,
            });
        }

        Self {
            request,
            results: vec![None; key_count],
            errors: vec![None; key_count],
            remaining_keys,
            key_hashes,
        }
    }

    /// Mark a key as found with the given value
    pub fn mark_found(&mut self, index: usize, value: UserValue) {
        self.results[index] = Some(value);
        // Note: Don't remove from remaining_keys here for performance
        // We'll filter when checking is_complete()
    }

    /// Mark a key as having an error
    pub fn mark_error(&mut self, index: usize, error: String) {
        self.errors[index] = Some(error);
        // Note: Don't remove from remaining_keys here for performance
    }

    /// Mark a key as found and remove from remaining keys (batched removal)
    pub fn mark_found_and_remove(&mut self, index: usize, value: UserValue) {
        self.results[index] = Some(value);
        // Use swap_remove for O(1) performance
        if let Some(pos) = self.remaining_keys.iter().position(|k| k.index == index) {
            self.remaining_keys.swap_remove(pos);
        }
    }

    /// Bulk remove found keys from remaining keys (optimized)
    pub fn remove_found_keys(&mut self) {
        self.remaining_keys
            .retain(|k| self.results[k.index].is_none() && self.errors[k.index].is_none());
    }

    /// Check if all keys have been resolved
    pub fn is_complete(&self) -> bool {
        self.remaining_keys.is_empty()
    }

    /// Convert to final response
    pub fn into_response(self) -> MultiGetResponse {
        let results = self
            .request
            .keys
            .into_iter()
            .enumerate()
            .map(|(i, key)| MultiGetResult {
                key,
                value: self.results[i].clone(),
                error: self.errors[i].clone(),
            })
            .collect();

        MultiGetResponse { results }
    }

    /// Get the hash for a key (pre-computed)
    pub fn get_key_hash(&self, key: &UserKey) -> u64 {
        self.key_hashes[key]
    }
}

/// Configuration for multiget operations
#[derive(Debug, Clone)]
pub struct MultiGetConfig {
    /// Maximum number of keys to process in a single batch
    pub max_batch_size: usize,
    /// Whether to use parallel I/O operations
    pub enable_parallel_io: bool,
    /// Number of concurrent I/O operations to allow
    pub max_concurrent_ios: usize,
    /// Whether to use io_uring for async I/O (Linux only)
    pub use_io_uring: bool,
}

impl Default for MultiGetConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            enable_parallel_io: true,
            max_concurrent_ios: 16,
            use_io_uring: cfg!(target_os = "linux"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiget_request_creation() {
        let keys = vec![b"key1".to_vec().into(), b"key2".to_vec().into()];
        let request = MultiGetRequest::new(keys.clone(), 100);

        assert_eq!(request.keys, keys);
        assert_eq!(request.seqno, 100);
    }

    #[test]
    fn test_multiget_context_initialization() {
        let keys = vec![b"key1".to_vec().into(), b"key2".to_vec().into()];
        let request = MultiGetRequest::new(keys.clone(), 100);
        let context = MultiGetContext::new(request);

        assert_eq!(context.results.len(), 2);
        assert_eq!(context.errors.len(), 2);
        assert_eq!(context.remaining_keys.len(), 2);
        assert_eq!(context.key_hashes.len(), 2);
    }

    #[test]
    fn test_multiget_context_mark_found() {
        let keys = vec![b"key1".to_vec().into(), b"key2".to_vec().into()];
        let request = MultiGetRequest::new(keys.clone(), 100);
        let mut context = MultiGetContext::new(request);

        let value: UserValue = b"value1".to_vec().into();
        context.mark_found(0, value.clone());

        assert_eq!(context.results[0], Some(value));
        // After optimization, remaining_keys are not removed immediately
        assert_eq!(context.remaining_keys.len(), 2);

        // But we can call remove_found_keys to clean them up
        context.remove_found_keys();
        assert_eq!(context.remaining_keys.len(), 1);
        assert_eq!(context.remaining_keys[0].index, 1);
    }
}
