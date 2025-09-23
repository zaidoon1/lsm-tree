// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Async I/O support for multiget operations
//!
//! This module provides async I/O capabilities using io_uring on Linux
//! and fallback to thread pools on other platforms.

use crate::{segment::Block, segment::BlockHandle, CompressionType, GlobalSegmentId};
use std::{collections::HashMap, path::Path, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Result of an async I/O operation
#[derive(Debug)]
pub struct AsyncIoResult {
    /// The segment ID this block belongs to
    pub segment_id: GlobalSegmentId,
    /// The block handle that was requested
    pub handle: BlockHandle,
    /// The loaded block, or error if failed
    pub result: crate::Result<Block>,
}

/// Async I/O operation request
#[derive(Debug, Clone)]
pub struct AsyncIoRequest {
    /// Segment ID for this request
    pub segment_id: GlobalSegmentId,
    /// Path to the segment file
    pub path: Arc<std::path::PathBuf>,
    /// Block handle to load
    pub handle: BlockHandle,
    /// Block type (for metrics)
    pub block_type: crate::segment::block::BlockType,
    /// Compression type
    pub compression: CompressionType,
}

/// Async I/O scheduler for parallel data block operations
pub struct AsyncIoScheduler {
    /// Whether io_uring is available and enabled
    use_io_uring: bool,
    /// Maximum concurrent I/O operations
    max_concurrent: usize,
    /// Thread pool for parallel I/O
    #[cfg(feature = "multiget_advanced")]
    thread_pool: Option<rayon::ThreadPool>,
}

impl AsyncIoScheduler {
    /// Create a new async I/O scheduler
    pub fn new(use_io_uring: bool, max_concurrent: usize) -> Self {
        Self {
            use_io_uring: use_io_uring && cfg!(target_os = "linux"),
            max_concurrent,
            #[cfg(feature = "multiget_advanced")]
            thread_pool: Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(std::cmp::max(1, max_concurrent))
                    .thread_name(|i| format!("lsm-multiget-{}", i))
                    .build()
                    .ok(),
            )
            .flatten(),
        }
    }

    /// Submit multiple I/O requests and wait for all to complete
    pub async fn submit_and_wait(
        &self,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Vec<AsyncIoResult> {
        if requests.is_empty() {
            return Vec::new();
        }

        if self.use_io_uring {
            #[cfg(target_os = "linux")]
            {
                self.submit_io_uring(
                    requests,
                    descriptor_table,
                    cache,
                    #[cfg(feature = "metrics")]
                    metrics,
                )
                .await
            }
            #[cfg(not(target_os = "linux"))]
            {
                // Fallback to thread pool
                self.submit_thread_pool(
                    requests,
                    descriptor_table,
                    cache,
                    #[cfg(feature = "metrics")]
                    metrics,
                )
                .await
            }
        } else {
            // Use thread pool for parallel I/O
            self.submit_thread_pool(
                requests,
                descriptor_table,
                cache,
                #[cfg(feature = "metrics")]
                metrics,
            )
            .await
        }
    }

    #[cfg(target_os = "linux")]
    async fn submit_io_uring(
        &self,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Vec<AsyncIoResult> {
        #[cfg(feature = "io_uring")]
        {
            self.submit_io_uring_impl(
                requests,
                descriptor_table,
                cache,
                #[cfg(feature = "metrics")]
                metrics,
            )
            .await
        }
        #[cfg(not(feature = "io_uring"))]
        {
            // Fall back to thread pool if io_uring feature not enabled
            self.submit_thread_pool(
                requests,
                descriptor_table,
                cache,
                #[cfg(feature = "metrics")]
                metrics,
            )
            .await
        }
    }

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    async fn submit_io_uring_impl(
        &self,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Vec<AsyncIoResult> {
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use tokio_uring::fs::File;

        // Group requests by file for batch operations
        let mut file_requests: BTreeMap<Arc<std::path::PathBuf>, Vec<AsyncIoRequest>> =
            BTreeMap::new();
        for request in requests {
            file_requests
                .entry(request.path.clone())
                .or_default()
                .push(request);
        }

        let mut all_results = Vec::new();

        // Process each file's requests using io_uring
        for (file_path, requests) in file_requests {
            match File::open(&*file_path).await {
                Ok(file) => {
                    let mut file_results = Vec::new();

                    // Submit all reads for this file in parallel
                    let mut read_futures = Vec::new();

                    for request in &requests {
                        let block_handle = &request.handle;
                        let mut buffer = vec![0u8; block_handle.size as usize];

                        // Submit async read using io_uring
                        let read_future = file.read_at(buffer, block_handle.offset);
                        read_futures.push((read_future, request.clone()));
                    }

                    // Wait for all reads to complete
                    for (read_future, request) in read_futures {
                        match read_future.await {
                            Ok((result, buffer)) => {
                                if result == buffer.len() {
                                    // Successfully read the block
                                    let block = crate::segment::block::Block {
                                        header: crate::segment::block::header::Header {
                                            data_length: buffer.len() as u32,
                                            ..Default::default()
                                        },
                                        data: buffer.into(),
                                    };

                                    file_results.push(AsyncIoResult {
                                        segment_id: request.segment_id,
                                        handle: request.handle,
                                        result: Ok(block),
                                    });
                                } else {
                                    file_results.push(AsyncIoResult {
                                        segment_id: request.segment_id,
                                        handle: request.handle,
                                        result: Err(crate::Error::Io(std::io::Error::new(
                                            std::io::ErrorKind::UnexpectedEof,
                                            "Incomplete read",
                                        ))),
                                    });
                                }
                            }
                            Err(e) => {
                                file_results.push(AsyncIoResult {
                                    segment_id: request.segment_id,
                                    handle: request.handle,
                                    result: Err(crate::Error::Io(e)),
                                });
                            }
                        }
                    }

                    all_results.extend(file_results);
                }
                Err(e) => {
                    // File open failed - create error results for all requests
                    for request in requests {
                        all_results.push(AsyncIoResult {
                            segment_id: request.segment_id,
                            handle: request.handle,
                            result: Err(crate::Error::Io(e.kind().into())),
                        });
                    }
                }
            }
        }

        all_results
    }

    #[cfg(target_os = "linux")]
    async fn try_io_uring_for_file(
        &self,
        path: &Path,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Result<Vec<AsyncIoResult>, std::io::Error> {
        #[cfg(feature = "io_uring")]
        {
            self.try_io_uring_for_file_impl(
                path,
                requests,
                descriptor_table,
                cache,
                #[cfg(feature = "metrics")]
                metrics,
            )
            .await
        }
        #[cfg(not(feature = "io_uring"))]
        {
            // Fall back to blocking I/O when io_uring is not available
            let sync_results = Self::load_sync_batch_blocking(
                path,
                &requests,
                descriptor_table,
                cache,
                #[cfg(feature = "metrics")]
                metrics,
            );
            Ok(sync_results)
        }
    }

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    async fn try_io_uring_for_file_impl(
        &self,
        path: &Path,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Result<Vec<AsyncIoResult>, std::io::Error> {
        use std::collections::HashMap;
        use tokio_uring::fs::File;

        // Check cache first to avoid unnecessary I/O
        let mut results = Vec::new();
        let mut uncached_requests = Vec::new();

        for request in requests {
            if let Some(block) = cache.get_block(request.segment_id, request.handle.offset()) {
                results.push(AsyncIoResult {
                    segment_id: request.segment_id,
                    handle: request.handle,
                    result: Ok(block),
                });
            } else {
                uncached_requests.push(request);
            }
        }

        if uncached_requests.is_empty() {
            return Ok(results);
        }

        // Open the file using tokio-uring
        let file = File::open(path).await?;

        // Pre-allocate buffers and futures for optimal performance
        let num_requests = uncached_requests.len();
        let mut read_futures = Vec::with_capacity(num_requests);
        results.reserve(results.len() + num_requests);

        // Submit all I/O operations in parallel using io_uring
        for request in &uncached_requests {
            let block_handle = &request.handle;
            // Pre-allocate buffer with exact size needed
            let buffer = vec![0u8; block_handle.size as usize];

            // Submit async read - io_uring will queue this operation
            let read_future = file.read_at(buffer, block_handle.offset);
            read_futures.push(read_future);
        }

        // Use futures::future::join_all for true parallelism
        use futures::future::join_all;
        let read_results = join_all(read_futures).await;

        // Process results in parallel using rayon for CPU-bound work
        #[cfg(feature = "multiget_advanced")]
        {
            use rayon::prelude::*;

            let processed_results: Vec<AsyncIoResult> = read_results
                .into_par_iter()
                .zip(uncached_requests.into_par_iter())
                .map(|(read_result, request)| {
                    Self::process_read_result(
                        read_result,
                        request,
                        cache,
                        #[cfg(feature = "metrics")]
                        metrics,
                    )
                })
                .collect();

            results.extend(processed_results);
        }
        #[cfg(not(feature = "multiget_advanced"))]
        {
            // Fallback to sequential processing when rayon not available
            for (read_result, request) in
                read_results.into_iter().zip(uncached_requests.into_iter())
            {
                let processed = Self::process_read_result(
                    read_result,
                    request,
                    cache,
                    #[cfg(feature = "metrics")]
                    metrics,
                );
                results.push(processed);
            }
        }

        Ok(results)
    }

    /// Process a single read result with optimized CPU-bound operations
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    fn process_read_result(
        read_result: Result<(usize, Vec<u8>), std::io::Error>,
        request: AsyncIoRequest,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> AsyncIoResult {
        match read_result {
            Ok((bytes_read, buffer)) => {
                if bytes_read == buffer.len() {
                    // Fast path: successful read with exact size
                    match Self::process_block_data(
                        &buffer,
                        &request,
                        cache,
                        #[cfg(feature = "metrics")]
                        metrics,
                    ) {
                        Ok(block) => AsyncIoResult {
                            segment_id: request.segment_id,
                            handle: request.handle,
                            result: Ok(block),
                        },
                        Err(e) => AsyncIoResult {
                            segment_id: request.segment_id,
                            handle: request.handle,
                            result: Err(e),
                        },
                    }
                } else {
                    // Incomplete read error
                    AsyncIoResult {
                        segment_id: request.segment_id,
                        handle: request.handle,
                        result: Err(crate::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!(
                                "Expected {} bytes, read {}",
                                request.handle.size, bytes_read
                            ),
                        ))),
                    }
                }
            }
            Err(e) => AsyncIoResult {
                segment_id: request.segment_id,
                handle: request.handle,
                result: Err(crate::Error::Io(e)),
            },
        }
    }

    /// Optimized block data processing (decompression + parsing + caching)
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    fn process_block_data(
        buffer: &[u8],
        request: &AsyncIoRequest,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> crate::Result<crate::segment::block::Block> {
        // Optimized decompression path
        let decompressed_data = if request.compression != crate::CompressionType::None {
            // Use in-place decompression when possible to avoid extra allocation
            crate::segment::block::decompress_to_bytes(buffer, request.compression)?
        } else {
            // Zero-copy path for uncompressed data
            buffer.to_vec()
        };

        // Parse the block
        let block = crate::segment::block::Block::from_bytes(decompressed_data.into())?;

        // Cache the block for future use (clone is necessary here)
        cache.insert_block(request.segment_id, request.handle.offset(), block.clone());

        // Update metrics atomically
        #[cfg(feature = "metrics")]
        {
            let metric = match request.block_type {
                crate::segment::block::BlockType::Data => &metrics.data_block_load_io,
                crate::segment::block::BlockType::Index => &metrics.index_block_load_io,
                crate::segment::block::BlockType::Filter => &metrics.filter_block_load_io,
            };
            metric.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(block)
    }

    async fn submit_thread_pool(
        &self,
        requests: Vec<AsyncIoRequest>,
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Vec<AsyncIoResult> {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        // Group by file path for better I/O locality
        let mut file_groups: HashMap<Arc<std::path::PathBuf>, Vec<AsyncIoRequest>> = HashMap::new();
        for request in requests {
            file_groups
                .entry(request.path.clone())
                .or_default()
                .push(request);
        }

        let mut join_set = JoinSet::new();

        // Clone shared resources for async tasks
        let descriptor_table = Arc::new(descriptor_table.clone());
        let cache = Arc::new(cache.clone());
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(metrics.clone());

        // Optimize file group processing with pre-allocated capacity
        let total_requests: usize = file_groups.values().map(|reqs| reqs.len()).sum();
        let mut all_results = Vec::with_capacity(total_requests);

        // Process each file group in parallel with optimal task distribution
        #[cfg(feature = "multiget_advanced")]
        if let Some(ref thread_pool) = self.thread_pool {
            // Use dedicated rayon thread pool with optimized task batching
            let file_groups_vec: Vec<_> = file_groups.into_iter().collect();
            let num_files = file_groups_vec.len();

            // Batch smaller files together for better load balancing
            let optimal_batch_size = std::cmp::max(1, num_files / self.max_concurrent);

            for batch in file_groups_vec.chunks(optimal_batch_size) {
                let desc_table = descriptor_table.clone();
                let cache_ref = cache.clone();
                #[cfg(feature = "metrics")]
                let metrics_ref = metrics.clone();

                let (tx, rx) = tokio::sync::oneshot::channel();
                let batch_owned = batch.to_vec();

                thread_pool.spawn(move || {
                    let mut batch_results = Vec::new();
                    for (path, file_requests) in batch_owned {
                        let results = Self::load_sync_batch_blocking(
                            &path,
                            &file_requests,
                            &desc_table,
                            &cache_ref,
                            #[cfg(feature = "metrics")]
                            &metrics_ref,
                        );
                        batch_results.extend(results);
                    }
                    let _ = tx.send(batch_results);
                });

                join_set.spawn(async move { rx.await.unwrap_or_default() });
            }
        } else {
            // Fall back to tokio spawn_blocking
            for (path, file_requests) in file_groups {
                let desc_table = descriptor_table.clone();
                let cache_ref = cache.clone();
                #[cfg(feature = "metrics")]
                let metrics_ref = metrics.clone();

                join_set.spawn_blocking(move || {
                    Self::load_sync_batch_blocking(
                        &path,
                        &file_requests,
                        &desc_table,
                        &cache_ref,
                        #[cfg(feature = "metrics")]
                        &metrics_ref,
                    )
                });
            }
        }

        #[cfg(not(feature = "multiget_advanced"))]
        {
            // Use tokio spawn_blocking when rayon is not available
            for (path, file_requests) in file_groups {
                let desc_table = descriptor_table.clone();
                let cache_ref = cache.clone();
                #[cfg(feature = "metrics")]
                let metrics_ref = metrics.clone();

                join_set.spawn_blocking(move || {
                    Self::load_sync_batch_blocking(
                        &path,
                        &file_requests,
                        &desc_table,
                        &cache_ref,
                        #[cfg(feature = "metrics")]
                        &metrics_ref,
                    )
                });
            }
        }

        // Collect results from all parallel tasks with optimal allocation
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(batch_results) => all_results.extend(batch_results),
                Err(join_error) => {
                    // Handle join error - log but continue processing other batches
                    log::error!("Async I/O task failed: {}", join_error);
                }
            }
        }

        all_results
    }

    fn load_sync_batch_blocking(
        path: &Path,
        requests: &[AsyncIoRequest],
        descriptor_table: &crate::DescriptorTable,
        cache: &crate::Cache,
        #[cfg(feature = "metrics")] metrics: &Metrics,
    ) -> Vec<AsyncIoResult> {
        let mut results = Vec::new();

        for request in requests {
            let result = crate::segment::util::load_block(
                request.segment_id,
                path,
                descriptor_table,
                cache,
                &request.handle,
                request.block_type,
                request.compression,
                #[cfg(feature = "metrics")]
                metrics,
            );

            results.push(AsyncIoResult {
                segment_id: request.segment_id,
                handle: request.handle,
                result,
            });
        }

        results
    }
}

/// Utility for batching I/O requests by locality
pub struct IoLocalityOptimizer {
    /// Requests grouped by file path
    file_groups: HashMap<Arc<std::path::PathBuf>, Vec<AsyncIoRequest>>,
}

impl IoLocalityOptimizer {
    /// Create a new I/O locality optimizer
    pub fn new() -> Self {
        Self {
            file_groups: HashMap::new(),
        }
    }

    /// Add a request to be optimized
    pub fn add_request(&mut self, request: AsyncIoRequest) {
        self.file_groups
            .entry(request.path.clone())
            .or_default()
            .push(request);
    }

    /// Get optimized request groups (sorted by file offset for sequential access)
    pub fn optimize(mut self) -> HashMap<Arc<std::path::PathBuf>, Vec<AsyncIoRequest>> {
        // Sort each file group by offset for sequential access
        for (_, requests) in self.file_groups.iter_mut() {
            requests.sort_by_key(|req| req.handle.offset().0);
        }
        self.file_groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::BlockOffset;

    #[test]
    fn test_io_locality_optimizer() {
        let mut optimizer = IoLocalityOptimizer::new();

        let path1 = Arc::new(std::path::PathBuf::from("/test/file1"));
        let _path2 = Arc::new(std::path::PathBuf::from("/test/file2"));

        // Add requests out of order
        optimizer.add_request(AsyncIoRequest {
            segment_id: (1, 1).into(),
            path: path1.clone(),
            handle: BlockHandle::new(BlockOffset(1000), 100),
            block_type: crate::segment::block::BlockType::Data,
            compression: CompressionType::None,
        });

        optimizer.add_request(AsyncIoRequest {
            segment_id: (1, 1).into(),
            path: path1.clone(),
            handle: BlockHandle::new(BlockOffset(500), 100),
            block_type: crate::segment::block::BlockType::Data,
            compression: CompressionType::None,
        });

        let optimized = optimizer.optimize();

        // Verify requests are sorted by offset
        let file1_requests = &optimized[&path1];
        assert_eq!(file1_requests.len(), 2);
        assert!(file1_requests[0].handle.offset().0 < file1_requests[1].handle.offset().0);
    }
}
