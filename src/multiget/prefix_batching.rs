// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Prefix-aware batching for multiget operations
//!
//! This module leverages the existing prefix bloom filter infrastructure
//! to optimize batch operations by grouping keys with common prefixes.

use crate::{prefix::SharedPrefixExtractor, segment::Segment};
use std::collections::HashMap;

/// A batch of keys grouped by common prefix
#[derive(Debug, Clone)]
pub struct PrefixBatch {
    /// The common prefix for this batch (empty for now)
    pub prefix: Vec<u8>,
    /// Keys in this batch with their original indices
    pub keys: Vec<super::KeyWithIndex>,
    /// Pre-computed bloom filter hashes
    pub key_hashes: Vec<u64>,
}

/// Prefix-aware batch organizer
pub struct PrefixBatchOrganizer {
    /// Prefix extractor to use
    prefix_extractor: Option<SharedPrefixExtractor>,
    /// Batches organized by prefix
    prefix_batches: HashMap<Vec<u8>, Vec<super::KeyWithIndex>>,
}

impl PrefixBatchOrganizer {
    /// Create a new prefix batch organizer
    pub fn new(prefix_extractor: Option<SharedPrefixExtractor>) -> Self {
        Self {
            prefix_extractor,
            prefix_batches: HashMap::new(),
        }
    }

    /// Add keys to be organized by prefix
    pub fn add_keys(&mut self, keys: Vec<super::KeyWithIndex>) {
        if let Some(ref extractor) = self.prefix_extractor {
            // Group keys by their prefixes
            for key_with_index in keys {
                // Get the first prefix (most common case for multiget)
                let prefixes: Vec<_> = extractor.extract(&key_with_index.key).collect();
                let prefix_vec = if let Some(prefix) = prefixes.first() {
                    prefix.to_vec()
                } else {
                    vec![] // Out of domain - empty prefix batch
                };

                self.prefix_batches
                    .entry(prefix_vec)
                    .or_default()
                    .push(key_with_index);
            }
        } else {
            // No prefix extractor - put all keys in a single "batch"
            self.prefix_batches.entry(vec![]).or_default().extend(keys);
        }
    }

    /// Get organized prefix batches
    pub fn into_batches(self) -> Vec<PrefixBatch> {
        self.prefix_batches
            .into_iter()
            .map(|(prefix, keys)| {
                let key_hashes = keys
                    .iter()
                    .map(|k| crate::segment::filter::standard_bloom::Builder::get_hash(&k.key))
                    .collect();

                PrefixBatch {
                    prefix,
                    keys,
                    key_hashes,
                }
            })
            .collect()
    }

    /// Check if a segment might contain any keys from a prefix batch
    pub fn segment_might_contain_prefix(
        segment: &Segment,
        prefix_batch: &PrefixBatch,
    ) -> crate::Result<bool> {
        // Use prefix bloom filter if available and compatible
        if let Some(ref extractor) = segment.prefix_extractor {
            if segment.prefix_extractor_compatible && !prefix_batch.prefix.is_empty() {
                if let Some(block) = &segment.pinned_filter_block {
                    let filter =
                        crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                            &block.data,
                        )?;

                    // Use prefix-based filtering for this batch
                    match filter.contains_prefix(&prefix_batch.prefix, extractor.as_ref()) {
                        Some(result) => return Ok(result),
                        None => {} // Out of domain, continue with hash-based checks
                    }
                } else if let Some(filter_block_handle) = &segment.regions.filter {
                    let block = crate::segment::util::load_block(
                        segment.global_id(),
                        &segment.path,
                        &segment.descriptor_table,
                        &segment.cache,
                        filter_block_handle,
                        crate::segment::block::BlockType::Filter,
                        crate::CompressionType::None,
                        #[cfg(feature = "metrics")]
                        &segment.metrics,
                    )?;

                    let filter =
                        crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                            &block.data,
                        )?;

                    match filter.contains_prefix(&prefix_batch.prefix, extractor.as_ref()) {
                        Some(result) => return Ok(result),
                        None => {} // Out of domain, continue with hash-based checks
                    }
                }
            }
        }

        // Fall back to individual hash checks
        if let Some(block) = &segment.pinned_filter_block {
            let filter = crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                &block.data,
            )?;

            // Check if any key in the batch might exist
            for &hash in &prefix_batch.key_hashes {
                if filter.contains_hash(hash) {
                    return Ok(true);
                }
            }
            Ok(false)
        } else if let Some(filter_block_handle) = &segment.regions.filter {
            let block = crate::segment::util::load_block(
                segment.global_id(),
                &segment.path,
                &segment.descriptor_table,
                &segment.cache,
                filter_block_handle,
                crate::segment::block::BlockType::Filter,
                crate::CompressionType::None,
                #[cfg(feature = "metrics")]
                &segment.metrics,
            )?;

            let filter = crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                &block.data,
            )?;

            for &hash in &prefix_batch.key_hashes {
                if filter.contains_hash(hash) {
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            // No bloom filter - assume keys might be present
            Ok(true)
        }
    }
}

/// Scoring information for segment access optimization
#[derive(Debug, Clone)]
pub struct SegmentPrefixScore {
    /// Segment ID
    pub segment_id: u64,
    /// Score based on likely key matches
    pub score: f64,
    /// Number of prefix batches that might match
    pub matching_batches: usize,
}

/// Advanced prefix bloom filter query optimizer
pub struct PrefixBloomOptimizer;

impl PrefixBloomOptimizer {
    /// Check multiple prefixes against a bloom filter in one operation
    pub fn batch_prefix_check(segment: &Segment, prefixes: &[Vec<u8>]) -> crate::Result<Vec<bool>> {
        if let Some(ref extractor) = segment.prefix_extractor {
            if segment.prefix_extractor_compatible {
                // Use pinned filter if available
                if let Some(block) = &segment.pinned_filter_block {
                    let filter =
                        crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                            &block.data,
                        )?;
                    let results = prefixes
                        .iter()
                        .map(|prefix| {
                            filter
                                .contains_prefix(prefix, extractor.as_ref())
                                .unwrap_or(true) // If out of domain, assume present
                        })
                        .collect();
                    return Ok(results);
                }

                // Load filter from disk if needed
                if let Some(filter_block_handle) = &segment.regions.filter {
                    let block = crate::segment::util::load_block(
                        segment.global_id(),
                        &segment.path,
                        &segment.descriptor_table,
                        &segment.cache,
                        filter_block_handle,
                        crate::segment::block::BlockType::Filter,
                        crate::CompressionType::None,
                        #[cfg(feature = "metrics")]
                        &segment.metrics,
                    )?;

                    let filter =
                        crate::segment::filter::standard_bloom::StandardBloomFilterReader::new(
                            &block.data,
                        )?;
                    let results = prefixes
                        .iter()
                        .map(|prefix| {
                            filter
                                .contains_prefix(prefix, extractor.as_ref())
                                .unwrap_or(true) // If out of domain, assume present
                        })
                        .collect();
                    return Ok(results);
                }

                // No bloom filter - all prefixes might be present
                Ok(vec![true; prefixes.len()])
            } else {
                // Extractor not compatible - can't do prefix filtering
                Ok(vec![true; prefixes.len()])
            }
        } else {
            // No prefix extractor - can't do prefix filtering
            Ok(vec![true; prefixes.len()])
        }
    }

    /// Estimate the selectivity of a prefix query
    pub fn estimate_prefix_selectivity(segment: &Segment, prefix: &[u8]) -> f64 {
        // This is a heuristic estimation
        // In a real implementation, we might maintain statistics

        if prefix.is_empty() {
            return 1.0; // Empty prefix matches everything
        }

        // Longer prefixes are generally more selective
        let base_selectivity = 1.0 / (256_f64.powf(prefix.len() as f64));

        // Adjust based on item count if available
        let key_count = segment.metadata.item_count as f64;
        if key_count > 0.0 {
            (base_selectivity * key_count).min(1.0)
        } else {
            base_selectivity
        }
    }

    /// Optimize segment access order based on prefix likelihood
    pub fn optimize_segment_order(
        segments: &[&Segment],
        prefix_batches: &[PrefixBatch],
    ) -> Vec<SegmentPrefixScore> {
        let mut segment_scores = Vec::new();

        for segment in segments {
            let mut total_score = 0.0;
            let mut batch_count = 0;

            for batch in prefix_batches {
                if let Ok(might_contain) =
                    PrefixBatchOrganizer::segment_might_contain_prefix(segment, batch)
                {
                    if might_contain {
                        // Weight by batch size - larger batches get higher priority
                        total_score += batch.keys.len() as f64;
                        batch_count += 1;
                    }
                }
            }

            if batch_count > 0 {
                segment_scores.push(SegmentPrefixScore {
                    segment_id: segment.id(),
                    score: total_score,
                    matching_batches: batch_count,
                });
            }
        }

        // Sort by score descending (highest scoring segments first)
        segment_scores.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        segment_scores
    }
}
