// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, GlobalTableId};
use crate::{
    table::block::BlockType, version::run::Ranged, Cache, CompressionType, DescriptorTable,
    KeyRange, Table,
};
use std::{path::Path, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

#[must_use]
pub fn aggregate_run_key_range(tables: &[Table]) -> KeyRange {
    let lo = tables.first().expect("run should never be empty");
    let hi = tables.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Shared state for a single logical read against one table.
///
/// Carries an opened descriptor that is reused across multiple block loads in a
/// point lookup or range iteration. This avoids repeated descriptor-table
/// lookups and redundant `File::open` calls while still going through the
/// standard block cache and metrics accounting.
#[derive(Default)]
pub struct ReadContext {
    pub(crate) fd: Option<Arc<std::fs::File>>,
}

// Uses the descriptor stored in `ctx` when available so filter/index/data blocks for a
// single table share one `Arc<File>`. Falls back to the descriptor cache, and only
// opens the underlying file when both context and cache miss.
#[warn(clippy::too_many_arguments)]
pub fn load_block_with_ctx(
    table_id: GlobalTableId,
    path: &Path,
    descriptor_table: &DescriptorTable,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    ctx: &mut ReadContext,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use std::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?} (ctx)");

    if let Some(block) = cache.get_block(table_id, handle.offset()) {
        #[cfg(feature = "metrics")]
        match block_type {
            BlockType::Filter => {
                metrics.filter_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Index => {
                metrics.index_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Data => {
                metrics.data_block_load_cached.fetch_add(1, Relaxed);
            }
            _ => {}
        };
        return Ok(block);
    }

    // Prefer a descriptor already held by the current read path; otherwise consult the
    // descriptor cache. On a full miss, open the file once and keep it both in the
    // context and the descriptor cache so later block loads for this table can reuse it.
    let mut fd_cache_miss = false;

    let fd = if let Some(fd) = &ctx.fd {
        fd.clone()
    } else if let Some(fd) = descriptor_table.access_for_table(&table_id) {
        #[cfg(feature = "metrics")]
        metrics.table_file_opened_cached.fetch_add(1, Relaxed);
        ctx.fd = Some(fd.clone());
        fd
    } else {
        let fd = std::fs::File::open(path)?;
        #[cfg(feature = "metrics")]
        metrics.table_file_opened.fetch_add(1, Relaxed);
        let fd = Arc::new(fd);
        ctx.fd = Some(fd.clone());
        fd_cache_miss = true;
        fd
    };

    let block = Block::from_file(&fd, *handle, compression)?;

    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    #[cfg(feature = "metrics")]
    match block_type {
        BlockType::Filter => {
            metrics.filter_block_load_io.fetch_add(1, Relaxed);
        }
        BlockType::Index => {
            metrics.index_block_load_io.fetch_add(1, Relaxed);
        }
        BlockType::Data => {
            metrics.data_block_load_io.fetch_add(1, Relaxed);
        }
        _ => {}
    };

    if fd_cache_miss {
        descriptor_table.insert_for_table(table_id, fd);
    }

    cache.insert_block(table_id, handle.offset(), block.clone());
    Ok(block)
}

// Accepts an optional `fd_hint` provided by higher-level iterators.
//
// When the hint is present, the descriptor cache is bypassed and the hint is used
// directly. This is useful for index-only paths that do not build a full
// `ReadContext`, but still want to reuse a descriptor that has already been opened
// by the caller. If the hint is absent, the function behaves like the legacy
// helper: consult the descriptor cache and open the file on a miss.
#[warn(clippy::too_many_arguments)]
pub fn load_block_with_fd_hint(
    table_id: GlobalTableId,
    path: &Path,
    descriptor_table: &DescriptorTable,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    fd_hint: Option<&Arc<std::fs::File>>,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use std::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?} (fd_hint)");

    if let Some(block) = cache.get_block(table_id, handle.offset()) {
        #[cfg(feature = "metrics")]
        match block_type {
            BlockType::Filter => {
                metrics.filter_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Index => {
                metrics.index_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Data => {
                metrics.data_block_load_cached.fetch_add(1, Relaxed);
            }
            _ => {}
        };
        return Ok(block);
    }

    let cached_fd = if fd_hint.is_some() {
        None
    } else {
        descriptor_table.access_for_table(&table_id)
    };
    let fd_cache_miss = cached_fd.is_none() && fd_hint.is_none();

    let fd: Arc<std::fs::File> = if let Some(fd) = fd_hint.cloned() {
        fd
    } else if let Some(fd) = cached_fd {
        #[cfg(feature = "metrics")]
        metrics.table_file_opened_cached.fetch_add(1, Relaxed);
        fd
    } else {
        let fd = std::fs::File::open(path)?;
        #[cfg(feature = "metrics")]
        metrics.table_file_opened.fetch_add(1, Relaxed);
        Arc::new(fd)
    };

    let block = Block::from_file(&fd, *handle, compression)?;

    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    #[cfg(feature = "metrics")]
    match block_type {
        BlockType::Filter => {
            metrics.filter_block_load_io.fetch_add(1, Relaxed);
        }
        BlockType::Index => {
            metrics.index_block_load_io.fetch_add(1, Relaxed);
        }
        BlockType::Data => {
            metrics.data_block_load_io.fetch_add(1, Relaxed);
        }
        _ => {}
    };

    if fd_cache_miss {
        descriptor_table.insert_for_table(table_id, fd);
    }

    cache.insert_block(table_id, handle.offset(), block.clone());
    Ok(block)
}

#[must_use]
pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

// TODO: Fuzz test
#[must_use]
pub fn compare_prefixed_slice(prefix: &[u8], suffix: &[u8], needle: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();

        return if combined_len > 0 { Greater } else { Equal };
    }

    let max_pfx_len = prefix.len().min(needle.len());

    {
        // SAFETY: We checked for max_pfx_len
        #[expect(unsafe_code, reason = "see safety")]
        let prefix = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        // SAFETY: We checked for max_pfx_len
        #[expect(unsafe_code, reason = "see safety")]
        let needle = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match prefix.cmp(needle) {
            Equal => {}
            ordering => return ordering,
        }
    }

    let rest_len = needle.len() - max_pfx_len;
    if rest_len == 0 {
        if !suffix.is_empty() {
            return std::cmp::Ordering::Greater;
        }
        return std::cmp::Ordering::Equal;
    }

    // SAFETY: We know that the prefix is definitely not longer than the needle
    // so we can safely truncate
    #[expect(unsafe_code, reason = "see safety")]
    let needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(needle)
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_compare_prefixed_slice() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        assert_eq!(Equal, compare_prefixed_slice(b"", b"", b""));

        assert_eq!(Greater, compare_prefixed_slice(b"a", b"", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"b", b"a", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"b", b"a"));

        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"y"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"yyyy", b"a", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"yyy", b"b", b"yyyyb"));
    }
}
