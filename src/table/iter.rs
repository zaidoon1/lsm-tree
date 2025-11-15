// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{data_block::Iter as DataBlockIter, BlockOffset, DataBlock, GlobalTableId};
use crate::{
    table::{
        block::ParsedItem,
        block_index::{BlockIndexIter, BlockIndexIterImpl},
        util::{load_block_with_ctx, ReadContext},
        BlockHandle,
    },
    Cache, CompressionType, DescriptorTable, InternalValue, SeqNo, UserKey,
};
use self_cell::self_cell;
use std::{fs::File, path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

type InnerIter<'a> = DataBlockIter<'a>;

pub enum Bound {
    Included(UserKey),
    Excluded(UserKey),
}
type Bounds = (Option<Bound>, Option<Bound>);

self_cell!(
    pub struct OwnedDataBlockIter {
        owner: DataBlock,

        #[covariant]
        dependent: InnerIter,
    }
);

impl OwnedDataBlockIter {
    fn seek_lower_inclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle /* TODO: , seqno */))
    }

    fn seek_upper_inclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle /* TODO: , seqno */))
    }

    fn seek_lower_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_exclusive(needle /* TODO: , seqno */))
    }

    fn seek_upper_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper_exclusive(needle /* TODO: , seqno */))
    }

    pub fn seek_lower_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_lower_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_lower_exclusive(key, seqno),
        }
    }

    pub fn seek_upper_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_upper_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_upper_exclusive(key, seqno),
        }
    }
}

impl Iterator for OwnedDataBlockIter {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedDataBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

fn create_data_block_reader(block: DataBlock) -> OwnedDataBlockIter {
    OwnedDataBlockIter::new(block, super::data_block::DataBlock::iter)
}

pub struct Iter {
    table_id: GlobalTableId,
    path: Arc<PathBuf>,

    #[expect(clippy::struct_field_names)]
    index_iter: BlockIndexIterImpl,

    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    index_initialized: bool,

    lo_offset: BlockOffset,
    lo_data_block: Option<OwnedDataBlockIter>,

    hi_offset: BlockOffset,
    hi_data_block: Option<OwnedDataBlockIter>,

    range: Bounds,

    // Context shared for this iterator so all index and data block loads reuse the
    // same descriptor for this table. This keeps descriptor-table lookups and
    // file opens to a minimum in long range scans.
    ctx: ReadContext,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    pub fn new(
        table_id: GlobalTableId,
        path: Arc<PathBuf>,
        index_iter: BlockIndexIterImpl,
        descriptor_table: Arc<DescriptorTable>,
        cache: Arc<Cache>,
        compression: CompressionType,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            table_id,
            path,

            index_iter,
            descriptor_table,
            cache,
            compression,

            index_initialized: false,

            lo_offset: BlockOffset(0),
            lo_data_block: None,

            hi_offset: BlockOffset(u64::MAX),
            hi_data_block: None,

            range: (None, None),

            ctx: ReadContext::default(),

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    pub fn set_lower_bound(&mut self, bound: Bound) {
        self.range.0 = Some(bound);
    }

    pub fn set_upper_bound(&mut self, bound: Bound) {
        self.range.1 = Some(bound);
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        // Always try to keep iterating inside the already-materialized low data block first; this
        // lets callers consume multiple entries without touching the index or cache again.
        if let Some(block) = &mut self.lo_data_block {
            if let Some(item) = block.next().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // Lazily initialize the index iterator here (not in `new`) so callers can set bounds
            // before any seek or I/O cost is incurred. Bounds exclusivity is enforced at the
            // data-block level; index seeks only narrow the span of blocks that need to be
            // inspected.
            if self.ctx.fd.is_none() {
                // Seed the context with a descriptor so both index traversal and data-block
                // materialization share the same `Arc<File>` for this table during the scan.
                if let Some(fd) = self.descriptor_table.access_for_table(&self.table_id) {
                    self.ctx.fd = Some(fd);
                } else {
                    let fd = fail_iter!(File::open(&*self.path).map(Arc::new));
                    self.descriptor_table
                        .insert_for_table(self.table_id, fd.clone());
                    self.ctx.fd = Some(fd);
                }
            }

            // Pass the descriptor down into the block-index iterator so index-block loads for
            // this scan can bypass the descriptor cache and reuse the same `Arc<File>`.
            self.index_iter.set_fd_hint(self.ctx.fd.clone());
            let mut ok = true;

            if let Some(bound) = &self.range.0 {
                // Seek to the first block whose end key is ≥ lower bound.
                // If this fails we can immediately conclude the range is empty.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                // Apply an upper-bound seek to cap the block span, but keep exact high-key
                // handling inside the data block so exclusivity is respected precisely.
                if let Some(bound) = &self.range.1 {
                    let key = match bound {
                        Bound::Included(k) | Bound::Excluded(k) => k,
                    };
                    ok = self.index_iter.seek_upper(key);
                }
            }

            self.index_initialized = true;

            if !ok {
                // No block in the index overlaps the requested window, so we clear state and
                // return EOF without touching any data blocks.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next() else {
                // No more block handles coming from the index. Flush any pending items buffered on
                // the high side (used by reverse iteration) before signalling completion.
                if let Some(block) = &mut self.hi_data_block {
                    if let Some(item) = block.next().map(Ok) {
                        return Some(item);
                    }
                }

                // Nothing left to serve; drop both buffers so the iterator can be reused safely.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = fail_iter!(handle);

            // Load the next data block referenced by the index handle. Use the shared block
            // cache when possible to avoid hitting the filesystem, and on a miss go through the
            // ctx-aware loader so the underlying descriptor stays shared with index-block loads.
            #[expect(clippy::single_match_else)]
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block_with_ctx(
                        self.table_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::table::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                        &mut self.ctx,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            // Forward path: seek the low side first to avoid returning entries below the lower
            // bound, then clamp the iterator on the high side. This keeps iteration inside
            // [low, high] with precise control over inclusivity/exclusivity.
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(item) = item {
                // Serve the first item immediately to match the simple path at the top and avoid
                // stashing it in an extra buffer.
                return Some(Ok(item));
            }
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Mirror the forward iterator: prefer consuming buffered items from the high data block to
        // avoid touching the index once a block has been materialized.
        if let Some(block) = &mut self.hi_data_block {
            if let Some(item) = block.next_back().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // Mirror forward iteration: initialize lazily so bounds can be applied up-front. The
            // index only restricts which blocks are considered; strict bound handling still
            // happens inside the data-block readers.
            if self.ctx.fd.is_none() {
                // Establish the shared descriptor exactly once so both index traversal and
                // backward data-block reads reuse the same `Arc<File>`.
                if let Some(fd) = self.descriptor_table.access_for_table(&self.table_id) {
                    self.ctx.fd = Some(fd);
                } else {
                    let fd = fail_iter!(File::open(&*self.path).map(Arc::new));
                    self.descriptor_table
                        .insert_for_table(self.table_id, fd.clone());
                    self.ctx.fd = Some(fd);
                }
            }

            self.index_iter.set_fd_hint(self.ctx.fd.clone());
            let mut ok = true;

            if let Some(bound) = &self.range.0 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                if let Some(bound) = &self.range.1 {
                    let key = match bound {
                        Bound::Included(k) | Bound::Excluded(k) => k,
                    };
                    ok = self.index_iter.seek_upper(key);
                }
            }

            self.index_initialized = true;

            if !ok {
                // No index span overlaps the requested window; clear both buffers and finish
                // early without touching any data blocks.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next_back() else {
                // Once the index is exhausted in reverse order, flush any items that are buffered
                // on the low side (for callers that walked forward first) before completing.
                if let Some(block) = &mut self.lo_data_block {
                    if let Some(item) = block.next_back().map(Ok) {
                        return Some(item);
                    }
                }

                // Nothing left to produce; reset both buffers to keep the iterator reusable.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = fail_iter!(handle);

            // Retrieve the next data block from the cache (or disk on miss) so the high-side
            // reader can serve entries in reverse order. Use the ctx-aware loader on cache miss
            // so reverse scans share the same table descriptor as forward scans.
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block_with_ctx(
                        self.table_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::table::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                        &mut self.ctx,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(item) = item {
                return Some(Ok(item));
            }
        }
    }
}
