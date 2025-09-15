use lsm_tree::{
    prefix::{FixedLengthExtractor, FixedPrefixExtractor, FullKeyExtractor},
    AbstractTree, Config, Memtable, SeqNo,
};
use std::sync::Arc;

#[test]
fn contains_prefix_basic_no_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    tree.insert("a", "1", 1);
    tree.insert("aa", "2", 2);
    tree.insert("b", "3", 3);
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("a", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("aaa", SeqNo::MAX, None)?);
    assert!(tree.contains_prefix("b", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("c", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_empty_prefix_with_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .open()?;

    // Empty tree => empty prefix should be false
    assert!(!tree.contains_prefix("", SeqNo::MAX, None)?);

    tree.insert("k1", "v", 1);
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_absent_many_segments_no_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    // Create many small segments with disjoint prefixes
    for i in 0..10 {
        for j in 0..10 {
            tree.insert(format!("p{}_{:03}", i, j), "v", 1);
        }
        tree.flush_active_memtable(0)?;
    }

    // Query a clearly absent prefix
    assert!(!tree.contains_prefix("zzz_", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_absent_many_segments_with_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .open()?;

    for i in 0..10 {
        for j in 0..10 {
            tree.insert(format!("pre{}_{:03}", i, j), "v", 1);
        }
        tree.flush_active_memtable(0)?;
    }

    // Query an absent 3-char prefix
    assert!(!tree.contains_prefix("zzz", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_tombstone_across_runs_no_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    // Value in older run
    tree.insert("foo_a", "v", 1);
    tree.flush_active_memtable(0)?;

    // Tombstone in newer run
    tree.remove("foo_a", 2);
    tree.flush_active_memtable(0)?;

    // Deleted => no prefix should exist
    assert!(!tree.contains_prefix("foo_", 3, None)?);

    // Reinsert later => becomes visible
    tree.insert("foo_b", "v", 4);
    tree.flush_active_memtable(0)?;
    assert!(tree.contains_prefix("foo_", 5, None)?);

    Ok(())
}

#[test]
fn contains_prefix_tombstone_across_runs_with_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .open()?;

    // Value in older run
    tree.insert("bar_a", "v", 1);
    tree.flush_active_memtable(0)?;

    // Tombstone in newer run
    tree.remove("bar_a", 2);
    tree.flush_active_memtable(0)?;

    // Deleted => no prefix should exist
    assert!(!tree.contains_prefix("bar", 3, None)?);

    // Reinsert later => becomes visible
    tree.insert("bar_b", "v", 4);
    tree.flush_active_memtable(0)?;
    assert!(tree.contains_prefix("bar", 5, None)?);

    Ok(())
}

#[test]
fn contains_prefix_ephemeral_memtable_with_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .open()?;

    // Overlay only
    let overlay = Arc::new(Memtable::default());
    overlay.insert(lsm_tree::InternalValue::from_components(
        b"usr_001",
        b"v",
        10,
        lsm_tree::ValueType::Value,
    ));

    assert!(tree.contains_prefix("usr", SeqNo::MAX, Some(overlay))?);

    Ok(())
}

#[test]
fn contains_prefix_ephemeral_memtable() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    // No data in tree; use ephemeral memtable overlay
    let overlay = Arc::new(Memtable::default());
    overlay.insert(lsm_tree::InternalValue::from_components(
        b"user_001",
        b"v",
        10,
        lsm_tree::ValueType::Value,
    ));

    assert!(tree.contains_prefix("user_", SeqNo::MAX, Some(overlay))?);

    Ok(())
}

#[test]
fn contains_prefix_with_prefix_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
        .open()?;

    for i in 0..50u32 {
        tree.insert(format!("pre{:03}", i), "v", 1);
    }
    for i in 0..50u32 {
        tree.insert(format!("zzz{:03}", i), "v", 1);
    }
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("pre", SeqNo::MAX, None)?);
    assert!(tree.contains_prefix("zzz", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("abc", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_mvcc_threshold() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    // Key becomes visible only after seqno > 100
    tree.insert("pre_key", "v", 100);
    tree.flush_active_memtable(0)?;

    assert!(
        !tree.contains_prefix("pre", 100, None)?,
        "at seqno=100 value is not visible"
    );
    assert!(
        tree.contains_prefix("pre", 101, None)?,
        "at seqno=101 value is visible"
    );

    Ok(())
}

#[test]
fn contains_prefix_with_tombstones() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    tree.insert("pre_a", "v", 1);
    tree.insert("pre_b", "v", 2);
    tree.flush_active_memtable(0)?;

    // Delete keys at seqno 3
    tree.remove("pre_a", 3);
    tree.remove("pre_b", 3);
    tree.flush_active_memtable(0)?;

    // At seqno 4, both keys are deleted -> contains_prefix should be false
    assert!(!tree.contains_prefix("pre_", 4, None)?);

    // Insert new key later -> becomes visible
    tree.insert("pre_c", "v", 5);
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("pre_", 6, None)?);

    Ok(())
}

#[test]
fn contains_prefix_out_of_domain_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    // Require length >= 5; short prefixes are out-of-domain for filter
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FixedLengthExtractor::new(5)))
        .open()?;

    tree.insert("abcde_key", "v", 1);
    tree.insert("fghij_key", "v", 1);
    tree.flush_active_memtable(0)?;

    // Short prefix still works functionally
    assert!(tree.contains_prefix("ab", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("zz", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_full_key_extractor() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir)
        .prefix_extractor(Arc::new(FullKeyExtractor))
        .open()?;

    tree.insert("alpha", "v", 1);
    tree.insert("alphabet", "v", 1);
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("alpha", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("beta", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn contains_prefix_empty_prefix_behavior() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(&dir).open()?;

    // Empty tree => empty prefix should be false
    assert!(!tree.contains_prefix("", SeqNo::MAX, None)?);

    tree.insert("k1", "v", 1);
    assert!(tree.contains_prefix("", SeqNo::MAX, None)?);

    Ok(())
}
