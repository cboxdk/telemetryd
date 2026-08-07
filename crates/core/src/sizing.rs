//! What a buffered record actually costs in memory.
//!
//! This exists because the previous estimate was wrong by roughly an order of
//! magnitude, and it was wrong in the direction that matters: `max_segment_bytes` is
//! the knob that decides when a segment seals, so under-counting means the buffer
//! grows far past the size the operator configured. A 256 MiB setting was measured
//! holding 6.4 GB resident.
//!
//! It counted the bytes of every string and stopped there. What it missed:
//!
//! - **`BTreeMap` allocates a node, not an entry.** A map holding two labels still
//!   allocates a full leaf node with room for eleven — several hundred bytes. Every
//!   record carries two such maps, so this alone was the dominant term and was
//!   counted as zero.
//! - **`String` is 24 bytes of struct** before any characters, and a record has
//!   several.
//! - **Every allocation carries allocator bookkeeping**, and a record made of many
//!   small strings is mostly bookkeeping.
//!
//! The goal here is not exactness — that is not knowable portably, and chasing it
//! would be false precision. The goal is to stop being *systematically* low, so that
//! "256 MiB" means something within a factor of two rather than a factor of ten.
//! Erring slightly high is the safe direction: it seals a little early, which costs a
//! marginally smaller segment and never an unexpected multi-gigabyte process.

use std::collections::BTreeMap;
use std::mem::size_of;

use crate::record::Labels;

/// Per-allocation allocator bookkeeping.
///
/// Size class rounding and header overhead vary by allocator; this is a middling
/// figure for the small allocations records are made of. It matters because a record
/// is typically a dozen small allocations rather than one big one.
const ALLOCATION_OVERHEAD: usize = 16;

/// How many entries fit in one `BTreeMap` leaf node.
///
/// `std`'s B-tree uses B = 6, so a node holds up to 11 key-value pairs. A map with a
/// single entry pays for the whole node.
const BTREE_NODE_CAPACITY: usize = 11;

/// Heap cost of one `BTreeMap` node holding `String` keys and values.
///
/// The node stores its keys and values inline, so this is dominated by
/// `11 * (24 + 24)` — the `String` structs — plus edges and length bookkeeping.
const fn btree_node_bytes() -> usize {
    BTREE_NODE_CAPACITY * (size_of::<String>() + size_of::<String>())
        + size_of::<usize>() * 4
        + ALLOCATION_OVERHEAD
}

/// Bytes a string's *contents* occupy on the heap.
///
/// The 24-byte `String` struct itself is not counted here — it lives inside whatever
/// owns it, which is counted separately by `size_of` on the owning type.
#[must_use]
pub fn string_bytes(value: &str) -> usize {
    if value.is_empty() {
        // An empty `String` does not allocate.
        0
    } else {
        value.len() + ALLOCATION_OVERHEAD
    }
}

/// Bytes an optional string costs.
#[must_use]
pub fn optional_string_bytes(value: Option<&String>) -> usize {
    value.map_or(0, |s| string_bytes(s))
}

/// Bytes a label set occupies: its nodes, plus the contents of every key and value.
#[must_use]
pub fn labels_bytes(labels: &Labels) -> usize {
    map_bytes(labels.len(), labels.iter().map(|(k, v)| k.len() + v.len()))
}

/// The same accounting for a bare map, for types that do not use [`Labels`].
#[must_use]
pub fn btree_bytes(map: &BTreeMap<String, String>) -> usize {
    map_bytes(map.len(), map.iter().map(|(k, v)| k.len() + v.len()))
}

fn map_bytes(entries: usize, contents: impl Iterator<Item = usize>) -> usize {
    if entries == 0 {
        // An empty `BTreeMap` has no root node allocated.
        return 0;
    }
    // Round up: a map with one entry still pays for a whole node, which is the case
    // the old estimate got most wrong.
    let nodes = entries.div_ceil(BTREE_NODE_CAPACITY);
    nodes * btree_node_bytes()
        + contents
            .map(|len| {
                // Keys and values are separate allocations.
                len + 2 * ALLOCATION_OVERHEAD
            })
            .sum::<usize>()
}

/// Bytes a `Vec` of `T` costs beyond the `Vec` struct, ignoring spare capacity.
#[must_use]
pub fn vec_bytes<T>(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        len * size_of::<T>() + ALLOCATION_OVERHEAD
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_string_costs_nothing() {
        assert_eq!(string_bytes(""), 0);
        assert!(string_bytes("x") > 0);
    }

    #[test]
    fn an_empty_map_costs_nothing_but_one_entry_costs_a_whole_node() {
        let empty = Labels::new();
        assert_eq!(labels_bytes(&empty), 0);

        let mut one = Labels::new();
        one.insert("a", "b");
        // The point of the whole module: a two-character map is not two bytes.
        assert!(
            labels_bytes(&one) > 300,
            "one entry should pay for a node, got {}",
            labels_bytes(&one)
        );
    }

    #[test]
    fn a_second_node_is_only_paid_for_past_the_first_nodes_capacity() {
        let mut small = Labels::new();
        for i in 0..BTREE_NODE_CAPACITY {
            small.insert(format!("k{i}"), "v");
        }
        let mut large = small.clone();
        large.insert("one-more", "v");

        // Crossing the boundary adds a node, not just an entry.
        let step = labels_bytes(&large) - labels_bytes(&small);
        assert!(step > btree_node_bytes(), "expected a new node, got {step}");
    }

    #[test]
    fn the_estimate_is_not_the_old_string_length_sum() {
        // The regression this module exists to prevent. A realistic log record's
        // labels: two short pairs. The old estimate called this ~30 bytes.
        let mut labels = Labels::new();
        labels.insert("app", "checkout");
        labels.insert("level", "info");
        let old_style: usize = labels.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
        assert!(
            labels_bytes(&labels) > old_style * 8,
            "old {old_style}, new {}",
            labels_bytes(&labels)
        );
    }
}
