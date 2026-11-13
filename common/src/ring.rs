//! Consistent hash ring used to choose which workers store a chunk.
//!
//! The ring holds every *registered* worker. Liveness is not tracked here:
//! callers pass an `eligible` predicate (e.g. "alive and not already holding
//! this chunk") so the same ring serves first placement and re-replication.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::models::WorkerId;

const DEFAULT_VNODES: usize = 100;

#[derive(Debug, Clone)]
pub struct HashRing {
    vnodes: usize,
    ring: BTreeMap<u64, WorkerId>,
    nodes: BTreeSet<WorkerId>,
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new(DEFAULT_VNODES)
    }
}

fn hash64(key: &str) -> u64 {
    let d = Sha256::digest(key.as_bytes());
    u64::from_be_bytes(d[..8].try_into().unwrap())
}

impl HashRing {
    pub fn new(vnodes: usize) -> Self {
        Self {
            vnodes: vnodes.max(1),
            ring: BTreeMap::new(),
            nodes: BTreeSet::new(),
        }
    }

    pub fn add_node(&mut self, id: &str) {
        if !self.nodes.insert(id.to_string()) {
            return; // already present
        }
        for v in 0..self.vnodes {
            self.ring.insert(hash64(&format!("{id}#{v}")), id.to_string());
        }
    }

    pub fn remove_node(&mut self, id: &str) {
        if self.nodes.remove(id) {
            self.ring.retain(|_, w| w != id);
        }
    }

    pub fn contains(&self, id: &str) -> bool {
        self.nodes.contains(id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &WorkerId> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Walks clockwise from `hash(key)` and returns up to `n` *distinct*
    /// workers for which `eligible(worker)` is true.
    ///
    /// - First placement: `pick_targets(chunk_id, 2, |w| alive(w))`
    /// - Re-replication:  `pick_targets(chunk_id, 1, |w| alive(w) && !holders.contains(w))`
    ///
    /// Returns fewer than `n` if not enough eligible workers exist.
    pub fn pick_targets<F>(&self, key: &str, n: usize, eligible: F) -> Vec<WorkerId>
    where
        F: Fn(&str) -> bool,
    {
        let mut out: Vec<WorkerId> = Vec::with_capacity(n);
        if n == 0 || self.ring.is_empty() {
            return out;
        }
        let start = hash64(key);
        let walk = self.ring.range(start..).chain(self.ring.range(..start));
        for (_, w) in walk {
            if out.len() == n {
                break;
            }
            if !out.contains(w) && eligible(w) {
                out.push(w.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ring4() -> HashRing {
        let mut r = HashRing::default();
        for w in ["worker1", "worker2", "worker3", "worker4"] {
            r.add_node(w);
        }
        r
    }

    #[test]
    fn picks_distinct_workers() {
        let r = ring4();
        for i in 0..500 {
            let t = r.pick_targets(&format!("chunk{i}"), 2, |_| true);
            assert_eq!(t.len(), 2);
            assert_ne!(t[0], t[1]);
        }
    }

    #[test]
    fn deterministic() {
        let r = ring4();
        assert_eq!(
            r.pick_targets("abc", 2, |_| true),
            r.pick_targets("abc", 2, |_| true)
        );
    }

    #[test]
    fn skips_ineligible() {
        let r = ring4();
        for i in 0..200 {
            let t = r.pick_targets(&format!("c{i}"), 2, |w| w != "worker2");
            assert!(!t.contains(&"worker2".to_string()));
            assert_eq!(t.len(), 2);
        }
    }

    #[test]
    fn returns_fewer_when_not_enough() {
        let r = ring4();
        let t = r.pick_targets("x", 2, |w| w == "worker3");
        assert_eq!(t, vec!["worker3".to_string()]);
        assert!(HashRing::default().pick_targets("x", 2, |_| true).is_empty());
    }

    #[test]
    fn roughly_balanced() {
        let r = ring4();
        let mut counts: HashMap<String, usize> = HashMap::new();
        for i in 0..4000 {
            for w in r.pick_targets(&format!("chunk{i}"), 2, |_| true) {
                *counts.entry(w).or_default() += 1;
            }
        }
        // 8000 placements / 4 workers = 2000 each; allow generous slack.
        for (_, c) in counts {
            assert!((1400..2600).contains(&c), "unbalanced: {c}");
        }
    }

    #[test]
    fn remove_node_works() {
        let mut r = ring4();
        r.remove_node("worker1");
        assert!(!r.contains("worker1"));
        assert_eq!(r.len(), 3);
        for i in 0..100 {
            assert!(!r
                .pick_targets(&format!("c{i}"), 3, |_| true)
                .contains(&"worker1".to_string()));
        }
    }
}