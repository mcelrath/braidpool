use crate::bead::{Bead, BeadHash};
use bitcoin::{Target, Work};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;

pub mod algorithms;

// A type alias which represents an index into Braid::beads
pub type BeadIdx = usize;
// A type representing parents, children, ancestors, or descendants
pub type Relatives = HashMap<BeadIdx, HashSet<BeadIdx>>;
// A type representing a set of beads indexed in Braid::beads
pub type BeadSet = HashSet<BeadIdx>;
// A type representing the work for each bead
pub type BeadWork = HashMap<BeadIdx, Work>;
// A type representing a cohort (a set of beads indexed in Braid::beads)
pub type Cohort = HashSet<BeadIdx>;
// A type alias which represents an index into Braid::cohorts
pub type CohortIdx = usize;

#[derive(Debug, Clone, PartialEq)]
pub enum AddBeadStatus {
    DuplicateBead,
    InvalidBead,
    BeadAdded,
    ParentsMissing,
}

#[derive(Debug, Clone)]
pub enum GenesisCheckStatus {
    GenesisBeadsValid,
    MissingGenesisBead,
    GenesisBeadsCountMismatch,
}

#[derive(Debug, Clone)]
pub enum BeadMessage {
    NewBead { bead: Bead },
    InvalidateBead { beadhash: BeadHash },
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash)]
pub enum ExtendStrategy {
    /// Optimized heuristic approach (default)
    Heuristic,
    /// Original approach but maintaining cache (incremental cache + algorithms::cohorts)
    Cached,
    /// Original unoptimized approach (clear cache + algorithms::cohorts)
    NoCache,
}

impl Default for ExtendStrategy {
    fn default() -> Self {
        ExtendStrategy::Heuristic
    }
}

#[derive(Clone, Debug, Default)]
pub struct Braid {
    pub beads: Vec<Bead>,
    pub bead_work: BeadWork,
    pub tips: BeadSet,
    pub cohorts: Vec<Cohort>,
    pub orphans: VecDeque<Bead>,
    pub geneses: BeadSet,
    pub index: HashMap<BeadHash, BeadIdx>,
    pub parents: Relatives,
    pub children: Relatives,
    // Performance optimization caches (public to crate only -- no one else should need them)
    pub(crate) ancestor_cache: Relatives,
    pub(crate) descendant_cache: Relatives,
    pub(crate) tail_cache: Vec<BeadSet>,
    pub(crate) cohort_map: HashMap<BeadIdx, CohortIdx>,
    pub(crate) orphan_index: HashSet<BeadHash>,
    pub(crate) waiting_orphans: HashMap<BeadHash, Vec<BeadHash>>,
    pub(crate) pending_orphans: HashMap<BeadHash, PendingOrphan>,
    pub extend_strategy: ExtendStrategy,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingOrphan {
    bead: Bead,
    missing: usize,
}

impl Braid {
    // ==================== Construction ====================

    ///Initializing the Braid object for keeping track of current state of Braid
    pub fn new(beads: impl IntoIterator<Item = Bead>) -> Self {
        Self::new_with_strategy(beads, ExtendStrategy::default())
    }

    pub fn new_with_strategy(
        beads: impl IntoIterator<Item = Bead>,
        strategy: ExtendStrategy,
    ) -> Self {
        let mut braid = Braid {
            extend_strategy: strategy,
            ..Default::default()
        };
        for bead in beads {
            let _ = braid.extend(&bead);
        }
        braid.process_orphans();
        braid
    }

    // ==================== Public API ====================

    /// Creates a BeadSet from an iterator over bead hashes.
    /// Panics if any hash is not found in the braid index.
    pub fn indices<I>(&self, bead_hashes: I) -> BeadSet
    where
        I: IntoIterator<Item = BeadHash>,
    {
        bead_hashes.into_iter().map(|b| self.index[&b]).collect()
    }

    /// Gets parent BeadSet for a bead.
    /// Panics if any parent hash is not found in the braid index.
    pub fn parent_indices(&self, bead: &Bead) -> BeadSet {
        bead.committed_metadata
            .parents
            .iter()
            .map(|h| self.index[h])
            .collect()
    }

    /// Rebuild caches (ancestor, descendant, tail, cohort_map) for cohorts starting at `start_idx`.
    fn rebuild_suffix(&mut self, start_idx: CohortIdx) {
        assert!(
            start_idx <= self.cohorts.len(),
            "rebuild_suffix: start_idx {} out of bounds (len={})",
            start_idx,
            self.cohorts.len()
        );

        self.tail_cache.truncate(start_idx);
        self.cohort_map.retain(|_, idx| *idx < start_idx);
        for cohort in self.cohorts.iter().skip(start_idx) {
            for &bead in cohort {
                self.ancestor_cache.remove(&bead);
                self.descendant_cache.remove(&bead);
            }
        }

        for (cohort_idx, cohort) in self.cohorts.iter().enumerate().skip(start_idx) {
            for &bead in cohort {
                self.cohort_map.insert(bead, cohort_idx);
                self.descendant_cache
                    .entry(bead)
                    .or_insert_with(HashSet::new);
            }

            let sub_parents = algorithms::sub_braid(cohort, &self.parents);
            let sub_children = algorithms::reverse(&sub_parents);
            let mut local_ancestors = Relatives::new();
            for &bead in cohort {
                algorithms::all_ancestors(bead, &sub_parents, &mut local_ancestors);
            }

            for (&bead, ancestors) in local_ancestors.iter() {
                self.ancestor_cache.insert(bead, ancestors.clone());
                for &ancestor in ancestors {
                    self.descendant_cache
                        .entry(ancestor)
                        .or_insert_with(HashSet::new)
                        .insert(bead);
                }
            }

            self.tail_cache
                .push(algorithms::cohort_tail(cohort, &sub_parents, &sub_children));
        }
    }

    fn tail_covered_by_parents(&self, cohort_idx: CohortIdx, parent_indices: &BeadSet) -> bool {
        if let Some(tail) = self.tail_cache.get(cohort_idx) {
            if tail.is_empty() {
                return true;
            }
            let tail_len = tail.len();
            let mut matches = 0;
            if parent_indices.len() < tail_len {
                for parent in parent_indices {
                    if tail.contains(parent) {
                        matches += 1;
                        if matches == tail_len {
                            return true;
                        }
                    }
                }
            } else {
                for &tail_idx in tail {
                    if parent_indices.contains(&tail_idx) {
                        matches += 1;
                        if matches == tail_len {
                            return true;
                        }
                    }
                }
            }
            matches == tail_len
        } else {
            false
        }
    }

    fn track_orphan(&mut self, bead: Bead, bead_hash: BeadHash, missing_parents: Vec<BeadHash>) {
        if missing_parents.is_empty() {
            self.orphans.push_back(bead);
            return;
        }
        self.orphan_index.insert(bead_hash);
        self.pending_orphans.insert(
            bead_hash,
            PendingOrphan {
                bead,
                missing: missing_parents.len(),
            },
        );
        for parent_hash in missing_parents {
            self.waiting_orphans
                .entry(parent_hash)
                .or_default()
                .push(bead_hash);
        }
    }

    fn wake_orphans(&mut self, parent_hash: &BeadHash) {
        if let Some(children) = self.waiting_orphans.remove(parent_hash) {
            for child_hash in children {
                if let Some(pending) = self.pending_orphans.get_mut(&child_hash) {
                    if pending.missing > 0 {
                        pending.missing -= 1;
                    }
                    if pending.missing == 0 {
                        if let Some(pending_entry) = self.pending_orphans.remove(&child_hash) {
                            self.orphans.push_back(pending_entry.bead);
                            self.orphan_index.remove(&child_hash);
                        }
                    }
                }
            }
        }
    }

    // ==================== Private: Graph Updates ====================

    /// Helper to determine the earliest cohort index from which recomputation should start
    /// for the Cached strategy, including backtracking logic.
    fn find_recomputation_start_cohort_idx(&self, bead_parents: &BeadSet) -> CohortIdx {
        let mut parent_indices: HashSet<usize> = bead_parents.clone();
        let mut start_cohort_idx = self.cohorts.len();

        if parent_indices.is_empty() {
            return 0; // If no parents, effectively starts from genesis
        }

        // Find the earliest cohort containing a parent
        for (i, cohort) in self.cohorts.iter().enumerate().rev() {
            let found: Vec<usize> = parent_indices
                .iter()
                .copied()
                .filter(|p| cohort.contains(p))
                .collect();
            if !found.is_empty() {
                start_cohort_idx = i;
                for p in found {
                    parent_indices.remove(&p);
                }
            }
            if parent_indices.is_empty() {
                break; // All parents found
            }
        }
        // If some parents were not found in existing cohorts, it means they are very old.
        // Or if initial braid is empty, start from 0.
        if !parent_indices.is_empty() || self.cohorts.is_empty() {
            start_cohort_idx = 0;
        }

        // Backtrack for tip status change
        if start_cohort_idx > 0 {
            start_cohort_idx -= 1;
        }

        // Backtrack for thick cohorts
        while start_cohort_idx > 0 {
            let cohort = &self.cohorts[start_cohort_idx];
            let has_internal_links = cohort.iter().any(|&b| {
                self.parents
                    .get(&b)
                    .map_or(false, |parents| parents.iter().any(|p| cohort.contains(p)))
            });
            if has_internal_links {
                start_cohort_idx -= 1;
            } else {
                break;
            }
        }
        start_cohort_idx
    }

    /// Attempts to extend the braid with the given bead.
    /// Returns true if the bead successfully extended the braid, false otherwise.
    pub fn extend(&mut self, bead: &Bead) -> AddBeadStatus {
        let bead_hash = bead.hash();
        if self.index.contains_key(&bead_hash) {
            return AddBeadStatus::DuplicateBead;
        }
        if self.orphan_index.contains(&bead_hash) || self.pending_orphans.contains_key(&bead_hash) {
            return AddBeadStatus::DuplicateBead;
        }

        let missing_parents: Vec<_> = bead
            .committed_metadata
            .parents
            .iter()
            .filter(|&&h| !self.index.contains_key(&h))
            .copied()
            .collect();
        if !missing_parents.is_empty() {
            self.track_orphan(bead.clone(), bead_hash, missing_parents);
            return AddBeadStatus::ParentsMissing;
        }

        let bead_parents = self.parent_indices(bead);

        // Insert bead into storage
        self.beads.push(bead.clone());
        let new_bead_index = self.beads.len() - 1;
        self.index.insert(bead_hash, new_bead_index);
        self.bead_work.insert(
            new_bead_index,
            Target::from_compact(bead.committed_metadata.weak_target).to_work(),
        );

        for &parent_index in &bead_parents {
            self.children
                .entry(parent_index)
                .or_default()
                .insert(new_bead_index);
        }
        self.parents.insert(new_bead_index, bead_parents.clone());
        self.children.entry(new_bead_index).or_default();

        for &parent_index in &bead_parents {
            self.tips.remove(&parent_index);
        }
        self.tips.insert(new_bead_index);
        if bead_parents.is_empty() {
            self.geneses.insert(new_bead_index);
        }

        self.wake_orphans(&bead_hash);

        // --- Strategy-Specific Cohort Updates ---
        match self.extend_strategy {
            ExtendStrategy::Heuristic => {
                // --- O(W) Heuristic Cohort Update ---
                // Logic:
                // 1. Find the range [idx_min, idx_max] of cohorts containing parents.
                // 2. If idx_min < idx_max, merge all cohorts in that range.
                // 3. Identify tail of the (possibly merged) parent cohort.
                // 4. If the new bead's parents include ALL of the tail, it extends the cohort (New Cohort).
                // 5. Otherwise, it merges into that cohort (Merge).

                let parent_indices_set = &bead_parents;

                if parent_indices_set.is_empty() {
                    let new_idx = self.cohorts.len();
                    let mut cohort = Cohort::new();
                    cohort.insert(new_bead_index);
                    self.cohorts.push(cohort);
                    self.rebuild_suffix(new_idx);
                    self.process_orphans();
                    return AddBeadStatus::BeadAdded;
                }

                let mut idx_max = None;
                let mut idx_min = None;
                let mut parents_found_count = 0;
                let total_parents = parent_indices_set.len();

                for (i, cohort) in self.cohorts.iter().enumerate().rev() {
                    let count_in_cohort = parent_indices_set
                        .iter()
                        .filter(|&p| cohort.contains(p))
                        .count();
                    if count_in_cohort > 0 {
                        if idx_max.is_none() {
                            idx_max = Some(i);
                        }
                        idx_min = Some(i);
                        parents_found_count += count_in_cohort;

                        if parents_found_count == total_parents {
                            break;
                        }
                    }
                }

                let insertion_idx = if let (Some(max), Some(min)) = (idx_max, idx_min) {
                    // Check if parents cover all internal tips of the latest parent cohort.
                    // Use the cached tail (which represents internal tips).
                    // If we span multiple cohorts (min < max), the effective tail of the merged group
                    // is the tail of the latest cohort (max).
                    const DENSE_TAIL_LIMIT: usize = 256;
                    let tail_len = self.tail_cache[max].len();
                    let covers_tips = if tail_len > DENSE_TAIL_LIMIT {
                        false
                    } else {
                        self.tail_covered_by_parents(max, parent_indices_set)
                    };

                    // Merge Spanned Cohorts if necessary
                    if min < max {
                        for i in (min + 1)..=max {
                            let merged = mem::take(&mut self.cohorts[i]);
                            self.cohorts[min].extend(merged);
                        }

                        self.cohorts.drain((min + 1)..=max);
                    }

                    // If we cover tips, we extend (min + 1). Else we merge into min.
                    if covers_tips {
                        min + 1
                    } else {
                        min
                    }
                } else {
                    0
                };

                // Apply changes
                if insertion_idx == self.cohorts.len() {
                    self.cohorts.push(HashSet::new());
                }

                self.cohorts[insertion_idx].insert(new_bead_index);

                if insertion_idx < self.cohorts.len() - 1 {
                    for i in (insertion_idx + 1)..self.cohorts.len() {
                        let beads_to_merge: Vec<_> = self.cohorts[i].iter().copied().collect();
                        self.cohorts[insertion_idx].extend(beads_to_merge);
                    }
                    self.cohorts.truncate(insertion_idx + 1);
                }

                let rebuild_start = idx_min.unwrap_or(insertion_idx);
                self.rebuild_suffix(rebuild_start);
            }
            ExtendStrategy::Cached => {
                let start_cohort_idx = self.find_recomputation_start_cohort_idx(&bead_parents);

                let initial_cohort = self
                    .cohorts
                    .get(start_cohort_idx)
                    .cloned()
                    .unwrap_or_else(|| algorithms::geneses(&self.parents));

                if start_cohort_idx < self.cohorts.len() {
                    self.cohorts.truncate(start_cohort_idx);
                }

                let mut scratch = Relatives::new();
                let new_cohorts = algorithms::cohorts(
                    &self.parents,
                    &self.children,
                    &initial_cohort,
                    &mut scratch,
                );

                self.cohorts.extend(new_cohorts);
                self.rebuild_suffix(start_cohort_idx);
            }
            ExtendStrategy::NoCache => {
                let geneses = algorithms::geneses(&self.parents);
                let mut scratch = Relatives::new();

                self.cohorts =
                    algorithms::cohorts(&self.parents, &self.children, &geneses, &mut scratch);

                self.ancestor_cache.clear();
                self.descendant_cache.clear();
                self.tail_cache.clear();
                self.cohort_map.clear();
                self.rebuild_suffix(0);
            }
        }

        self.process_orphans();

        AddBeadStatus::BeadAdded
    }

    /// Process orphan beads to see if any can now be added to the braid
    /// This method checks if all parents of orphan beads are now available
    /// and recursively extends the braid with those beads
    /// FIXME this algorithm is O(n^2)
    fn process_orphans(&mut self) {
        while let Some(orphan_bead) = self.orphans.pop_front() {
            match self.extend(&orphan_bead) {
                AddBeadStatus::BeadAdded => {}
                AddBeadStatus::ParentsMissing => {
                    let missing = orphan_bead
                        .committed_metadata
                        .parents
                        .iter()
                        .filter(|&&h| !self.index.contains_key(&h))
                        .copied()
                        .collect::<Vec<_>>();
                    self.track_orphan(orphan_bead.clone(), orphan_bead.hash(), missing);
                }
                AddBeadStatus::DuplicateBead | AddBeadStatus::InvalidBead => {
                    // Ignore duplicates from reprocessing
                }
            }
        }
    }

    // FIXME What is this for? Is it just overly defensive?
    pub fn check_geneses(&self, geneses: &[BeadHash]) -> GenesisCheckStatus {
        if geneses.len() != self.geneses.len() {
            return GenesisCheckStatus::GenesisBeadsCountMismatch;
        }
        let all_exist = geneses.iter().all(|h| {
            self.index
                .get(h)
                .map_or(false, |idx| self.geneses.contains(idx))
        });
        if all_exist {
            GenesisCheckStatus::GenesisBeadsValid
        } else {
            GenesisCheckStatus::MissingGenesisBead
        }
    }

    pub fn insert_geneses(&mut self, geneses: Vec<Bead>) {
        for bead in geneses {
            let bead_hash = bead.hash();
            if !self.index.contains_key(&bead_hash) {
                self.beads.push(bead.clone());
                let new_index = self.beads.len() - 1;
                self.index.insert(bead_hash, new_index);
                self.geneses.insert(new_index);
            }
        }
    }
}

#[cfg(test)]
mod algorithm_tests;
#[cfg(test)]
mod tests;
