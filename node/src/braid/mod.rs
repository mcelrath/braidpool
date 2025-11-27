use crate::bead::{Bead, BeadHash};
use bitcoin::{Target, Work};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::ThreadId;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

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

// Global shared cohorts data
static COHORTS: LazyLock<Arc<RwLock<Vec<Cohort>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Vec::new())));

// Global registry of bead consumers for bead distribution to threads
static THREADS: LazyLock<Arc<Mutex<HashMap<ThreadId, mpsc::Sender<BeadMessage>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

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

// Remove thread-local - each thread manages its own braid instance

#[derive(Clone, Debug, Default)]
pub struct Braid {
    pub beads: Vec<Bead>,
    pub bead_work: BeadWork,
    pub tips: BeadSet,
    pub cohorts: Vec<Cohort>,
    pub orphans: Vec<Bead>,
    pub geneses: BeadSet,
    pub index: HashMap<BeadHash, BeadIdx>,
    pub parents: Relatives,
    pub children: Relatives,
    // Performance optimization caches (public to crate only -- no one else should need them)
    pub ancestor_cache: Relatives,
    pub(crate) descendant_cache: Relatives,
    pub(crate) tail_cache: Vec<BeadSet>,
    pub extend_strategy: ExtendStrategy,
}

impl Braid {
    ///Initializing the Braid object for keeping track of current state of Braid
    pub fn new(beads: impl IntoIterator<Item = Bead>) -> Self {
        let beads: Vec<Bead> = beads.into_iter().collect();
        if beads.is_empty() {
            return Braid::default();
        }

        // Build index mapping from bead hash to bead index
        let index: HashMap<_, _> = beads
            .iter()
            .enumerate()
            .map(|(idx, b)| (b.hash(), idx))
            .collect();

        // Build bead_work map from weak_target in each bead
        let bead_work: BeadWork = beads
            .iter()
            .enumerate()
            .map(|(idx, bead)| {
                let target = Target::from_compact(bead.committed_metadata.weak_target);
                let work = target.to_work();
                (idx, work)
            })
            .collect();

        // Build parents map from bead parent references
        let parents = beads
            .iter()
            .enumerate()
            .map(|(idx, bead)| {
                let parent_set: BeadSet = bead
                    .committed_metadata
                    .parents
                    .iter()
                    .map(|h| index[h])
                    .collect();
                (idx, parent_set)
            })
            .collect();

        // Build children map using reverse of parents
        let children = algorithms::reverse(&parents);
        // Find genesis beads (beads with no parents)
        let geneses = algorithms::geneses(&parents);
        // Find tips using algorithms::tips
        let tips = algorithms::tips(&children);
        // Initialize ancestor cache using algorithms::all_ancestors
        let mut ancestor_cache = Relatives::new();
        // Compute cohorts using algorithms::cohorts, and populate the ancestor_cache
        let cohorts = algorithms::cohorts(&parents, &children, &geneses, &mut ancestor_cache);

        // Init descendant_cache empty (incremental updates in extend)
        let descendant_cache = Relatives::new();

        // Truncate ancestor cache to only contain intra-cohort relationships for performance optimization
        // This must happen AFTER cohort computation to avoid corrupting the algorithm
        //for cohort in &cohorts {
        //    algorithms::truncate_cache_to_cohort(&mut ancestor_cache, cohort);
        //}

        let mut tail_cache = Vec::new();
        for c in &cohorts {
            let sub_parents = algorithms::sub_braid(c, &parents);
            let sub_children = algorithms::reverse(&sub_parents);
            tail_cache.push(algorithms::cohort_tail(c, &sub_parents, &sub_children));
        }

        // Return braid with computed values
        Braid {
            beads,
            index,
            bead_work,
            parents,
            children,
            geneses,
            tips,
            cohorts,
            ancestor_cache,
            descendant_cache,
            tail_cache,
            ..Default::default()
        }
    }

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

    /// Attempts to extend the braid with the given bead.
    /// Returns true if the bead successfully extended the braid, false otherwise.
    pub fn extend(&mut self, bead: &Bead) -> AddBeadStatus {
        // If the braid is empty and bead has no parents, treat as genesis bead
        if self.beads.is_empty() && bead.committed_metadata.parents.is_empty() {
            *self = Braid::new(vec![bead.clone()]);
            return AddBeadStatus::BeadAdded;
        }
        // No parents: bad block i.e. the extend will add beads after the genesis
        //bead is done and the extension of genesis beads to Braid shall be done via Braid::new
        if bead.committed_metadata.parents.is_empty() {
            return AddBeadStatus::InvalidBead;
        }
        // Already seen this bead in the main braid
        let bead_hash = bead.hash();
        if self.index.contains_key(&bead_hash) {
            return AddBeadStatus::DuplicateBead;
        }
        // FIXME we can make this faster by using a HashMap
        // Already seen this bead in the orphans list
        if self.orphans.iter().any(|b| b.hash() == bead_hash) {
            return AddBeadStatus::DuplicateBead;
        }
        // Don't have all parents - check if they exist
        let all_parents_exist = bead
            .committed_metadata
            .parents
            .iter()
            .all(|h| self.index.contains_key(h));

        if !all_parents_exist {
            self.orphans.push(bead.clone());
            return AddBeadStatus::ParentsMissing;
        }

        let parent_indices_set = self.parent_indices(bead);

        // Insert bead into beads vector
        self.beads.push(bead.clone());
        let new_bead_index = self.beads.len() - 1;
        self.index.insert(bead_hash, new_bead_index);

        // Update bead_work for the new bead
        let target = Target::from_compact(bead.committed_metadata.weak_target);
        let work = target.to_work();
        self.bead_work.insert(new_bead_index, work);

        // Update parents and children HashMaps for the new bead
        for &parent_index in &parent_indices_set {
            // Update children mapping for the parent
            self.children
                .entry(parent_index)
                .or_default()
                .insert(new_bead_index);
        }
        self.parents
            .insert(new_bead_index, parent_indices_set.clone());
        self.children.entry(new_bead_index).or_default();

        // Update caches and tips
        for &parent_index in &parent_indices_set {
            self.tips.remove(&parent_index);
        }
        self.tips.insert(new_bead_index);

        match self.extend_strategy {
            ExtendStrategy::Heuristic => {
                // --- O(W) Heuristic Cohort Update ---
                // Logic:
                // 1. Find the range [idx_min, idx_max] of cohorts containing parents.
                // 2. If idx_min < idx_max, merge all cohorts in that range.
                // 3. Identify tail of the (possibly merged) parent cohort.
                // 4. If the new bead's parents include ALL of the tail, it extends the cohort (New Cohort).
                // 5. Otherwise, it merges into that cohort (Merge).

                let mut idx_max = None;
                let mut idx_min = None;
                let mut parents_found_count = 0;
                let total_parents = parent_indices_set.len();

                // Find cohort range of parents. Iterate backwards.
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
                    let covers_tips = self.tail_cache[max]
                        .iter()
                        .all(|t| parent_indices_set.contains(t));

                    // Merge Spanned Cohorts if necessary
                    if min < max {
                        // Merge cohorts (min+1)..=max into min.
                        for i in (min + 1)..=max {
                            let beads_to_merge: Vec<_> = self.cohorts[i].iter().cloned().collect();
                            let target_cohort_idx = min;

                            for &b in &beads_to_merge {
                                let mut ancestors =
                                    self.ancestor_cache.get(&b).cloned().unwrap_or_default();
                                if let Some(parents) = self.parents.get(&b) {
                                    for &p in parents {
                                        if self.cohorts[target_cohort_idx].contains(&p) {
                                            ancestors.insert(p);
                                            if let Some(p_anc) = self.ancestor_cache.get(&p) {
                                                ancestors.extend(p_anc);
                                            }
                                        }
                                    }
                                }
                                self.ancestor_cache.insert(b, ancestors);
                            }
                            self.cohorts[target_cohort_idx].extend(beads_to_merge);
                        }

                        self.cohorts.drain((min + 1)..=max);
                        // max is gone, min is the new head of this group
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

                // Calculate and insert ancestors for the new bead BEFORE inserting it into the cohort
                let mut new_bead_ancestors = HashSet::new();
                let target_cohort = &self.cohorts[insertion_idx];
                for &p in &parent_indices_set {
                    if target_cohort.contains(&p) {
                        new_bead_ancestors.insert(p);
                        if let Some(p_anc) = self.ancestor_cache.get(&p) {
                            new_bead_ancestors.extend(p_anc);
                        }
                    }
                }
                self.ancestor_cache
                    .insert(new_bead_index, new_bead_ancestors);

                self.cohorts[insertion_idx].insert(new_bead_index);

                // If we inserted before the end, we must merge all subsequent cohorts
                if insertion_idx < self.cohorts.len() - 1 {
                    // Similar merge logic as above, merging into insertion_idx
                    for i in (insertion_idx + 1)..self.cohorts.len() {
                        let beads_to_merge: Vec<_> = self.cohorts[i].iter().cloned().collect();
                        let target_cohort_idx = insertion_idx;

                        for &b in &beads_to_merge {
                            let mut ancestors =
                                self.ancestor_cache.get(&b).cloned().unwrap_or_default();
                            if let Some(parents) = self.parents.get(&b) {
                                for &p in parents {
                                    if self.cohorts[target_cohort_idx].contains(&p) {
                                        ancestors.insert(p);
                                        if let Some(p_anc) = self.ancestor_cache.get(&p) {
                                            ancestors.extend(p_anc);
                                        }
                                    }
                                }
                            }
                            self.ancestor_cache.insert(b, ancestors);
                        }
                        self.cohorts[target_cohort_idx].extend(beads_to_merge);
                    }
                    self.cohorts.truncate(insertion_idx + 1);
                }

                // Update tail_cache for affected cohorts
                self.tail_cache.truncate(insertion_idx);
                for cohort in self.cohorts.iter().skip(insertion_idx) {
                    let sub_p = algorithms::sub_braid(cohort, &self.parents);
                    let sub_c = algorithms::reverse(&sub_p);
                    self.tail_cache
                        .push(algorithms::cohort_tail(cohort, &sub_p, &sub_c));
                }
            }
            ExtendStrategy::Cached => {
                // --- Cached Strategy (Incremental Cache + Algo) ---
                // Find the earliest cohort index among all parents.
                let mut parent_indices: HashSet<usize> = parent_indices_set.clone();
                let mut start_cohort_idx = self.cohorts.len();

                if !parent_indices.is_empty() {
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
                            break;
                        }
                    }
                    if !parent_indices.is_empty() {
                        start_cohort_idx = 0;
                    }
                } else {
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

                let initial_cohort =
                    if !self.cohorts.is_empty() && start_cohort_idx < self.cohorts.len() {
                        self.cohorts[start_cohort_idx].clone()
                    } else {
                        algorithms::geneses(&self.parents)
                    };

                // Invalidate cache for beads that will be recomputed
                // FIXME instead we should be recomputing these cache elements
                if start_cohort_idx < self.cohorts.len() {
                    for cohort in self.cohorts.iter().skip(start_cohort_idx + 1) {
                        for &b in cohort {
                            self.ancestor_cache.remove(&b);
                        }
                    }
                }

                // Truncate
                if start_cohort_idx < self.cohorts.len() {
                    self.cohorts.truncate(start_cohort_idx);
                    self.tail_cache.truncate(start_cohort_idx);
                }

                // Recompute
                let new_cohorts = algorithms::cohorts(
                    &self.parents,
                    &self.children,
                    &initial_cohort,
                    &mut self.ancestor_cache,
                );

                self.cohorts.extend(new_cohorts);

                // Update tail cache
                for cohort in self.cohorts.iter().skip(start_cohort_idx) {
                    let sub_p = algorithms::sub_braid(cohort, &self.parents);
                    let sub_c = algorithms::reverse(&sub_p);
                    self.tail_cache
                        .push(algorithms::cohort_tail(cohort, &sub_p, &sub_c));
                }
            }
            ExtendStrategy::NoCache => {
                // --- NoCache Strategy (Clear Cache + Algo) ---
                self.ancestor_cache.clear();
                let geneses = algorithms::geneses(&self.parents);
                let initial_cohort: Cohort = HashSet::new(); // Empty initial cohort triggers genesis use in algo, but explicit is better

                self.cohorts = algorithms::cohorts(
                    &self.parents,
                    &self.children,
                    &geneses, // Using geneses as initial cohort
                    &mut self.ancestor_cache,
                );

                self.tail_cache = self
                    .cohorts
                    .iter()
                    .map(|c| {
                        let sub_p = algorithms::sub_braid(c, &self.parents);
                        let sub_c = algorithms::reverse(&sub_p);
                        algorithms::cohort_tail(c, &sub_p, &sub_c)
                    })
                    .collect();
            }
        }

        self.process_orphans();

        AddBeadStatus::BeadAdded
    }

    /// Process orphan beads to see if any can now be added to the braid
    /// This method checks if all parents of orphan beads are now available
    /// and recursively extends the braid with those beads
    fn process_orphans(&mut self) {
        let mut i = 0;
        while i < self.orphans.len() {
            // Check if all parents are now available for this orphan
            let all_parents_available = self.orphans[i]
                .committed_metadata
                .parents
                .iter()
                .all(|h| self.index.contains_key(h));

            if all_parents_available {
                let orphan_bead = self.orphans.remove(i);
                match self.extend(&orphan_bead) {
                    AddBeadStatus::BeadAdded => {
                        // Recursively process remaining orphans as this addition
                        // might enable more orphans to be processed
                        self.process_orphans();
                        return;
                    }
                    AddBeadStatus::ParentsMissing => {
                        self.orphans.insert(i, orphan_bead);
                        i += 1;
                    }
                    AddBeadStatus::DuplicateBead | AddBeadStatus::InvalidBead => {
                        // Don't re-add, continue to next
                    }
                }
            } else {
                i += 1;
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
