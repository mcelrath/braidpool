use crate::bead::{Bead, BeadHash};
use bitcoin::{Target, Work};
use std::collections::{HashMap, HashSet};

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
    pub orphans: Vec<Bead>,
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
    pub extend_strategy: ExtendStrategy,
}

impl Braid {
    // ==================== Construction ====================

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
                (
                    idx,
                    Target::from_compact(bead.committed_metadata.weak_target).to_work(),
                )
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
        println!("Parents map in Braid::new(): {:?}", parents);

        // Build children map using reverse of parents
        let children = algorithms::reverse(&parents);
        println!("Children map in Braid::new(): {:?}", children);
        // Find genesis beads (beads with no parents)
        let geneses = algorithms::geneses(&parents);
        // Find tips using algorithms::tips
        let tips = algorithms::tips(&children);
        // Initialize ancestor cache using algorithms::all_ancestors
        //let mut ancestor_cache = parents.clone();
        let mut ancestor_cache = Relatives::new(); //parents.clone());
                                                   // Compute cohorts using algorithms::cohorts, and populate the ancestor_cache
                                                   // FIXME use heuristic algorithm?
        println!("Calling cohorts");
        println!("ancestors: {:?}", ancestor_cache);
        println!("initial_cohort: {:?}", geneses);
        let cohorts = algorithms::cohorts(&parents, &children, &geneses, &mut ancestor_cache);
        println!("Cohorts in Braid::new(): {:?}", cohorts);
        println!("Ancestors in Braid::new(): {:?}", ancestor_cache);

        // Ensure all beads have entries in ancestor_cache (even if empty)
        for cohort in &cohorts {
            for &bead in cohort {
                ancestor_cache.entry(bead).or_insert_with(|| HashSet::new());
            }
        }
        println!("Cohorts(2) in Braid::new(): {:?}", cohorts);
        println!("Ancestors(2) in Braid::new(): {:?}", ancestor_cache);

        // Populate descendant_cache by reversing ancestor_cache (symmetric relationship)
        let mut descendant_cache = Relatives::new();

        // First, ensure all beads have entries in descendant_cache (even if empty)
        for cohort in &cohorts {
            for &bead in cohort {
                descendant_cache
                    .entry(bead)
                    .or_insert_with(|| HashSet::new());
            }
        }

        // Then populate the descendant relationships
        for (bead, ancestors) in &ancestor_cache {
            for ancestor in ancestors {
                descendant_cache
                    .entry(*ancestor)
                    .or_insert_with(|| HashSet::new())
                    .insert(*bead);
            }
        }

        let tail_cache: Vec<BeadSet> = cohorts
            .iter()
            .map(|c| algorithms::geneses(&algorithms::sub_braid(c, &descendant_cache)))
            .collect();

        // Populate cohort_map
        let cohort_map: HashMap<_, _> = cohorts
            .iter()
            .enumerate()
            .flat_map(|(cohort_idx, cohort)| {
                cohort.iter().map(move |&bead_idx| (bead_idx, cohort_idx))
            })
            .collect();

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
            cohort_map,
            ..Default::default()
        }
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

    // ==================== Private: Graph Updates ====================

    /// Helper function to encapsulate common logic for adding a new bead
    /// and updating the core graph structures (beads, index, parents, children, tips).
    /// Returns the index of the newly added bead.
    fn prepare_new_bead_and_update_graph(
        &mut self,
        bead: &Bead,
        bead_hash: BeadHash,
        bead_parents: &BeadSet,
    ) -> BeadIdx {
        // Add bead to beads vector
        self.beads.push(bead.clone());
        let new_bead_index = self.beads.len() - 1;
        self.index.insert(bead_hash, new_bead_index);

        // Update bead_work for the new bead
        self.bead_work.insert(
            new_bead_index,
            Target::from_compact(bead.committed_metadata.weak_target).to_work(),
        );

        // Update parents and children HashMaps for the new bead
        for &parent_index in bead_parents {
            // Update children mapping for the parent
            self.children
                .entry(parent_index)
                .or_default()
                .insert(new_bead_index);
        }
        self.parents.insert(new_bead_index, bead_parents.clone());
        self.children.entry(new_bead_index).or_default(); // Ensure new bead has an entry even if no children yet

        // Update tips
        for &parent_index in bead_parents {
            self.tips.remove(&parent_index);
        }
        self.tips.insert(new_bead_index);

        new_bead_index
    }

    // ==================== Private: Helpers ====================

    /// Compute the tail (internal tips) of a cohort.
    ///
    /// This helper encapsulates the common pattern of creating a sub-braid
    /// for the cohort and computing its tail.
    fn compute_tail(&self, cohort: &Cohort) -> BeadSet {
        let sub_p = algorithms::sub_braid(cohort, &self.parents);
        let sub_c = algorithms::reverse(&sub_p);
        algorithms::cohort_tail(cohort, &sub_p, &sub_c)
    }

    /// Rebuild cohort_map from a starting cohort index.
    ///
    /// If start_idx is 0, clears and rebuilds the entire cohort_map.
    /// Otherwise, retains entries for beads in cohorts[0..start_idx] and rebuilds from start_idx onward.
    fn rebuild_cohort_map_range(&mut self, start_idx: CohortIdx) {
        if start_idx == 0 {
            self.cohort_map.clear();
        } else {
            self.cohort_map
                .retain(|&_bead_idx, cohort_idx| *cohort_idx < start_idx);
        }

        self.cohort_map
            .extend(self.cohorts.iter().enumerate().skip(start_idx).flat_map(
                |(cohort_idx, cohort)| cohort.iter().map(move |&bead_idx| (bead_idx, cohort_idx)),
            ));
    }

    // ==================== Private: Cohort Operations ====================

    /// Atomically merge a range of cohorts into a target cohort, updating all caches.
    ///
    /// This function merges all cohorts in the range `[begin..=end]` into `target_idx`,
    /// updating ancestor_cache, cohort_map, and tail_cache atomically.
    ///
    /// # Arguments
    ///
    /// * `target_idx` - The cohort index that will receive all merged beads
    /// * `begin` - Start of the merge range (inclusive)
    /// * `end` - End of the merge range (inclusive)
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * `begin > end` - Invalid range
    /// * `target_idx >= self.cohorts.len()` - Target doesn't exist
    /// * `end >= self.cohorts.len()` - End index out of bounds
    ///
    /// # Cache Updates
    ///
    /// * `ancestor_cache` - Updated for all merged beads based on parents in target cohort
    /// * `cohort_map` - Fully rebuilt after drain (indices shift)
    /// * `tail_cache` - Updated from min(target_idx, begin) onward
    fn cohort_merge(&mut self, begin: CohortIdx) {
        let end = self.cohorts.len();
        if begin == end {
            return;
        }

        let mut new_cohort = Cohort::new();
        for c in (begin..end).rev() {
            new_cohort.extend(&self.cohorts[c]);
            for &b in &self.cohorts[c] {
                for a in begin..c {
                    self.ancestor_cache
                        .get_mut(&b)
                        .unwrap()
                        .extend(&self.cohorts[a]);
                }
            }
            new_cohort.extend(&self.cohorts[c]);
            self.cohorts.pop();
        }

        self.cohorts.push(new_cohort);

        // Update the cohort map and tail cache
        self.cohort_map
            .extend(self.cohorts[begin].iter().map(|&b| (b, begin)));
        self.tail_cache[begin].clear();
        let tips = self.tail_cache.pop().unwrap();
        self.tail_cache[begin].extend(tips);
        self.tail_cache.truncate(begin);
        println!("Merged cohorts: {:?}", self.cohorts.last());
        // FIXME update descendant_cache
    }

    /// Atomically add a single bead to a specific cohort with its intra-cohort ancestors.
    ///
    /// This function adds a bead to a cohort and updates all caches (ancestor_cache,
    /// cohort_map, tail_cache) atomically.
    ///
    /// # Arguments
    ///
    /// * `bead` - The bead index to add to the cohort
    /// * `cohort` - The cohort index to add the bead to
    /// * `ancestors` - Pre-computed intra-cohort ancestors for this bead
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * `bead >= self.beads.len()` - Bead doesn't exist
    /// * `cohort >= self.cohorts.len()` - Cohort doesn't exist
    /// * `self.cohorts[cohort].contains(&bead)` - Bead already in cohort
    /// * Any ancestor is not in the same cohort
    ///
    /// # Cache Updates
    ///
    /// * `ancestor_cache` - Updated with provided ancestors for the bead
    /// * `cohort_map` - Updated to map bead to cohort
    /// * `tail_cache` - Only the affected cohort's tail is recomputed
    fn cohort_add(&mut self, bead: BeadIdx, cohort: CohortIdx, ancestors: &BeadSet) {
        assert!(
            bead < self.beads.len(),
            "cohort_add: bead index {} out of bounds (len={})",
            bead,
            self.beads.len()
        );
        assert!(
            cohort <= self.cohorts.len(),
            "cohort_add: cohort index {} out of bounds (len={})",
            cohort,
            self.cohorts.len()
        );
        if cohort == self.cohorts.len() {
            self.cohorts.push(BeadSet::new());
            self.tail_cache.push(BeadSet::new());
        }
        assert!(
            !self.cohorts[cohort].contains(&bead),
            "cohort_add: bead {} already in cohort {}",
            bead,
            cohort
        );

        /*
                for &ancestor in ancestors {
                    assert!(
                        self.cohorts[cohort].contains(&ancestor),
                        "cohort_add: ancestor {} not in cohort {}",
                        ancestor,
                        cohort
                    );
                }
        */

        self.cohorts[cohort].insert(bead);

        self.ancestor_cache.insert(bead, ancestors.clone());

        // Ensure the bead itself has an entry in descendant_cache (even if empty)
        self.descendant_cache
            .entry(bead)
            .or_insert_with(|| HashSet::new());

        // Symmetric update for descendant_cache (intra-cohort descendants)
        for &ancestor in ancestors {
            self.descendant_cache
                .entry(ancestor)
                .or_insert_with(|| HashSet::new())
                .insert(bead);
        }

        // 11166676515354272457
        self.cohort_map.insert(bead, cohort);

        //self.tail_cache[cohort] = self.compute_tail(&self.cohorts[cohort]);
        self.tail_cache[cohort] = algorithms::geneses(&algorithms::sub_braid(
            &self.cohorts[cohort],
            &self.descendant_cache,
        ))
    }

    /// Rebuild all caches from a starting cohort index.
    ///
    /// This function rebuilds cohort_map and tail_cache from the specified starting index.
    /// It does NOT compute cohorts - the caller is responsible for cohort computation.
    ///
    /// # Arguments
    ///
    /// * `start_idx` - First cohort index to rebuild caches for
    /// * `preserve_ancestor_cache` - If false, clear ancestor_cache entries for beads in
    ///   cohorts[start_idx..]; if true, preserve them (caller has already updated via
    ///   algorithms::cohorts or manual updates)
    ///
    /// # Panics
    ///
    /// Panics if:
    /// * `start_idx > self.cohorts.len()` - Invalid starting index
    ///
    /// # Cache Updates
    ///
    /// * `ancestor_cache` - Cleared for affected beads if preserve_ancestor_cache=false
    /// * `cohort_map` - Rebuilt for beads in cohorts[start_idx..]
    /// * `tail_cache` - Recomputed from start_idx onward
    ///
    /// # Usage
    ///
    /// * **Heuristic**: preserve_ancestor_cache=true (maintains ancestor_cache incrementally)
    /// * **Cached**: preserve_ancestor_cache=true (algorithms::cohorts updates ancestor_cache)
    /// * **NoCache**: preserve_ancestor_cache=true (algorithms::cohorts updates ancestor_cache)
    fn rebuild_caches_from(&mut self, start_idx: CohortIdx, preserve_ancestor_cache: bool) {
        assert!(
            start_idx <= self.cohorts.len(),
            "rebuild_caches_from: start_idx {} out of bounds (len={})",
            start_idx,
            self.cohorts.len()
        );

        if !preserve_ancestor_cache {
            self.cohorts
                .iter()
                .skip(start_idx)
                .flat_map(|cohort| cohort.iter())
                .for_each(|&b| {
                    self.ancestor_cache.remove(&b);
                    self.descendant_cache.remove(&b);
                });
        }

        self.rebuild_cohort_map_range(start_idx);

        self.tail_cache.truncate(start_idx);
        for cohort in self.cohorts.iter().skip(start_idx) {
            //self.tail_cache.push(self.compute_tail(cohort));
            self.tail_cache
                .push(algorithms::geneses(&algorithms::sub_braid(
                    &cohort,
                    &self.descendant_cache,
                )));
            //self.tail_cache.push(
            //    cohort
            //        .iter()
            //        .filter_map(|&b| {
            //            if self.descendant_cache[&b].is_empty() {
            //                Some(b)
            //            } else {
            //                None
            //            }
            //        })
            //        .collect(),
            //)
        }
    }

    /// Helper to find the minimum and maximum cohort indices that contain any of the given parent beads.
    /// Returns `(Option<min_idx>, Option<max_idx>)`.
    fn find_parent_cohort_span(
        &self,
        bead_parents: &BeadSet,
    ) -> (Option<CohortIdx>, Option<CohortIdx>) {
        let cohort_indices: Vec<CohortIdx> = bead_parents
            .iter()
            .filter_map(|&p| self.cohort_map.get(&p).copied())
            .collect();

        if cohort_indices.is_empty() {
            (None, None)
        } else {
            (
                Some(*cohort_indices.iter().min().unwrap()),
                Some(*cohort_indices.iter().max().unwrap()),
            )
        }
    }

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
        // Already seen this bead in the orphans list
        if self.orphan_index.contains(&bead_hash) {
            return AddBeadStatus::DuplicateBead;
        }
        // Don't have all parents - check if they exist
        if !bead
            .committed_metadata
            .parents
            .iter()
            .all(|h| self.index.contains_key(h))
        {
            self.orphan_index.insert(bead_hash);
            self.orphans.push(bead.clone());
            return AddBeadStatus::ParentsMissing;
        }

        let bead_parents = self.parent_indices(bead);
        let new_bead_index = self.prepare_new_bead_and_update_graph(bead, bead_hash, &bead_parents);

        // --- Strategy-Specific Cohort Updates ---
        match self.extend_strategy {
            ExtendStrategy::Heuristic => {
                println!("=====================================================");
                println!(
                    "Adding bead:{:?} with parents: {:?}",
                    new_bead_index, bead_parents
                );
                println!("Cohorts:    {:?}", self.cohorts);
                println!("Ancestors:  {:?}", self.ancestor_cache);
                println!("Descendant: {:?}", self.descendant_cache);
                println!("Tails:      {:?}", self.tail_cache);
                let oldest_cohort = bead_parents
                    .iter()
                    .map(|p| self.cohort_map[p])
                    .min()
                    .unwrap(); // Not an unsafe unwrap because we determined all parents are present
                println!("Oldest cohort: {:?}", oldest_cohort);

                let oldest_cohort = bead_parents
                    .iter()
                    .map(|p| self.cohort_map[p])
                    .min()
                    .unwrap();

                let newest_cohort = bead_parents
                    .iter()
                    .map(|p| self.cohort_map[p])
                    .max()
                    .unwrap();
                // Check if parents cover ALL tips of the newest parent cohort
                //let covers_tips = self.tail_cache[newest_cohort]
                //    .iter()
                //    .all(|t| bead_parents.contains(t));
                //let covers_tips = bead_parents.is_subset(&self.tail_cache[oldest_cohort]);
                let covers_tips = if oldest_cohort == newest_cohort {
                    bead_parents.is_subset(&self.tail_cache[oldest_cohort])
                } else {
                    // Parents span multiple cohorts - check newest cohort's tail
                    bead_parents.is_subset(&self.tail_cache[newest_cohort])
                };
                let all_parents_in_tails = bead_parents.iter().all(|&p| {
                    let p_cohort = self.cohort_map[&p];
                    self.tail_cache[p_cohort].contains(&p)
                });

                // Check if ALL beads in the newest cohort's tail are covered by parents
                let covers_newest_tail = self.tail_cache[newest_cohort]
                    .iter()
                    .all(|t| bead_parents.contains(t));

                // If all parents are in the same tail_cache, merge the NEXT cohort and all that follow
                //let merge_point = if self.tail_cache[oldest_cohort] == bead_parents {
                let merge_point = if all_parents_in_tails {
                    oldest_cohort + 1
                } else {
                    // Some tail beads are NOT parents → merge into oldest cohort
                    oldest_cohort
                };
                println!("oldest_cohort: {:?}, newest_cohort: {:?}, covers_tips: {:?}, coverst_newest_tail: {:?}, merge_point: {:?}", oldest_cohort, newest_cohort, covers_tips, covers_newest_tail, merge_point);
                println!(
                    "tail_cache[oldest_cohort]: {:?}, bead_parents: {:?}, all_parents_in_tails {:?}",
                    self.tail_cache[oldest_cohort], bead_parents, all_parents_in_tails
                );

                println!("Ancestors after adding bead: {:?}", self.ancestor_cache);
                println!("Merging cohorts: {:?}", &self.cohorts[merge_point..]);

                let mut new_cohort = Cohort::new();
                for c in (merge_point..self.cohorts.len()).rev() {
                    new_cohort.extend(&self.cohorts[c]);
                    for &b in &self.cohorts[c] {
                        for a in merge_point..c {
                            self.ancestor_cache
                                .get_mut(&b)
                                .unwrap()
                                .extend(&self.cohorts[a]);
                        }
                    }
                    new_cohort.extend(&self.cohorts[c]);
                    self.cohorts.pop();
                }
                self.cohorts.push(new_cohort);
                println!("Merged cohorts: {:?}", self.cohorts.last());
                // FIXME
                self.cohorts[merge_point].insert(new_bead_index);
                // Compute intra-cohort ancestors only
                let cohort_parents =
                    algorithms::sub_braid(&self.cohorts[merge_point], &self.parents);
                algorithms::all_ancestors(
                    new_bead_index,
                    &cohort_parents,
                    &mut self.ancestor_cache,
                );
                // Update descendant cache
                self.descendant_cache
                    .extend(algorithms::reverse(&algorithms::sub_braid(
                        &self.cohorts[merge_point],
                        &self.ancestor_cache,
                    )));

                self.tips.retain(|&b| !bead_parents.contains(&b));

                // Update the cohort map and tail cache
                self.cohort_map
                    .extend(self.cohorts[merge_point].iter().map(|&b| (b, merge_point)));

                self.tail_cache.truncate(merge_point);
                self.tail_cache.insert(
                    merge_point,
                    algorithms::geneses(&algorithms::sub_braid(
                        &self.cohorts[merge_point],
                        &self.descendant_cache,
                    )),
                );

                //self.tail_cache.insert(merge_point, self.tips.clone());
                println!("Merged cohorts: {:?}", self.cohorts.last());
                // FIXME update descendant_cache

                println!("Cohorts after merge: {:?}", self.cohorts);
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

                let new_cohorts = algorithms::cohorts(
                    &self.parents,
                    &self.children,
                    &initial_cohort,
                    &mut self.ancestor_cache,
                );

                self.cohorts.extend(new_cohorts);
                self.rebuild_caches_from(start_cohort_idx, true);
            }
            ExtendStrategy::NoCache => {
                self.ancestor_cache.clear();
                self.descendant_cache.clear();
                let geneses = algorithms::geneses(&self.parents);

                self.cohorts = algorithms::cohorts(
                    &self.parents,
                    &self.children,
                    &geneses,
                    &mut self.ancestor_cache,
                );

                let tips_set = algorithms::tips(&self.children);
                algorithms::cohorts(
                    &self.children,
                    &self.parents,
                    &tips_set,
                    &mut self.descendant_cache,
                );

                self.rebuild_caches_from(0, true);
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
                let orphan_hash = orphan_bead.hash();
                self.orphan_index.remove(&orphan_hash);

                match self.extend(&orphan_bead) {
                    AddBeadStatus::BeadAdded => {
                        self.process_orphans();
                        return;
                    }
                    AddBeadStatus::ParentsMissing => {
                        self.orphan_index.insert(orphan_hash);
                        self.orphans.insert(i, orphan_bead);
                        i += 1;
                    }
                    AddBeadStatus::DuplicateBead | AddBeadStatus::InvalidBead => {
                        // Don't re-add to orphans or orphan_index
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
