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
    pub(crate) ancestor_cache: Relatives,
    pub(crate) descendant_cache: Relatives,
    pub(crate) tail_cache: Vec<BeadSet>,
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

        // Truncate ancestor cache to only contain intra-cohort relationships for performance optimization
        // This must happen AFTER cohort computation to avoid corrupting the algorithm
        for cohort in &cohorts {
            algorithms::truncate_cache_to_cohort(&mut ancestor_cache, cohort);
        }

        // Populate the descendant cache for work_sort, hwpath, and descendant_work
        // Use sub_braid approach to ensure only intra-cohort descendant relationships
        let mut descendant_cache = Relatives::new();
        let mut tail_cache = Vec::<BeadSet>::new();
        for c in &cohorts {
            let mut cohort_descendants = Relatives::new();
            let mut temp_cache = Relatives::new();

            // Build sub-braid for this cohort to limit descendant computation to cohort scope
            let sub_parents = algorithms::sub_braid(&c, &parents);
            let sub_children = algorithms::reverse(&sub_parents);

            // Compute descendants only within this cohort using the sub-children
            // Descendants are found by calling all_ancestors with the children map
            for &bead in c {
                algorithms::all_ancestors(
                    bead,
                    &sub_children,
                    &mut cohort_descendants,
                    &mut temp_cache,
                );
            }

            // Add cohort-specific descendants to the main cache
            descendant_cache.extend(cohort_descendants);

            tail_cache.push(algorithms::cohort_tail(
                &c,
                &sub_parents,
                &algorithms::reverse(&sub_parents),
            ));
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
}

impl Braid {
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

        // Find earliest parent of <bead> in cohorts and nuke all cohorts after that (Python algorithm)
        let mut found_parents = BeadSet::new();
        let mut dangling = BeadSet::new();
        dangling.insert(new_bead_index);

        // Iterate cohorts in reverse, collecting found parents and dangling beads
        // This matches the Python algorithm exactly
        while let Some(cohort) = self.cohorts.pop() {
            self.tail_cache.pop(); // Invalidate the tail cache for this cohort
                                   // found_parents |= set(p for p in bead.parents) & c
            for &parent_index in &parent_indices_set {
                if cohort.contains(&parent_index) {
                    found_parents.insert(parent_index);
                }
            }

            // This is not the the first cohort we popped, in which case `cohort` is an ancestor of
            // everything in `dangling`. Add elements of this cohort as ancestors to everything in
            // `dangling`. Then add the cohort to dangling.
            if !dangling.is_empty() {
                for dangling_index in &dangling {
                    // The only element not in in ancestor_cache is the new one. The ancestor_cache
                    // element for new_bead_index will be computed by cohorts()
                    if *dangling_index != new_bead_index {
                        self.ancestor_cache
                            .get_mut(&dangling_index)
                            .unwrap()
                            .extend(cohort.iter().copied());
                    }
                }
            }
            dangling.extend(cohort.iter().copied());

            // Break if we found all parents
            if found_parents.len() == parent_indices_set.len() {
                break;
            }
        }

        // Remove parents from tips if present
        for &parent_index in &parent_indices_set {
            self.tips.remove(&parent_index);
        }

        // Add the new bead's index to tips
        self.tips.insert(new_bead_index);

        // Compute new cohorts from the dangling set
        if !dangling.is_empty() {
            let sub_parents = algorithms::sub_braid(&dangling, &self.parents);
            let sub_children = algorithms::reverse(&sub_parents);
            let sub_geneses = algorithms::geneses(&sub_parents);

            // Ensure the ancestor_cache is fully populated for the new_bead_index before cohort recomputation.
            // This fills the persistent self.ancestor_cache with all transitive ancestors of the new bead.
            algorithms::all_ancestors(
                new_bead_index,
                &sub_parents,
                &mut Relatives::new(), // Use a temporary accumulator that we throw away
                &mut self.ancestor_cache, // This is the persistent cache
            );

            // Call algorithms::cohorts. It will use the persistent self.ancestor_cache as its memoization store.
            dangling.remove(&new_bead_index); // Remove the new bead from dangling
            let new_cohorts = algorithms::cohorts(
                &sub_parents,
                &sub_children,
                &dangling,
                &mut self.ancestor_cache, // Pass the Braid's persistent cache which has already
                                          // been updated for everything in dangling except the new
                                          // bead
            );

            // Update cohort tail cache for new cohorts
            for (cohort_idx, cohort) in new_cohorts.iter().enumerate() {
                let sub_sub_parents = algorithms::sub_braid(cohort, &sub_parents);
                let sub_sub_children = algorithms::reverse(&sub_sub_parents);
                let cohort_tails = algorithms::tips(&sub_sub_children);
                self.tail_cache
                    .insert(self.cohorts.len() + cohort_idx, cohort_tails);
            }

            // Add the new cohorts
            for new_cohort in new_cohorts {
                self.cohorts.push(new_cohort);
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
