use crate::bead::{Bead, BeadHash};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread::ThreadId;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

pub mod algorithms;

// A type which indexes Braid::beads
pub type BeadIdx = usize;
// A type representing children or parents
pub type Relatives = HashMap<BeadIdx, HashSet<BeadIdx>>;
// A type representing a set of beads indexed in Braid::beads
pub type BeadSet = HashSet<BeadIdx>;

//#[derive(Clone, Debug, Serialize, PartialEq, Deserialize)]
// A type representing a cohort (a set of beads indexed in Braid::beads)
pub type Cohort = HashSet<BeadIdx>;

#[derive(Debug, Clone, PartialEq)]
pub enum AddBeadStatus {
    DagAlreadyContainsBead,
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

#[derive(Clone, Debug)]
pub struct Braid {
    pub beads: Vec<Bead>,
    pub tips: HashSet<BeadIdx>,
    pub cohorts: Vec<Cohort>,
    pub orphans: Vec<Bead>,
    pub geneses: HashSet<BeadIdx>,
    pub index: HashMap<BeadHash, BeadIdx>,
    pub parents: Relatives,
    pub children: Relatives,
}

impl Braid {
    ///Initializing the Braid object for keeping track of current state of Braid
    pub fn new(geneses: Vec<Bead>) -> Self {
        let mut beads = Vec::new();
        let mut bead_indices = HashSet::new();
        let mut index = HashMap::new();
        let mut genesis_cohort: Vec<Cohort> = Vec::new();

        for (idx, bead) in geneses.into_iter().enumerate() {
            beads.push(bead.clone());
            bead_indices.insert(idx);
            index.insert(bead.hash(), idx);
        }
        if bead_indices.len() != 0 {
            genesis_cohort.push(bead_indices.clone());
        }
        Braid {
            beads,
            tips: bead_indices.clone(),
            cohorts: genesis_cohort,
            orphans: Vec::new(),
            geneses: bead_indices,
            index,
            parents: Relatives::new(),
            children: Relatives::new(),
        }
    }
}
#[allow(unused)]
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
        if self.beads.iter().any(|b| b.hash() == bead_hash) {
            return AddBeadStatus::DagAlreadyContainsBead;
        }

        // Already seen this bead in the orphans list
        if self.orphans.iter().any(|b| b.hash() == bead_hash) {
            return AddBeadStatus::DagAlreadyContainsBead;
        }

        // Don't have all parents
        for parent_hash in &bead.committed_metadata.parents {
            let parent_exists = self.index.contains_key(parent_hash);

            if !parent_exists {
                self.orphans.push(bead.clone());
                return AddBeadStatus::ParentsMissing;
            }
        }

        // Insert bead into beads vector
        self.beads.push(bead.clone());
        let new_bead_index = self.beads.len() - 1;
        self.index.insert(bead_hash, new_bead_index);

        // Update parents and children HashMaps for the new bead
        let mut parent_indices = HashSet::new();
        for parent_hash in &bead.committed_metadata.parents {
            if let Some(&parent_index) = self.index.get(parent_hash) {
                parent_indices.insert(parent_index);
                // Update children mapping for the parent
                self.children
                    .entry(parent_index)
                    .or_insert_with(HashSet::new)
                    .insert(new_bead_index);
            }
        }
        self.parents.insert(new_bead_index, parent_indices);
        self.children
            .entry(new_bead_index)
            .or_insert_with(HashSet::new);

        // Find earliest parent of <bead> in cohorts and nuke all cohorts after that (Python algorithm)
        let mut found_parents = HashSet::new();
        let mut dangling = HashSet::new();
        dangling.insert(new_bead_index);

        // Collect cohorts to restore (those popped before finding all parents)
        let mut cohorts_to_restore = Vec::new();

        // Iterate cohorts in reverse, collecting found parents and dangling beads
        while let Some(cohort) = self.cohorts.pop() {
            // Check if this cohort contains any parents of the new bead
            let mut cohort_has_parents = false;
            for parent_hash in &bead.committed_metadata.parents {
                if let Some(&parent_index) = self.index.get(parent_hash) {
                    if cohort.contains(&parent_index) {
                        found_parents.insert(parent_index);
                        cohort_has_parents = true;
                    }
                }
            }

            if cohort_has_parents {
                // This cohort has parents - add it to dangling and stop
                for idx in cohort.iter() {
                    dangling.insert(*idx);
                }
                // If we found all parents, we're done
                if found_parents.len() == bead.committed_metadata.parents.len() {
                    break;
                }
            } else {
                // This cohort doesn't have parents - save it to restore later
                cohorts_to_restore.push(cohort);
                // Still add its beads to dangling since they're after the parent boundary
                for idx in cohorts_to_restore.last().unwrap().iter() {
                    dangling.insert(*idx);
                }
            }
        }

        // Restore cohorts that were popped but didn't contain parents (in reverse order to maintain original)
        for cohort in cohorts_to_restore.into_iter().rev() {
            self.cohorts.push(cohort);
        }

        // Remove parents from tips if present
        for parent_hash in &bead.committed_metadata.parents {
            // Find the index of the parent bead
            if let Some(&parent_index) = self.index.get(parent_hash) {
                self.tips.remove(&parent_index);
            }
        }

        // Add the new bead's index to tips
        self.tips.insert(new_bead_index);

        // Compute new cohorts from the dangling set using direct map usage
        if !dangling.is_empty() {
            // Use simple approach: get sub_braid parents and compute children directly
            let sub_parents = algorithms::sub_braid(&dangling, &self.parents);
            let sub_children = algorithms::reverse(&sub_parents);

            // Call cohorts exactly like Python: cohorts(sub_parents, sub_children, None)
            let new_cohorts = algorithms::cohorts(&sub_parents, &sub_children, None);

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
        // Process orphans in reverse order to maintain proper indexing
        let mut i = self.orphans.len();
        while i > 0 {
            i -= 1;

            // Check if all parents are now available for this orphan
            let mut all_parents_available = true;
            for parent_hash in &self.orphans[i].committed_metadata.parents {
                if !self.index.contains_key(parent_hash) {
                    all_parents_available = false;
                    break;
                }
            }

            if all_parents_available {
                // Remove the orphan bead first, then process it
                let orphan_bead = self.orphans.remove(i);

                // Now extend with the orphan bead
                match self.extend(&orphan_bead) {
                    AddBeadStatus::BeadAdded => {
                        // Recursively process remaining orphans as this addition
                        // might enable more orphans to be processed
                        self.process_orphans();
                        return; // Exit current processing as recursion will handle the rest
                    }
                    AddBeadStatus::DagAlreadyContainsBead => {
                        continue;
                    }
                    AddBeadStatus::InvalidBead => {
                        continue;
                    }
                    AddBeadStatus::ParentsMissing => {
                        self.orphans.push(orphan_bead);
                    }
                }
            }
        }
    }

    pub fn check_geneses(&self, geneses: &Vec<BeadHash>) -> GenesisCheckStatus {
        if (geneses.len() != self.geneses.len()) {
            return GenesisCheckStatus::GenesisBeadsCountMismatch;
        }
        for bead_hash in geneses {
            let index = self.index.get(bead_hash);
            let bead_exists = match index {
                Some(idx) => self.geneses.contains(idx),
                None => false,
            };
            if !bead_exists {
                return GenesisCheckStatus::MissingGenesisBead;
            }
        }
        return GenesisCheckStatus::GenesisBeadsValid;
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
