use super::BeadIdx;
use crate::error::BraidError;
use num::{BigUint, One, Zero};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Returns the set of **genesis beads** from a given Braid object.
///
/// A **genesis bead** is defined as a bead that has no parents, i.e., it is a root node in the Braid DAG.
/// These beads represent the starting points in the Braidpool architecture, with no dependencies upstream.
///
/// # Arguments
///
/// * `braid_obj` - A reference to the `Braid` object.  
///   While not directly used in the logic here, it is included to align with interface expectations or future-proofing.
///
/// * `parents` - A map from bead indices to their parent bead indices.
///   Each entry represents a bead and its set of parent beads.
///
/// # Returns
///
/// A `HashSet<BeadIdx>` containing the indices of all beads that do not have any parents.
///
/// # Reference
///
/// For architectural context, refer to the [Braidpool Specification](https://github.com/braidpool/braidpool/blob/dev/docs/braidpool_spec.md).
pub fn geneses(parents: &HashMap<BeadIdx, HashSet<BeadIdx>>) -> HashSet<BeadIdx> {
    let mut genesis_bead_indices: HashSet<BeadIdx> = HashSet::new();
    for bead in parents {
        if parents[&bead.0].is_empty() {
            genesis_bead_indices.insert(*bead.0);
        }
    }
    return genesis_bead_indices;
}

/// Returns the set of **tip beads** from parent and child relationships.
///
/// A **tip bead** is defined as a bead that has no children (i.e., no other bead references it as a parent).
/// These beads represent the leaves or endpoints in the Braid DAG structure.
///
/// # Arguments
///
/// * `parents` - A map from each bead index to a `HashSet` of its parent bead indices.
/// * `children` - A map from each bead index to a `HashSet` of its child bead indices.
///
/// # Returns
///
/// A `HashSet<BeadIdx>` containing the indices of all beads that are **not referenced** as a parent by any other bead.
pub fn tips(
    _parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> HashSet<BeadIdx> {
    geneses(children)
}

/// Reverses the parent mapping of beads to generate a child mapping.
///
/// Given a mapping from each bead to its parent beads (`parents`), this function constructs
/// the reverse: a mapping from each bead to the set of **children** beads that reference it as a parent.
///
/// # Arguments
///
/// * `parents` - A `HashMap` where each key is a bead index, and the value is a `HashSet` of its parent bead indices.
///
/// # Returns
///
/// A `HashMap<BeadIdx, HashSet<BeadIdx>>` where each key is a bead index, and the value is a set of its children.
pub fn reverse(parents: &HashMap<BeadIdx, HashSet<BeadIdx>>) -> HashMap<BeadIdx, HashSet<BeadIdx>> {
    let mut children: HashMap<BeadIdx, HashSet<BeadIdx>> = HashMap::new();

    for (bead, bparents) in parents {
        if !children.contains_key(bead) {
            children.insert(*bead, HashSet::new());
        }

        for parent in bparents {
            if !children.contains_key(parent) {
                children.insert(*parent, HashSet::new());
            }
            children.get_mut(parent).unwrap().insert(*bead);
        }
    }
    children
}

/// Returns the complete set of **child beads** for a given set of bead indices.
///
/// This function computes the immediate children of a set of beads.
///
/// It is useful when traversing a Braid structure **forward** from a set of beads
/// to explore their direct descendants.
///
/// # Arguments
///
/// * `beads` - A `HashSet` of bead indices whose children need to be found.
/// * `children` - A reference to a `HashMap` representing bead → children mappings.
///
/// # Returns
///
/// A `HashSet<BeadIdx>` containing all bead indices that are children of the given input beads.
pub fn generation(
    beads: &HashSet<BeadIdx>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> HashSet<BeadIdx> {
    let mut retval: HashSet<BeadIdx> = HashSet::new();
    for b in beads {
        retval.extend(&children[b]);
    }
    retval
}

/// Computes all ancestors for a bead using an iterative algorithm.
/// Translated directly from Python implementation.
/// Assumes bead not in ancestors.
pub fn all_ancestors(
    bead: BeadIdx,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    ancestors: &mut HashMap<BeadIdx, HashSet<BeadIdx>>,
) {
    let mut work_stack: Vec<(BeadIdx, bool)> = vec![(bead, false)]; // (bead, is_processed)

    while let Some((current, is_processed)) = work_stack.pop() {
        if is_processed {
            // We've finished processing all parents, compute ancestors
            let current_ancestors = ancestors.entry(current).or_insert_with(HashSet::new);
            current_ancestors.clear();

            // Add direct parents
            if let Some(parent_set) = parents.get(&current) {
                current_ancestors.extend(parent_set);
            }

            // Update with ancestors of all parents
            if let Some(parent_set) = parents.get(&current) {
                let parent_indices: Vec<BeadIdx> = parent_set.iter().copied().collect();
                for parent_idx in parent_indices {
                    let parent_ancestors: HashSet<BeadIdx> = ancestors
                        .get(&parent_idx)
                        .map(|pa| pa.clone())
                        .unwrap_or_default();
                    let current_ancestors = ancestors.get_mut(&current).unwrap();
                    current_ancestors.extend(parent_ancestors);
                }
            }
        } else {
            // Mark as being processed
            work_stack.push((current, true));

            // Add any unprocessed parents to the stack
            if let Some(parent_set) = parents.get(&current) {
                for parent_idx in parent_set {
                    if !ancestors.contains_key(parent_idx) {
                        work_stack.push((*parent_idx, false));
                    }
                }
            }
        }
    }
}

/// Computes the **cohorts** in a Braid, representing subgraphs (slices) bounded by graph cuts.
///
/// A **cohort** is a set of bead indices forming a layer where all included beads share the same
/// topological generation, i.e., all their ancestors lie strictly in earlier cohorts, and all
/// descendants lie strictly in later cohorts.
///
/// Graphically, this corresponds to a **graph cut**: a boundary line across the DAG such that
/// every bead on the right side of the cut has all beads on the left side as its ancestors.
///
/// # Arguments
///
/// * `parents` - A map from bead index to its parent indices, used for ancestry traversal.
/// * `children` - A map from bead index to its child indices. Required parameter.
/// * `initial_cohort` - Optional starting cohort (e.g., genesis beads). If `None`, it defaults to `geneses(parents)`.
///
/// # Returns
///
/// A generator that yields `HashSet<BeadIdx>`, where each set represents a **cohort** in topological order.
/// Each cohort is disjoint and collectively they partition the beads in the Braid up to time `T`.
pub fn cohorts(
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    initial_cohort: Option<&HashSet<BeadIdx>>,
) -> Vec<HashSet<BeadIdx>> {
    let dag_tips = tips(parents, children);
    let mut cohort = match initial_cohort {
        Some(initial) => initial.clone(),
        None => geneses(parents),
    };
    let mut oldcohort = HashSet::new();
    let mut head = cohort.clone();
    let mut tail = cohort.clone();
    let mut result = Vec::new();

    loop {
        // Don't let head have ancestors to stop iteration
        let mut ancestors: HashMap<BeadIdx, HashSet<BeadIdx>> = HashMap::new();
        for h in &head {
            ancestors.insert(*h, HashSet::new());
        }

        cohort = head.clone();

        // DFS search
        loop {
            if head.is_empty() {
                return result; // StopIteration and return
            }

            // Calculate new tail
            for b in cohort.difference(&oldcohort) {
                tail.extend(&children[b]); // Add the next generation to the tail
            }
            tail.extend(cohort.difference(&oldcohort).copied()); // Add any beads in oldcohort but not in cohort

            let cohort_has_tips = cohort.iter().any(|b| dag_tips.contains(b));
            if cohort_has_tips {
                tail.extend(dag_tips.difference(&cohort).copied()); // If there are any tips in cohort, add tips to tail
            } else {
                // If there are no tips in cohort subtract off cohort
                for t in &cohort {
                    tail.remove(t);
                }
            }

            oldcohort = cohort.clone(); // Copy so we can tell if new tail has changed anything

            // Calculate ancestors
            for t in tail.difference(&HashSet::from_iter(ancestors.keys().copied())) {
                all_ancestors(*t, parents, &mut ancestors); // half the CPU time is here
            }

            // Calculate cohort
            cohort = ancestors
                .values()
                .flat_map(|set| set.iter())
                .copied()
                .collect(); // Union all ancestors with the cohort

            // Check termination cases
            if dag_tips.is_subset(&cohort) {
                head.clear(); // StopIteration and return
                break; // and yield the current cohort
            }
            if !cohort.is_empty()
                && tail.iter().all(|t| {
                    ancestors
                        .get(t)
                        .map_or(false, |ancestors_of_t| ancestors_of_t == &cohort)
                })
            {
                head = tail.clone(); // Head of next cohort is tail from previous iteration
                break; // Yield successful cohort
            }
            if cohort == oldcohort {
                // Cohort hasn't changed, we may be looping
                if dag_tips.is_subset(&tail) {
                    head.clear();
                    cohort.extend(&tail);
                    tail.clear();
                    break; // Yield cohort+tail
                }
                cohort.extend(&tail);
            }
        }

        oldcohort.clear();
        if !cohort.is_empty() {
            result.push(cohort.clone());
        }
    }
}

/// Returns the tail of a cohort.
/// Translated directly from Python implementation.
pub fn cohort_tail(
    cohort: &HashSet<BeadIdx>,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> HashSet<BeadIdx> {
    cohort_head(cohort, children, parents)
}

/// Returns the head of a cohort.
/// Translated directly from Python implementation.
pub fn cohort_head(
    cohort: &HashSet<BeadIdx>,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> HashSet<BeadIdx> {
    let tail = generation(
        &generation(cohort, parents)
            .difference(cohort)
            .copied()
            .collect::<HashSet<_>>(),
        children,
    );
    let cohort_geneses = geneses(parents);

    if tail.is_empty() || tail.iter().any(|t| cohort_geneses.contains(t)) {
        cohort_geneses
    } else {
        tail
    }
}

/// Constructs a **sub-braid** from a specified set of bead indices within a Braid DAG.
///
/// A *sub-braid* is defined as the subgraph induced by a subset of beads —
/// that is, only the beads in the input `beads` set are considered, and only the parent
/// relationships between those beads are retained.
///
/// This is especially useful in contexts like:
/// - **Pruning** parts of the DAG.
/// - **Cohort isolation** or localized validation.
/// - Visualization of subgraphs or ancestry scopes.
///
/// The result has the properties:
///     geneses(sub_braid(beads, parents)) == cohort_head(beads, parents)
///     tips(sub_braid(beads, parents)) == cohort_tail(beads, parents)
///     cohorts(sub_braid(beads, parents)) == [beads]
///
/// # Arguments
///
/// * `beads` - A set of bead indices to include in the sub-braid.
/// * `parents` - A mapping from each bead index to its set of parent bead indices (the full parent DAG).
///
/// # Returns
///
/// A `HashMap<BeadIdx, HashSet<BeadIdx>>` where:
/// - Keys are bead indices from the `beads` set.
/// - Values are sets of **parents also within the `beads` set**.
pub fn sub_braid(
    beads: &HashSet<BeadIdx>,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> HashMap<BeadIdx, HashSet<BeadIdx>> {
    beads
        .iter()
        .map(|b| {
            let parent_set: HashSet<BeadIdx> = parents.get(b).map_or(HashSet::new(), |ps| {
                ps.iter().filter(|p| beads.contains(p)).copied().collect()
            });
            (*b, parent_set)
        })
        .collect()
}

/// Computes the descendant work for each bead in the Braid DAG.
/// Translated directly from Python implementation: `descendant_work(parents, children=None, bead_work=None, in_cohorts=None)`
pub fn descendant_work(
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    bead_work: Option<&HashMap<BeadIdx, BigUint>>,
    in_cohorts: Option<&[HashSet<BeadIdx>]>,
) -> HashMap<BeadIdx, BigUint> {
    let default_bead_work: HashMap<BeadIdx, BigUint> =
        parents.keys().map(|&k| (k, BigUint::one())).collect();
    let bead_work = bead_work.unwrap_or(&default_bead_work);

    let mut previous_work = BigUint::zero();
    let rev_cohorts = match in_cohorts {
        Some(cohorts) => cohorts.iter().rev().cloned().collect::<Vec<_>>(),
        None => cohorts(children, parents, None),
    };

    let mut retval = HashMap::new();

    for cohort in rev_cohorts {
        let sub_children = sub_braid(&cohort, children);
        let mut sub_descendants = HashMap::new();

        for b in &cohort {
            all_ancestors(*b, &sub_children, &mut sub_descendants);
            let descendant_sum: BigUint = sub_descendants
                .get(b)
                .map(|descendants| descendants.iter().map(|d| &bead_work[d]).sum())
                .unwrap_or(BigUint::zero());
            retval.insert(*b, previous_work.clone() + &bead_work[b] + descendant_sum);
        }

        // All beads in the next cohort have ALL beads in this cohort as descendants.
        let cohort_work_sum: BigUint = cohort.iter().map(|b| &bead_work[b]).sum();
        previous_work += cohort_work_sum;
    }

    retval
}

/// Custom comparison function for sorting beads. This function requires the work function,
/// which should be the output of descendant_work().
/// Translated directly from Python implementation.
pub fn bead_cmp(
    a: BeadIdx,
    b: BeadIdx,
    dwork: &HashMap<BeadIdx, BigUint>,
    awork: &HashMap<BeadIdx, BigUint>,
) -> Result<Ordering, BraidError> {
    if dwork[&a] < dwork[&b] {
        Ok(Ordering::Less) // highest work
    } else if dwork[&a] > dwork[&b] {
        Ok(Ordering::Greater)
    } else if awork[&a] < awork[&b] {
        Ok(Ordering::Less)
    } else if awork[&a] > awork[&b] {
        Ok(Ordering::Greater)
    } else if a > b {
        Ok(Ordering::Less) // same work, fall back on block hash ("luck")
    } else if a < b {
        Ok(Ordering::Greater)
    } else {
        Ok(Ordering::Equal)
    }
}

/// Return a sorting key lambda suitable for sorting beads by work.
/// Translated directly from Python implementation.
fn work_sort_key_fn<'a>(
    parents: &'a HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &'a HashMap<BeadIdx, HashSet<BeadIdx>>,
    bead_work: &'a HashMap<BeadIdx, BigUint>,
) -> impl Fn(&BeadIdx, &BeadIdx) -> Result<Ordering, BraidError> + 'a {
    let dwork = descendant_work(parents, children, Some(bead_work), None);
    let awork = descendant_work(children, parents, Some(bead_work), None);

    move |a: &BeadIdx, b: &BeadIdx| bead_cmp(*a, *b, &dwork, &awork)
}

/// Find the highest (descendant) work path, by following the highest weights through the DAG.
/// Translated directly from Python implementation: `highest_work_path(parents, children=None, bead_work=None)`
pub fn highest_work_path(
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    bead_work: Option<HashMap<BeadIdx, BigUint>>,
) -> Result<Vec<BeadIdx>, BraidError> {
    let bead_work = match bead_work {
        Some(work) => work,
        None => parents.keys().map(|&k| (k, BigUint::one())).collect(),
    };

    let sort_key_fn = work_sort_key_fn(parents, children, &bead_work);
    let mut hwpath = vec![*geneses(parents)
        .iter()
        .max_by(|a, b| sort_key_fn(a, b).unwrap())
        .ok_or(BraidError::HighestWorkBeadFetchFailed)?];

    let dag_tips = tips(parents, children);
    while !dag_tips.contains(&hwpath[hwpath.len() - 1]) {
        let mut children_set = HashSet::new();
        children_set.insert(hwpath[hwpath.len() - 1]);
        let current_children = generation(&children_set, children);
        let max_child = current_children
            .iter()
            .max_by(|a, b| sort_key_fn(a, b).unwrap())
            .ok_or(BraidError::HighestWorkBeadFetchFailed)?;
        hwpath.push(*max_child);
    }

    Ok(hwpath)
}

/// Check a cohort using check_cohort_ancestors in both directions.
/// Translated directly from Python implementation.
pub fn check_cohort(
    cohort: &HashSet<BeadIdx>,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> bool {
    check_cohort_ancestors(cohort, parents, children)
        && check_cohort_ancestors(cohort, children, parents)
}

/// Check a cohort by determining the set of ancestors of all beads.
/// This computation is done over the ENTIRE DAG since any ancestor could have a long dangling path leading to this cohort.
/// This will not determine if a cohort has valid sub-cohorts since the merging of any two or more adjacent cohorts is still a valid cohort.
///
/// This checks in one direction only, looking at the ancestors of `cohort`. To check in the other direction, reverse the order of the parents and children arguments.
/// Translated directly from Python implementation.
pub fn check_cohort_ancestors(
    cohort: &HashSet<BeadIdx>,
    parents: &HashMap<BeadIdx, HashSet<BeadIdx>>,
    children: &HashMap<BeadIdx, HashSet<BeadIdx>>,
) -> bool {
    let mut ancestors = HashMap::new();
    let mut allancestors = HashSet::new();

    let head = cohort_head(cohort, parents, children);

    for b in cohort {
        all_ancestors(*b, parents, &mut ancestors);
        if let Some(bead_ancestors) = ancestors.get(b) {
            allancestors.extend(bead_ancestors);
        }
    }

    allancestors.retain(|a| !cohort.contains(a));

    if !allancestors.is_empty() {
        let gen_children = generation(&allancestors, children);
        let diff: HashSet<BeadIdx> = gen_children.difference(&allancestors).copied().collect();
        if diff != head {
            return false;
        }
    }

    true
}
