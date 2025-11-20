use super::algorithms::*;
use super::{AddBeadStatus, Braid};
use crate::braid::Cohort;
use crate::relatives;
use crate::utils::test_utils::test_utility_functions::emit_bead;
use std::collections::{HashMap, HashSet};

#[test]
pub fn test_extend_functionality() {
    // Create a braid with one bead.
    let test_bead_0 = emit_bead();

    let mut test_braid = Braid {
        beads: vec![test_bead_0.clone()],
        geneses: HashSet::from([0]),
        tips: HashSet::from([0]),
        orphans: Vec::new(),
        cohorts: vec![Cohort::from([0])],
        index: HashMap::from([(test_bead_0.hash(), 0)]),
        parents: relatives!(),
        children: relatives!(),
    };
    assert_eq!(
        test_braid.cohorts,
        vec![Cohort::from([0])],
        "Initial cohort should contain only the genesis bead"
    );

    // Test simple chain extension: 0 -> 1 -> 2
    let mut test_bead_1 = emit_bead();
    test_bead_1
        .committed_metadata
        .parents
        .insert(test_bead_0.hash());

    let result = test_braid.extend(&test_bead_1);
    assert_eq!(result, AddBeadStatus::BeadAdded);
    assert_eq!(test_braid.beads.len(), 2);
    assert!(test_braid.cohorts.len() >= 2); // Should have at least 2 cohorts
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&0))); // Contains bead 0
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&1))); // Contains bead 1

    let mut test_bead_2 = emit_bead();
    test_bead_2
        .committed_metadata
        .parents
        .insert(test_bead_1.hash());

    let result = test_braid.extend(&test_bead_2);
    assert_eq!(result, AddBeadStatus::BeadAdded);
    assert_eq!(test_braid.beads.len(), 3);
    assert!(test_braid.cohorts.len() >= 3); // Should have at least 3 cohorts
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&2))); // Contains bead 2

    // Test branching: beads 3 and 4 both branch from bead 2
    let mut test_bead_3 = emit_bead();
    test_bead_3
        .committed_metadata
        .parents
        .insert(test_bead_2.hash());

    let mut test_bead_4 = emit_bead();
    test_bead_4
        .committed_metadata
        .parents
        .insert(test_bead_2.hash());

    let result = test_braid.extend(&test_bead_3);
    assert_eq!(result, AddBeadStatus::BeadAdded);

    let result = test_braid.extend(&test_bead_4);
    assert_eq!(result, AddBeadStatus::BeadAdded);

    assert_eq!(test_braid.beads.len(), 5);
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&3))); // Contains bead 3
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&4))); // Contains bead 4

    // Test merge: bead 5 merges from beads 3 and 4
    let mut test_bead_5 = emit_bead();
    test_bead_5
        .committed_metadata
        .parents
        .insert(test_bead_3.hash());
    test_bead_5
        .committed_metadata
        .parents
        .insert(test_bead_4.hash());

    let result = test_braid.extend(&test_bead_5);
    assert_eq!(result, AddBeadStatus::BeadAdded);
    assert_eq!(test_braid.beads.len(), 6);
    assert!(test_braid.cohorts.iter().any(|c| c.contains(&5))); // Contains bead 5

    // Verify braid integrity
    assert_eq!(test_braid.geneses, HashSet::from([0])); // Still only one genesis
    assert_eq!(test_braid.tips, HashSet::from([5])); // Bead 5 is the only tip
    assert_eq!(test_braid.orphans.len(), 0); // No orphans
}

#[test]
pub fn test_orphans_functionality() {
    let test_bead_0 = emit_bead();
    let mut test_bead_1 = emit_bead();
    test_bead_1
        .committed_metadata
        .parents
        .insert(test_bead_0.hash());

    let mut test_bead_2 = emit_bead();
    // Give test_bead_2 a fake parent that doesn't exist in the braid
    let fake_parent_hash = emit_bead().hash();
    test_bead_2
        .committed_metadata
        .parents
        .insert(fake_parent_hash);
    let mut test_bead_3 = emit_bead();
    test_bead_3
        .committed_metadata
        .parents
        .insert(test_bead_1.hash());

    let mut test_braid = Braid {
        beads: vec![test_bead_0.clone()],
        geneses: HashSet::from([0]),
        tips: HashSet::from([0]),
        orphans: Vec::new(),
        cohorts: vec![Cohort::from([0])],
        index: HashMap::from([(test_bead_0.hash(), 0)]),
        parents: relatives!(),
        children: relatives!(),
    };
    assert_eq!(
        test_braid.cohorts,
        vec![Cohort::from([0])],
        "Initial cohort should contain only the genesis bead"
    );
    let result = test_braid.extend(&test_bead_1);
    assert_eq!(result, AddBeadStatus::BeadAdded);
    let result = test_braid.extend(&test_bead_2);
    assert_eq!(result, AddBeadStatus::ParentsMissing);
    assert_eq!(test_braid.orphans.len(), 1);
    let result = test_braid.extend(&test_bead_3);
    assert_eq!(result, AddBeadStatus::BeadAdded);
    assert_eq!(test_braid.orphans.len(), 1);
    let result = test_braid.extend(&test_bead_2);
    assert_eq!(result, AddBeadStatus::DagAlreadyContainsBead);
    assert_eq!(test_braid.orphans.len(), 1);
}

#[test]
pub fn test_diamond_path_highest_work() {
    // Test diamond pattern: 0 -> (1,2) -> 3
    // This tests highest work path selection in a complex braid structure
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1, 2],
    );

    let children = reverse(&parents);

    // Test the highest work path algorithm
    let path = highest_work_path(&parents, &children, None).unwrap();

    // Should return one of the valid paths in the diamond
    // Path 0->1->3 or path 0->2->3, both are valid
    assert!(path == vec![0, 1, 3] || path == vec![0, 2, 3]);

    // The path should start at genesis (0) and end at tip (3)
    assert_eq!(path[0], 0);
    assert_eq!(path[path.len() - 1], 3);
}

