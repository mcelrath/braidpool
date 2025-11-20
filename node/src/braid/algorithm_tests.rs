// Pure algorithm tests for braid consensus algorithms
// These tests only test pure algorithm functions that operate on HashMap data structures
// and do NOT depend on the Bead struct or braid integration.

use super::algorithms::*;
use crate::braid::BeadIdx;
use crate::relatives;
use num::BigUint;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;

// Directory containing braid test files (relative to project root)
const BRAID_TEST_DIR: &str = "tests/braids";

#[test]
pub fn test_reverse() {
    let parents1 = relatives!(
        0 => [],
        1 => [],
        2 => [0],
        3 => [1, 2],
        4 => [3],
    );
    let reverse_children_mapping = reverse(&parents1);
    let expected_children_mapping = relatives!(
        0 => [2],
        1 => [3],
        2 => [3],
        3 => [4],
        4 => [],
    );
    assert_eq!(reverse_children_mapping, expected_children_mapping);
}

#[test]
pub fn test_genesis_empty() {
    let parents = relatives!();
    let genesis_indices = geneses(&parents);
    assert_eq!(genesis_indices, HashSet::new());
}

#[test]
pub fn test_genesis_single() {
    let parents = relatives!(
        0 => [],
    );
    let genesis_indices = geneses(&parents);
    assert_eq!(genesis_indices, HashSet::from([0]));
}

#[test]
pub fn test_genesis_multiple() {
    let parents = relatives!(
        0 => [],
        1 => [],
        2 => [0, 1],
    );
    let genesis_indices = geneses(&parents);
    assert_eq!(genesis_indices, HashSet::from([0, 1]));
}

#[test]
pub fn test_genesis_chain() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [1],
        3 => [2],
    );
    let genesis_indices = geneses(&parents);
    assert_eq!(genesis_indices, HashSet::from([0]));
}

#[test]
pub fn test_genesis_three_parallel() {
    let parents = relatives!(
        0 => [],
        1 => [],
        2 => [],
        3 => [1],
        4 => [0],
    );
    let genesis_indices = geneses(&parents);
    assert_eq!(genesis_indices, HashSet::from([0, 1, 2]));
}

#[test]
pub fn test_tips_empty() {
    let parents = relatives!();
    let children = reverse(&parents);
    let tips_indices = tips(&parents, &children);
    assert_eq!(tips_indices, HashSet::new());
}

#[test]
pub fn test_tips_single() {
    let parents = relatives!(
        0 => [],
    );
    let children = reverse(&parents);
    let tips_indices = tips(&parents, &children);
    assert_eq!(tips_indices, HashSet::from([0]));
}

#[test]
pub fn test_tips_simple() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
    );
    let children = reverse(&parents);
    let tips_indices = tips(&parents, &children);
    assert_eq!(tips_indices, HashSet::from([1, 2]));
}

#[test]
pub fn test_all_ancestors_simple() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [1],
        3 => [2],
    );

    let mut ancestors = relatives!();
    all_ancestors(3, &parents, &mut ancestors);

    assert_eq!(ancestors.get(&0), Some(&HashSet::new()));
    assert_eq!(ancestors.get(&1), Some(&HashSet::from([0])));
    assert_eq!(ancestors.get(&2), Some(&HashSet::from([0, 1])));
    assert_eq!(ancestors.get(&3), Some(&HashSet::from([0, 1, 2])));
}

#[test]
pub fn test_cohorts_simple() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [1],
        3 => [2],
    );

    let children = reverse(&parents);
    let cohort_indices = cohorts(&parents, &children, None);

    // Each bead should be in its own cohort since it's a simple chain
    assert_eq!(cohort_indices.len(), 4);
    assert!(cohort_indices.iter().any(|c| c == &HashSet::from([0])));
    assert!(cohort_indices.iter().any(|c| c == &HashSet::from([1])));
    assert!(cohort_indices.iter().any(|c| c == &HashSet::from([2])));
    assert!(cohort_indices.iter().any(|c| c == &HashSet::from([3])));
}

#[test]
pub fn test_cohorts_parallel() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
    );

    let children = reverse(&parents);
    let cohort_indices = cohorts(&parents, &children, None);

    // Should have one cohort with [0] and another with [1, 2]
    assert_eq!(cohort_indices.len(), 2);
    let has_genesis = cohort_indices.iter().any(|c| c == &HashSet::from([0]));
    let has_parallel = cohort_indices
        .iter()
        .any(|c| c.contains(&1) && c.contains(&2) && c.len() == 2);
    assert!(has_genesis);
    assert!(has_parallel);
}

#[test]
pub fn test_sub_braid_simple() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [1],
        3 => [2],
        4 => [2],  // parallel branch
    );

    let cohort = HashSet::from([1, 2, 3, 4]);
    let sub_parents = sub_braid(&cohort, &parents);

    // Should include nodes and their descendants
    assert_eq!(sub_parents.len(), 4);
    assert!(sub_parents.contains_key(&1));
    assert!(sub_parents.contains_key(&2));
    assert!(sub_parents.contains_key(&3));
    assert!(sub_parents.contains_key(&4));

    // Should not include parent 0
    assert!(!sub_parents.contains_key(&0));
}

#[test]
pub fn test_cohort_head_tail() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1, 2],
    );

    let children = reverse(&parents);
    let cohort = HashSet::from([1, 2, 3]);

    let head = cohort_head(&cohort, &parents, &children);
    let tail = cohort_tail(&cohort, &parents, &children);

    // Head should be genesis beads of the sub-braid (1 and 2)
    assert_eq!(head, HashSet::from([1, 2]));

    // Tail should be tip beads of the sub-braid (just 3)
    assert_eq!(tail, HashSet::from([3]));
}

#[test]
pub fn test_highest_work_path_simple() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1],
    );

    let children = reverse(&parents);
    let path = highest_work_path(&parents, &children, None).unwrap();

    // Should return one of the valid paths
    assert!(path == vec![0, 1, 3] || path == vec![0, 2]);
}

#[test]
pub fn test_check_cohort() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1, 2],
    );

    let children = reverse(&parents);

    // Single bead should always be a valid cohort
    let single_bead_cohort = HashSet::from([0]);
    assert!(check_cohort(&single_bead_cohort, &parents, &children));

    let single_bead_cohort_2 = HashSet::from([3]);
    assert!(check_cohort(&single_bead_cohort_2, &parents, &children));

    // This should be a valid cohort (connected subgraph)
    let valid_cohort = HashSet::from([1, 2, 3]);
    assert!(check_cohort(&valid_cohort, &parents, &children));
}

#[test]
pub fn test_descendant_work() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [1],
    );

    let children = reverse(&parents);

    // Create work HashMap manually since it needs BigUint values
    let work_values: HashMap<BeadIdx, BigUint> = HashMap::from([
        (0, BigUint::from(1u64)),
        (1, BigUint::from(2u64)),
        (2, BigUint::from(3u64)),
    ]);

    let descendant_work = descendant_work(&parents, &children, Some(&work_values), None);

    // 0: 1 + 2 + 3 = 6
    // 1: 2 + 3 = 5
    // 2: 3 = 3
    assert_eq!(descendant_work.get(&0), Some(&BigUint::from(6u64)));
    assert_eq!(descendant_work.get(&1), Some(&BigUint::from(5u64)));
    assert_eq!(descendant_work.get(&2), Some(&BigUint::from(3u64)));
}

#[test]
pub fn test_bead_cmp() {
    // Create BigUint values manually since the macro doesn't support custom types
    let work_values: HashMap<BeadIdx, BigUint> = HashMap::from([
        (0, BigUint::from(10u64)),
        (1, BigUint::from(20u64)),
        (2, BigUint::from(15u64)),
    ]);
    let awork_values: HashMap<BeadIdx, BigUint> = HashMap::from([
        (0, BigUint::from(10u64)),
        (1, BigUint::from(20u64)),
        (2, BigUint::from(15u64)),
    ]);

    // Test ordering - bead_cmp returns Result<Ordering, BraidError>
    assert_eq!(
        bead_cmp(1, 0, &work_values, &awork_values).unwrap(),
        std::cmp::Ordering::Greater
    );
    assert_eq!(
        bead_cmp(0, 1, &work_values, &awork_values).unwrap(),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        bead_cmp(0, 2, &work_values, &awork_values).unwrap(),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        bead_cmp(2, 0, &work_values, &awork_values).unwrap(),
        std::cmp::Ordering::Greater
    );
}

#[test]
pub fn test_generation() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1, 2],
    );

    let children = reverse(&parents);

    // Test generation from genesis beads
    let genesis_beads = HashSet::from([0]);
    let gen0 = generation(&genesis_beads, &children);
    assert_eq!(gen0, HashSet::from([1, 2]));

    // Test generation from middle beads
    let middle_beads = HashSet::from([1, 2]);
    let gen1 = generation(&middle_beads, &children);
    assert_eq!(gen1, HashSet::from([3]));

    // Test generation from tips (no children)
    let tip_beads = HashSet::from([3]);
    let gen2 = generation(&tip_beads, &children);
    assert_eq!(gen2, HashSet::new());
}

#[test]
pub fn test_check_cohort_ancestors() {
    let parents = relatives!(
        0 => [],
        1 => [0],
        2 => [0],
        3 => [1, 2],
    );

    let children = reverse(&parents);

    // Single bead should always pass check_cohort_ancestors
    let single_cohort = HashSet::from([0]);
    assert!(check_cohort_ancestors(&single_cohort, &parents, &children));

    let single_cohort_2 = HashSet::from([3]);
    assert!(check_cohort_ancestors(
        &single_cohort_2,
        &parents,
        &children
    ));

    // Connected subgraph should pass
    let connected_cohort = HashSet::from([1, 2, 3]);
    assert!(check_cohort_ancestors(
        &connected_cohort,
        &parents,
        &children
    ));
}

/// ****
/// File-based tests for algorithm functions
/// ****

// JSONBraid structure for loading test data from JSON files with HashSet for algorithm compatibility
#[derive(Clone, Debug, Deserialize)]
struct JSONBraid {
    pub description: String,
    pub parents: HashMap<BeadIdx, HashSet<BeadIdx>>,
    pub children: HashMap<BeadIdx, HashSet<BeadIdx>>,
    pub geneses: HashSet<BeadIdx>,
    pub tips: HashSet<BeadIdx>,
    pub cohorts: Vec<HashSet<BeadIdx>>,
    // this is populated in the test files but always 1. TODO: improve tests with different work
    // per bead to further exercise hwpath and descendant_work
    #[allow(unused)]
    pub bead_work: HashMap<BeadIdx, u32>,
    pub work: HashMap<BeadIdx, u32>,
    pub highest_work_path: Vec<BeadIdx>,
}

impl JSONBraid {
    /// Load and convert from JSON file to HashSet format
    /// Panics if the file cannot be loaded, with a clear error message including the filename
    pub fn load(file_path: &str) -> Self {
        let file_content = std::fs::read_to_string(file_path)
            .unwrap_or_else(|e| panic!("Failed to read test file '{}': {}", file_path, e));
        serde_json::from_str(&file_content)
            .unwrap_or_else(|e| panic!("Failed to parse JSON in test file '{}': {}", file_path, e))
    }

    /// Returns an iterator over all JSONBraids in the test directory
    pub fn tests() -> Box<dyn Iterator<Item = (JSONBraid, String)>> {
        // Get the project root directory from Cargo's environment variable
        let project_root = env!("CARGO_MANIFEST_DIR");
        let test_dir = format!("{}/../{}", project_root, BRAID_TEST_DIR);

        let dir_entries = fs::read_dir(&test_dir)
            .unwrap_or_else(|e| panic!("Failed to read test directory '{}': {}", test_dir, e));

        // Collect all JSON test files first
        let test_files: Vec<(JSONBraid, String)> = dir_entries
            .filter_map(|entry| {
                let entry = entry.unwrap_or_else(|e| panic!("Failed to read directory: {:?}", e));
                let path = entry.path();

                // Only include JSON files
                if path.extension().and_then(|s| s.to_str()) == Some("json") {
                    let file_path = path
                        .to_str()
                        .expect("Cannot stringify file path (Invalid UTF-8?)");
                    let filename = path
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string();
                    Some((JSONBraid::load(file_path), filename))
                } else {
                    None
                }
            })
            .collect();

        // Panic if no test files were found
        if test_files.is_empty() {
            panic!("No JSON test files found in directory '{}'", test_dir);
        }

        Box::new(test_files.into_iter())
    }
}

#[test]
pub fn test_genesis_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();

        let computed_genesis = geneses(&parents);
        let expected_genesis = file_braid.geneses.clone();

        assert_eq!(
            computed_genesis, expected_genesis,
            "Genesis mismatch in file '{}' [{}]",
            filename, file_braid.description
        );
    }
}

#[test]
pub fn test_tips_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let children = reverse(&parents);

        let computed_tips = tips(&parents, &children);
        let expected_tips = file_braid.tips.clone();

        assert_eq!(
            computed_tips, expected_tips,
            "Tips mismatch in file '{}' [{}]",
            filename, file_braid.description
        );
    }
}

#[test]
pub fn test_reverse_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let computed_children = reverse(&parents);
        let expected_children = file_braid.children.clone();

        assert_eq!(
            computed_children, expected_children,
            "Reverse mismatch in file '{}' [{}]",
            filename, file_braid.description
        );
    }
}

#[test]
pub fn test_cohorts_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let children = reverse(&parents);
        let computed_cohorts = cohorts(&parents, &children, None);
        let expected_cohorts = file_braid.cohorts.clone();

        // Sort both for comparison
        computed_cohorts.iter().for_each(|c| {
            let mut sorted_vec: Vec<BeadIdx> = c.iter().copied().collect();
            sorted_vec.sort();
        });
        expected_cohorts.iter().for_each(|c| {
            let mut sorted_vec: Vec<BeadIdx> = c.iter().copied().collect();
            sorted_vec.sort();
        });

        // The algorithm must produce EXACT results matching the JSON test cases
        assert_eq!(
            computed_cohorts, expected_cohorts,
            "Cohorts mismatch in file '{}' [{}] (expected: {:?}, got: {:?})",
            filename, file_braid.description, expected_cohorts, computed_cohorts
        );
    }
}

#[test]
pub fn test_highest_work_path_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let children = reverse(&parents);

        // Create bead work maps from file data
        let bead_work: HashMap<BeadIdx, BigUint> = file_braid
            .bead_work
            .iter()
            .map(|(k, v)| (*k, BigUint::from(*v)))
            .collect();

        let computed_path = highest_work_path(&parents, &children, Some(bead_work));

        // The algorithm must produce EXACT results matching the JSON test cases
        let path = computed_path.unwrap();
        assert_eq!(
            path, file_braid.highest_work_path,
            "Highest work path mismatch in file '{}' [{}] (expected: {:?}, got: {:?})",
            filename, file_braid.description, file_braid.highest_work_path, path
        );
    }
}

#[test]
pub fn test_descendant_work_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let children = reverse(&parents);

        // Create work maps from file data
        let work: HashMap<BeadIdx, BigUint> = file_braid
            .work
            .iter()
            .map(|(k, v)| (*k, BigUint::from(*v)))
            .collect();

        let computed_descendant_work = descendant_work(&parents, &children, Some(&work), None);

        // Verify that all beads have work calculations
        for (bead_idx, _) in &parents {
            assert!(
                computed_descendant_work.contains_key(bead_idx),
                "Missing descendant work for bead {}",
                bead_idx
            );

            // Descendant work should be at least the bead's own work
            let zero_work = BigUint::from(0u64);
            let bead_work = work.get(bead_idx).unwrap_or(&zero_work);
            let desc_work = computed_descendant_work.get(bead_idx).unwrap();
            assert!(
                desc_work >= bead_work,
                "Descendant work in file '{}' [{}] should be >= bead work for bead {}",
                filename,
                file_braid.description,
                bead_idx
            );
        }
    }
}

#[test]
pub fn test_cohort_head_tail_from_files() {
    for (file_braid, filename) in JSONBraid::tests() {
        let parents = file_braid.parents.clone();
        let children = reverse(&parents);

        // Test each cohort from the file
        for cohort in &file_braid.cohorts {
            if !cohort.is_empty() {
                let head = cohort_head(&cohort, &parents, &children);
                let tail = cohort_tail(&cohort, &parents, &children);

                // Head should be non-empty for valid cohorts
                assert!(
                    !head.is_empty(),
                    "Empty head for cohort {:?} in file: {} [{}]",
                    cohort,
                    filename,
                    file_braid.description
                );

                // Tail should be non-empty for valid cohorts
                assert!(
                    !tail.is_empty(),
                    "Empty tail for cohort {:?} in file: {} [{}]",
                    cohort,
                    filename,
                    file_braid.description
                );

                // Head should consist of beads in the cohort
                assert!(
                    head.iter().all(|bead| cohort.contains(bead)),
                    "Head contains beads not in cohort for file: {} [{}]",
                    filename,
                    file_braid.description
                );

                // Tail should consist of beads in the cohort
                assert!(
                    tail.iter().all(|bead| cohort.contains(bead)),
                    "Tail contains beads not in cohort for file: {} [{}]",
                    filename,
                    file_braid.description
                );
            }
        }
    }
}
