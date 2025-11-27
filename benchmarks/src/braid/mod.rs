pub mod benchmark_types;
pub mod simple_generator;

// Re-export for benchmark use
pub use benchmark_types::{SimpleNetwork, SimpleNode, Transmission, BeadIdx, NodeId, ParentMap};