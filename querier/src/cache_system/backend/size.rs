use std::{
    fmt::Debug,
    ops::{Add, Sub},
};

/// Strongly-typed memory size.
///
/// Can be used to represent in-RAM memory as well as on-disc memory.
pub trait MemorySize:
    Add<Output = Self> + Copy + Debug + PartialOrd + Send + Sub<Output = Self> + 'static
{
    /// Create memory size of zero.
    fn zero() -> Self;
}

pub trait MemorySizeProvider: Debug + Send + Sync + 'static {
    /// Cache key.
    type K;

    /// Cached value.
    type V;

    /// Size that can be estimated.
    type S;

    /// Estimate size of given key-value pair.
    fn size(&self, k: &Self::K, v: &Self::V) -> Self::S;
}
