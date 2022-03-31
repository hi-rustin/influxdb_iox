//! LRU (Least Recently Used) cache system.
//!
//! # Internals
//! Here we describe the internals of the LRU cache system.
//!
//! ## Requirements
//! To understand the construction, we first must understand what the LRU system tries to achieve:
//!
//! - **Single Pool:** Have a single memory pool for multiple LRU backends.
//! - **Eviction Cascade:** Adding data any of the backends (or modifying an existing entry) should check if there is
//!   enough space left in the LRU backend. If not, we must remove the lest recently used entries over all backends
//!   (including the one that just got a new entry) until there is enough space.
//!
//! This has the following consequences:
//!
//! - **Cyclic Structure:** The LRU backends communicate with the pool, but the pool also neeeds to communicate with
//!   all the backends. This creates some form of cyclic data structure.
//! - **Type Erasure:** The pool is only specific to the memory size type, not the key and value types of the
//!   participating backends. So at some place we need to perform type erasure.
//!
//! ## Data Structures
//!
//! ```text
//!               .~~~~~~~~~~~~.            .~~~~~~~~~~~~~~~~~.
//! ------------->: MemoryPool :--(mutex)-->: MemoryPoolInner :-----------------------------------+
//!               :    <S>     :            :       <S>       :                                   |
//!               .~~~~~~~~~~~~.            .~~~~~~~~~~~~~~~~~.                                   |
//!                   ^                                                                           |
//!                   |                                                                           |
//!                 (arc)                                                                         |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |  : LruBackendInner :<--: PoolMemberGuardImpl :<-(dyn)-: PoolMemberGuard : |
//!                   |  : <K1, V1, S>     :   :     <K1, V1, S>     :        :       <S>       : |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |        ^                           ^                           ^          |
//!                   |        |                           |                           |          |
//!                   |        |                           +-------------+-------------+          |
//!                   |        |                                    (call lock)                   |
//!                   |        |                           +-------------+-------------+          |
//!                   |     (mutex)                        |                           |          |
//!   .~~~~~~~~~~~~~. |        |                   .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//! ->: LruBackend  :-+      (arc)                 : PoolMemberImpl :           : PoolMember :<---+
//!   : <K1, V1, S> : |        |                   :   <K1, V1, S>  :           :    <S>     :    |
//!   :             :----------+-------------------:                :<--(dyn)---:            :    |
//!   .~~~~~~~~~~~~~. |                            .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |                                                                           |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |  : LruBackendInner :<--: PoolMemberGuardImpl :<-(dyn)-: PoolMemberGuard : |
//!                   |  : <K2, V2, S>     :   :     <K2, V2, S>     :        :       <S>       : |
//!                   |  .~~~~~~~~~~~~~~~~~.   .~~~~~~~~~~~~~~~~~~~~~.        .~~~~~~~~~~~~~~~~~. |
//!                   |        ^                           ^                           ^          |
//!                   |        |                           |                           |          |
//!                   |        |                           +-------------+-------------+          |
//!                   |        |                                    (call lock)                   |
//!                   |        |                           +-------------+-------------+          |
//!                   |     (mutex)                        |                           |          |
//!   .~~~~~~~~~~~~~. |        |                   .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.    |
//! ->: LruBackend  :-+      (arc)                 : PoolMemberImpl :           : PoolMember :<---+
//!   : <K2, V2, S> :          |                   :   <K2, V2, S>  :           :    <S>     :
//!   :             :----------+-------------------:                :<--(dyn)---:            :
//!   .~~~~~~~~~~~~~.                              .~~~~~~~~~~~~~~~~.           .~~~~~~~~~~~~.
//! ```
//!
//! ## Locking
//! What and how we lock depends on the operation.
//!
//! Note that all locks are bare mutexes, there are no read-write-locks. "Only read" is not really an important use
//! case since even `get` requires updating the "last used" timestamp of the corresponding entry.
//!
//! ### Get
//! For [`get`](CacheBackend::get) we only need to update the "last used" timestamp for the affected entry. No
//! pool-wide operations are required. We just [`LruBackendInner`] and perform the read operation of the inner backend
//! and the modification of the "last used" timestamp.
//!
//! ### Remove
//! For [`remove`](CacheBackend::remove) the pool usage can only decrease, so other backends are never affected. We
//! first lock [`MemoryPoolInner`], then [`LruBackendInner`] and then perform the modification on both.
//!
//! ### Set
//! [`set`](CacheBackend::set) is the most complex operation and requires a bit of a lock dance:
//!
//! 1. Lock [`MemoryPoolInner`]
//! 2. Lock [`LruBackendInner`]
//! 3. Check if the entry already exists and remove it.
//! 4. Drop lock of [`LruBackendInner`] so that the pool can use it to free up space.
//! 5. Request to add more data to the pool:
//!    1. Check if we need to free up space, otherwise we can already procede to step 6.
//!    2. Lock all pool members ([`PoolMember::lock`] which ultimately locks [`LruBackendInner`])
//!    3. Loop:
//!       1. Ask pool members if they have anything to free.
//!       2. Pick least recently used result and free it
//!    4. Drop locks of [`LruBackendInner`]
//! 6. Lock [`LruBackendInner`]
//! 7. Drop lock of [`LruBackendInner`] and [`MemoryPoolInner`]
//!
//! The global locks in step 5.2 are required so that the reads in step 5.3.1 and the resulting actions in step 5.3.2
//! are consistent. Otherwise an interleaved `get` request might invalidate the results.
use std::{
    any::Any,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    hash::Hash,
    ops::Deref,
    sync::Arc,
};

use parking_lot::{Mutex, MutexGuard};
use time::{Time, TimeProvider};

use super::{
    addressable_heap::AddressableHeap,
    size::{MemorySize, MemorySizeProvider},
    CacheBackend,
};

/// Inner state of [`MemoryPool`] which is always behind a mutex.
#[derive(Debug)]
struct MemoryPoolInner<S>
where
    S: MemorySize,
{
    /// Memory limit.
    limit: S,

    /// Current memory usage.
    current: S,

    /// Members (= backends) that use this pool.
    members: BTreeMap<String, Box<dyn PoolMember<S = S>>>,
}

impl<S> MemoryPoolInner<S>
where
    S: MemorySize,
{
    /// Register new pool member.
    ///
    /// # Panic
    /// Panics when a member with the specific ID is already registered.
    fn register_member(&mut self, id: String, member: Box<dyn PoolMember<S = S>>) {
        match self.members.entry(id) {
            Entry::Vacant(v) => {
                v.insert(member);
            }
            Entry::Occupied(o) => {
                panic!("Member '{}' already registered", o.key());
            }
        }
    }

    /// Unregister pool member.
    ///
    /// # Panic
    /// Panics when the member with the specified ID is unknown (or was already unregistered).
    fn unregister_member(&mut self, id: &str) {
        assert!(self.members.remove(id).is_some(), "Member '{}' unknown", id);
    }

    /// Add used memory too pool.
    fn add(&mut self, s: S) {
        self.current = self.current + s;

        if self.current > self.limit {
            // lock all members
            let mut members: Vec<_> = self.members.values().map(|member| member.lock()).collect();

            // evict data until we are below the limit
            while self.current > self.limit {
                let mut options: Vec<_> = members
                    .iter_mut()
                    .filter_map(|member| member.could_remove().map(|t| (t, member)))
                    .collect();
                options.sort_by_key(|(t, _member)| *t);

                let (_t, member) = options.first_mut().expect("accounting out of sync");
                let s = member.remove_oldest();
                self.current = self.current - s;
            }
        }
    }

    /// Remove used memory from pool.
    fn remove(&mut self, s: S) {
        self.current = self.current - s;
    }
}

/// Memory pool.
///
/// This can be used with [`LruBackend`].
#[derive(Debug)]
pub struct MemoryPool<S>
where
    S: MemorySize,
{
    inner: Mutex<MemoryPoolInner<S>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl<S> MemoryPool<S>
where
    S: MemorySize,
{
    /// Creates new empty memory pool with given limit.
    pub fn new(limit: S, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            inner: Mutex::new(MemoryPoolInner {
                limit,
                current: S::zero(),
                members: BTreeMap::new(),
            }),
            time_provider,
        }
    }

    /// Get current pool usage.
    pub fn current(&self) -> S {
        self.inner.lock().current
    }
}

/// Inner state of [`LruBackend`].
///
/// This is used by [`LruBackend`] directly but also by [`PoolMemberImpl`] to add it to a [`MemoryPool`]/[`MemoryPoolInner`].
#[derive(Debug)]
struct LruBackendInner<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
    last_used: AddressableHeap<K, S, Time>,
}

/// [Cache backend](CacheBackend) that wraps another backend and limits its memory usage.
#[derive(Debug)]
pub struct LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    id: String,
    inner: Arc<Mutex<LruBackendInner<K, V, S>>>,
    pool: Arc<MemoryPool<S>>,
    size_provider: Arc<dyn MemorySizeProvider<K = K, V = V, S = S>>,
}

impl<K, V, S> LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    /// Create new backend w/o any known keys.
    ///
    /// The inner backend MUST NOT contain any data at this point, otherwise we will not track any size for these entries.
    ///
    /// # Panic
    /// - Panics if the given ID is already used within the given pool.
    /// - If the inner backend is not empty.
    pub fn new(
        inner_backend: Box<dyn CacheBackend<K = K, V = V>>,
        pool: Arc<MemoryPool<S>>,
        id: String,
        size_provider: Arc<dyn MemorySizeProvider<K = K, V = V, S = S>>,
    ) -> Self {
        assert!(inner_backend.is_empty(), "inner backend is not empty");

        let inner = Arc::new(Mutex::new(LruBackendInner {
            inner_backend,
            last_used: AddressableHeap::new(),
        }));

        pool.inner.lock().register_member(
            id.clone(),
            Box::new(PoolMemberImpl {
                inner: Arc::clone(&inner),
            }),
        );

        Self {
            id,
            inner,
            pool,
            size_provider,
        }
    }

    /// Get underlying / inner backend.
    pub fn inner_backend(&self) -> LruBackendInnerBackendHandle<'_, K, V, S> {
        LruBackendInnerBackendHandle {
            inner: self.inner.lock(),
        }
    }
}

impl<K, V, S> Drop for LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    fn drop(&mut self) {
        self.pool.inner.lock().unregister_member(&self.id);
    }
}

impl<K, V, S> CacheBackend for LruBackend<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        let mut inner = self.inner.lock();

        match inner.inner_backend.get(k) {
            Some(v) => {
                // update "last used"
                let now = self.pool.time_provider.now();
                let (size, _last_used) = inner
                    .last_used
                    .remove(k)
                    .expect("backend and last-used table out of sync");
                inner.last_used.insert(k.clone(), size, now);

                Some(v)
            }
            None => None,
        }
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        // determine all attributes before getting any locks
        let size = self.size_provider.size(&k, &v);
        let now = self.pool.time_provider.now();

        // get locks
        let mut pool = self.pool.inner.lock();

        // check for oversized entries
        if size > pool.limit {
            return;
        }

        // maybe clean from pool
        {
            let mut inner = self.inner.lock();
            if let Some((size, _last_used)) = inner.last_used.remove(&k) {
                pool.remove(size);
            }
        }

        // pool-wide operation
        // Since this may call back to this very backend to remove entries, we MUST NOT hold an inner lock at this point.
        pool.add(size);

        // add new entry to inner backend AFTER adding it to the pool, so we are never overcommitting resources.
        let mut inner = self.inner.lock();
        inner.inner_backend.set(k.clone(), v);
        inner.last_used.insert(k, size, now);
    }

    fn remove(&mut self, k: &Self::K) {
        let mut pool = self.pool.inner.lock();
        let mut inner = self.inner.lock();

        inner.inner_backend.remove(k);
        if let Some((size, _last_used)) = inner.last_used.remove(k) {
            pool.remove(size);
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.lock().last_used.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

/// A member of a [`MemoryPool`]/[`MemoryPoolInner`].
///
/// Must be [locked](Self::lock) to gain access.
///
/// The only implementation of this is [`PoolMemberImpl`]. This indiction is required to erase `K` and `V` from specific
/// backend so we can stick it into the generic pool.
trait PoolMember: Debug + Send + 'static {
    /// Memory size type.
    type S;

    /// Lock pool member.
    fn lock(&self) -> Box<dyn PoolMemberGuard<S = Self::S> + '_>;
}

/// The only implementation of [`PoolMember`].
///
/// In constast to the trait, this still contains `K` and `V`.
#[derive(Debug)]
pub struct PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    inner: Arc<Mutex<LruBackendInner<K, V, S>>>,
}

impl<K, V, S> PoolMember for PoolMemberImpl<K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    type S = S;

    fn lock(&self) -> Box<dyn PoolMemberGuard<S = Self::S> + '_> {
        Box::new(PoolMemberGuardImpl {
            inner: self.inner.lock(),
        })
    }
}

/// Locked [`MemoryPool`]/[`MemoryPoolInner`] member.
///
/// The only implementation of this is [`PoolMemberGuardImpl`]. This indiction is required to erase `K` and `V` from
/// specific backend so we can stick it into the generic pool.
trait PoolMemberGuard: Debug {
    /// Memory size type.
    type S;

    /// Check if this member has anything that could be removed. If so, return the "last used" timestamp of the oldest
    /// entry.
    fn could_remove(&self) -> Option<Time>;

    /// Remove oldest entry and return size of the removed entry.
    ///
    /// # Panic
    /// This must only be used if [`could_remove`](Self::could_remove) was used to check if there is anything to check
    /// if there is an entry that could be removed. Panics if this is not the case.
    fn remove_oldest(&mut self) -> Self::S;
}

/// The only implementation of [`PoolMemberGuard`].
///
/// In constast to the trait, this still contains `K` and `V`.
#[derive(Debug)]
pub struct PoolMemberGuardImpl<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    inner: MutexGuard<'a, LruBackendInner<K, V, S>>,
}

impl<'a, K, V, S> PoolMemberGuard for PoolMemberGuardImpl<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    type S = S;

    fn could_remove(&self) -> Option<Time> {
        self.inner.last_used.peek().map(|(_k, _s, t)| *t)
    }

    fn remove_oldest(&mut self) -> Self::S {
        let (k, s, _t) = self.inner.last_used.pop().expect("nothing to remove");
        self.inner.inner_backend.remove(&k);
        s
    }
}

/// Helper for [`LruBackend::inner_backend`].
#[derive(Debug)]
pub struct LruBackendInnerBackendHandle<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    inner: MutexGuard<'a, LruBackendInner<K, V, S>>,
}

impl<'a, K, V, S> Deref for LruBackendInnerBackendHandle<'a, K, V, S>
where
    K: Clone + Eq + Debug + Hash + Ord + Send + 'static,
    V: Clone + Debug + Send + 'static,
    S: MemorySize,
{
    type Target = dyn CacheBackend<K = K, V = V>;

    fn deref(&self) -> &Self::Target {
        self.inner.inner_backend.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::{Add, Sub},
        time::Duration,
    };

    use time::MockProvider;

    use super::*;

    #[test]
    #[should_panic(expected = "inner backend is not empty")]
    fn test_panic_inner_not_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        LruBackend::new(
            Box::new(HashMap::from([(String::from("foo"), 1usize)])),
            Arc::clone(&pool),
            String::from("id"),
            Arc::clone(&size_provider) as _,
        );
    }

    #[test]
    #[should_panic(expected = "Member 'id' already registered")]
    fn test_panic_id_collission() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let _backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id"),
            Arc::clone(&size_provider) as _,
        );
        let _backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id"),
            Arc::clone(&size_provider) as _,
        );
    }

    #[test]
    fn test_reregister_member() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id"),
            Arc::clone(&size_provider) as _,
        );
        drop(backend1);
        let _backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id"),
            Arc::clone(&size_provider) as _,
        );
    }

    #[test]
    fn test_empty() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        assert_eq!(pool.current().0, 0);

        let _backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );

        assert_eq!(pool.current().0, 0);
    }

    #[test]
    fn test_override() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.set(String::from("a"), 4usize);
        assert_eq!(pool.current().0, 7);
    }

    #[test]
    fn test_remove() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(10),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );

        backend.set(String::from("a"), 5usize);
        assert_eq!(pool.current().0, 5);

        backend.set(String::from("b"), 3usize);
        assert_eq!(pool.current().0, 8);

        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);

        assert_eq!(backend.get(&String::from("a")), None);
        assert_inner_backend(&backend, [(String::from("b"), 3)]);

        // removing it again should just work
        backend.remove(&String::from("a"));
        assert_eq!(pool.current().0, 3);
    }

    #[test]
    fn test_eviction_order() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(21),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let mut backend1 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );
        let mut backend2 = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id2"),
            Arc::clone(&size_provider) as _,
        );

        backend1.set(String::from("b"), 1usize);
        backend2.set(String::from("a"), 2usize);
        backend1.set(String::from("a"), 3usize);
        backend1.set(String::from("c"), 4usize);
        assert_eq!(pool.current().0, 10);

        time_provider.inc(Duration::from_millis(1));

        backend1.set(String::from("d"), 5usize);
        assert_eq!(pool.current().0, 15);

        time_provider.inc(Duration::from_millis(1));
        backend2.set(String::from("b"), 6usize);
        assert_eq!(pool.current().0, 21);

        time_provider.inc(Duration::from_millis(1));

        // now are exactly at capacity
        assert_inner_backend(
            &backend1,
            [
                (String::from("a"), 3),
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // adding a single element will drop the smallest key from the first backend (by ID)
        backend1.set(String::from("foo1"), 1usize);
        assert_eq!(pool.current().0, 19);
        assert_inner_backend(
            &backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // now we can fill up data up to the capacity again
        backend1.set(String::from("foo2"), 2usize);
        assert_eq!(pool.current().0, 21);
        assert_inner_backend(
            &backend1,
            [
                (String::from("b"), 1),
                (String::from("c"), 4),
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // can evict two keys at the same time
        backend1.set(String::from("foo3"), 2usize);
        assert_eq!(pool.current().0, 18);
        assert_inner_backend(
            &backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("a"), 2), (String::from("b"), 6)]);

        // can evict from another backend
        backend1.set(String::from("foo4"), 4usize);
        assert_eq!(pool.current().0, 20);
        assert_inner_backend(
            &backend1,
            [
                (String::from("d"), 5),
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
            ],
        );
        assert_inner_backend(&backend2, [(String::from("b"), 6)]);

        // can evict multiple timestamps
        backend1.set(String::from("foo5"), 7usize);
        assert_eq!(pool.current().0, 16);
        assert_inner_backend(
            &backend1,
            [
                (String::from("foo1"), 1),
                (String::from("foo2"), 2),
                (String::from("foo3"), 2),
                (String::from("foo4"), 4),
                (String::from("foo5"), 7),
            ],
        );
        assert_inner_backend(&backend2, []);
    }

    #[test]
    fn test_get_updates_last_used() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(6),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let mut backend = LruBackend::new(
            Box::new(HashMap::new()),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );

        backend.set(String::from("a"), 1usize);
        backend.set(String::from("b"), 2usize);

        time_provider.inc(Duration::from_millis(1));

        backend.set(String::from("c"), 3usize);

        time_provider.inc(Duration::from_millis(1));

        assert_eq!(backend.get(&String::from("a")), Some(1usize));

        assert_eq!(pool.current().0, 6);
        assert_inner_backend(
            &backend,
            [
                (String::from("a"), 1),
                (String::from("b"), 2),
                (String::from("c"), 3),
            ],
        );

        backend.set(String::from("foo"), 3usize);
        assert_eq!(pool.current().0, 4);
        assert_inner_backend(&backend, [(String::from("a"), 1), (String::from("foo"), 3)]);
    }

    #[test]
    fn test_oversized_entries_are_never_added() {
        #[derive(Debug)]
        struct PanicAllBackend {}

        impl CacheBackend for PanicAllBackend {
            type K = String;
            type V = usize;

            fn get(&mut self, _k: &Self::K) -> Option<Self::V> {
                panic!("should never be called")
            }

            fn set(&mut self, _k: Self::K, _v: Self::V) {
                panic!("should never be called")
            }

            fn remove(&mut self, _k: &Self::K) {
                panic!("should never be called")
            }

            fn is_empty(&self) -> bool {
                true
            }

            fn as_any(&self) -> &dyn Any {
                self as &dyn Any
            }
        }

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let pool = Arc::new(MemoryPool::new(
            TestSize(1),
            Arc::clone(&time_provider) as _,
        ));
        let size_provider = Arc::new(TestMemorySizeProvider {});

        let mut backend = LruBackend::new(
            Box::new(PanicAllBackend {}),
            Arc::clone(&pool),
            String::from("id1"),
            Arc::clone(&size_provider) as _,
        );

        backend.set(String::from("a"), 2usize);
        assert_eq!(pool.current().0, 0);
    }

    #[test]
    fn test_generic() {
        use crate::cache_system::backend::test_util::test_generic;

        #[derive(Debug)]
        struct ZeroSizeProvider {}

        impl MemorySizeProvider for ZeroSizeProvider {
            type K = u8;
            type V = String;
            type S = TestSize;

            fn size(&self, _k: &Self::K, _v: &Self::V) -> Self::S {
                TestSize(0)
            }
        }

        test_generic(|| {
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let pool = Arc::new(MemoryPool::new(
                TestSize(10),
                Arc::clone(&time_provider) as _,
            ));
            let size_provider = Arc::new(ZeroSizeProvider {});

            LruBackend::new(
                Box::new(HashMap::new()),
                Arc::clone(&pool),
                String::from("id"),
                Arc::clone(&size_provider) as _,
            )
        });
    }

    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    struct TestSize(usize);

    impl MemorySize for TestSize {
        fn zero() -> Self {
            Self(0)
        }
    }

    impl Add for TestSize {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_add(rhs.0).expect("overflow"))
        }
    }

    impl Sub for TestSize {
        type Output = Self;

        fn sub(self, rhs: Self) -> Self::Output {
            Self(self.0.checked_sub(rhs.0).expect("underflow"))
        }
    }

    #[derive(Debug)]
    struct TestMemorySizeProvider {}

    impl MemorySizeProvider for TestMemorySizeProvider {
        type K = String;
        type V = usize;
        type S = TestSize;

        fn size(&self, _k: &Self::K, v: &Self::V) -> Self::S {
            TestSize(*v)
        }
    }

    fn assert_inner_backend<const N: usize>(
        backend: &LruBackend<String, usize, TestSize>,
        data: [(String, usize); N],
    ) {
        let inner_backend = backend.inner_backend();
        let inner_backend = inner_backend
            .as_any()
            .downcast_ref::<HashMap<String, usize>>()
            .unwrap();
        let expected = HashMap::from(data);
        assert_eq!(inner_backend, &expected);
    }
}
