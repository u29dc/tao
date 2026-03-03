use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;

use thiserror::Error;

/// Capacity policy for the rendered HTML cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenderCachePolicy {
    max_entries: NonZeroUsize,
}

impl RenderCachePolicy {
    /// Build a cache policy with a strict max entry count.
    pub fn new(max_entries: usize) -> Result<Self, RenderCachePolicyError> {
        let max_entries = NonZeroUsize::new(max_entries)
            .ok_or(RenderCachePolicyError::ZeroCapacity { max_entries })?;
        Ok(Self { max_entries })
    }

    /// Return maximum cached entry count.
    #[must_use]
    pub fn max_entries(&self) -> usize {
        self.max_entries.get()
    }
}

/// Error returned by invalid cache policy inputs.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RenderCachePolicyError {
    /// Capacity must be non-zero.
    #[error("render cache max_entries must be greater than zero (got {max_entries})")]
    ZeroCapacity {
        /// Provided capacity value.
        max_entries: usize,
    },
}

/// Result of inserting a rendered HTML entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheInsertOutcome {
    /// Inserted a new cache entry without eviction.
    Inserted,
    /// Updated existing cache entry.
    Updated,
    /// Inserted a new entry and evicted least-recently-used key.
    InsertedAndEvicted {
        /// Evicted content hash key.
        evicted_key: String,
    },
}

/// Hash-keyed rendered markdown HTML cache with LRU eviction.
#[derive(Debug, Clone)]
pub struct RenderedHtmlCache {
    policy: RenderCachePolicy,
    entries: HashMap<String, String>,
    lru: VecDeque<String>,
}

impl RenderedHtmlCache {
    /// Build an empty rendered HTML cache.
    #[must_use]
    pub fn new(policy: RenderCachePolicy) -> Self {
        Self {
            policy,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    /// Return active cache policy.
    #[must_use]
    pub fn policy(&self) -> RenderCachePolicy {
        self.policy
    }

    /// Return current entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return true when cache has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Fetch rendered HTML by hash key and mark key as recently used.
    pub fn get(&mut self, hash_key: &str) -> Option<&str> {
        if self.entries.contains_key(hash_key) {
            self.touch(hash_key);
        }
        self.entries.get(hash_key).map(String::as_str)
    }

    /// Insert rendered HTML by hash key and apply eviction policy.
    pub fn insert(&mut self, hash_key: String, rendered_html: String) -> CacheInsertOutcome {
        if self.entries.contains_key(&hash_key) {
            self.entries.insert(hash_key.clone(), rendered_html);
            self.touch(&hash_key);
            return CacheInsertOutcome::Updated;
        }

        if self.entries.len() == self.policy.max_entries() {
            let evicted_key = self
                .lru
                .pop_front()
                .expect("lru queue cannot be empty when entries are at capacity");
            self.entries.remove(&evicted_key);
            self.entries.insert(hash_key.clone(), rendered_html);
            self.lru.push_back(hash_key);
            return CacheInsertOutcome::InsertedAndEvicted { evicted_key };
        }

        self.entries.insert(hash_key.clone(), rendered_html);
        self.lru.push_back(hash_key);
        CacheInsertOutcome::Inserted
    }

    /// Build a deterministic hash key from markdown content.
    #[must_use]
    pub fn hash_key(markdown: &str) -> String {
        blake3::hash(markdown.as_bytes()).to_hex().to_string()
    }

    fn touch(&mut self, hash_key: &str) {
        if let Some(position) = self.lru.iter().position(|key| key == hash_key) {
            self.lru.remove(position);
        }
        self.lru.push_back(hash_key.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheInsertOutcome, RenderCachePolicy, RenderCachePolicyError, RenderedHtmlCache};

    #[test]
    fn policy_rejects_zero_capacity() {
        let error = RenderCachePolicy::new(0).expect_err("zero capacity should fail");
        assert_eq!(
            error,
            RenderCachePolicyError::ZeroCapacity { max_entries: 0 }
        );
    }

    #[test]
    fn cache_returns_hit_for_existing_hash() {
        let policy = RenderCachePolicy::new(2).expect("policy");
        let mut cache = RenderedHtmlCache::new(policy);

        let key = RenderedHtmlCache::hash_key("# hello");
        let outcome = cache.insert(key.clone(), "<h1>hello</h1>".to_string());
        assert_eq!(outcome, CacheInsertOutcome::Inserted);

        let cached = cache.get(&key).expect("cache hit");
        assert_eq!(cached, "<h1>hello</h1>");
    }

    #[test]
    fn cache_evicts_least_recently_used_entry() {
        let policy = RenderCachePolicy::new(2).expect("policy");
        let mut cache = RenderedHtmlCache::new(policy);

        let first = RenderedHtmlCache::hash_key("first");
        let second = RenderedHtmlCache::hash_key("second");
        let third = RenderedHtmlCache::hash_key("third");

        cache.insert(first.clone(), "<p>first</p>".to_string());
        cache.insert(second.clone(), "<p>second</p>".to_string());

        // Touch first so second becomes LRU.
        let _ = cache.get(&first);

        let outcome = cache.insert(third.clone(), "<p>third</p>".to_string());
        assert_eq!(
            outcome,
            CacheInsertOutcome::InsertedAndEvicted {
                evicted_key: second.clone()
            }
        );

        assert!(cache.get(&second).is_none());
        assert!(cache.get(&first).is_some());
        assert!(cache.get(&third).is_some());
    }

    #[test]
    fn cache_updates_existing_entry_without_eviction() {
        let policy = RenderCachePolicy::new(1).expect("policy");
        let mut cache = RenderedHtmlCache::new(policy);

        let key = RenderedHtmlCache::hash_key("doc");
        cache.insert(key.clone(), "<p>v1</p>".to_string());

        let outcome = cache.insert(key.clone(), "<p>v2</p>".to_string());
        assert_eq!(outcome, CacheInsertOutcome::Updated);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&key), Some("<p>v2</p>"));
    }
}
