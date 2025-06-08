use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use linked_hash_map::LinkedHashMap;

use crate::metadata::TableBucket;

pub struct FairBucketStatusMap<S> {
    map: LinkedHashMap<TableBucket, Arc<S>>,
    size: AtomicUsize,
}

impl<S> FairBucketStatusMap<S> {
    pub fn new() -> Self {
        Self {
            map: LinkedHashMap::new(),
            size: AtomicUsize::new(0),
        }
    }

    /// Moves the bucket to the end of the iteration order
    pub fn move_to_end(&mut self, table_bucket: TableBucket)
    where
        TableBucket: Eq + Hash,
        S: Clone,
    {
        if let Some(status) = self.map.remove(&table_bucket) {
            self.map.insert(table_bucket, status);
        }
    }

    /// Updates the status and moves the bucket to the end
    pub fn update_and_move_to_end(&mut self, table_bucket: TableBucket, status: S)
    where
        TableBucket: Eq + Hash,
    {
        self.map.remove(&table_bucket);
        self.map.insert(table_bucket, Arc::new(status));
        self.update_size();
    }

    /// Updates the status without changing the order
    pub fn update(&mut self, table_bucket: TableBucket, status: Arc<S>)
    where
        TableBucket: Eq + Hash,
    {
        self.map.insert(table_bucket, status);
        self.update_size();
    }

    /// Removes a bucket
    pub fn remove(&mut self, table_bucket: &TableBucket)
    where
        TableBucket: Eq + Hash,
    {
        self.map.remove(table_bucket);
        self.update_size();
    }

    /// Returns an immutable view of all buckets
    pub fn bucket_set(&self) -> HashSet<&TableBucket>
    where
        TableBucket: Eq + Hash,
    {
        self.map.keys().collect()
    }

    /// Clears all buckets
    pub fn clear(&mut self) {
        self.map.clear();
        self.update_size();
    }

    /// Checks if a bucket exists
    pub fn contains(&self, table_bucket: &TableBucket) -> bool
    where
        TableBucket: Eq + Hash,
    {
        self.map.contains_key(table_bucket)
    }

    /// Returns an immutable view of the bucket-status map
    pub fn bucket_status_map(&self) -> &LinkedHashMap<TableBucket, Arc<S>> {
        &self.map
    }

    /// Returns status values in current order
    pub fn bucket_status_values(&self) -> Vec<&Arc<S>> {
        self.map.values().collect()
    }

    /// Gets the status for a bucket
    pub fn status_value(&self, table_bucket: &TableBucket) -> Option<&Arc<S>>
    where
        TableBucket: Eq + Hash,
    {
        self.map.get(table_bucket)
    }

    /// Applies a function to each bucket-status pair
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&TableBucket, &S),
    {
        for (bucket, status) in &self.map {
            f(bucket, status);
        }
    }

    /// Gets the current bucket count (thread-safe)
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn set(&mut self, bucket_to_status: HashMap<TableBucket, Arc<S>>)
    where
        TableBucket: Eq + Hash + Clone,
        S: Clone,
    {
        self.map.clear();

        // Group buckets by table ID
        let mut table_to_buckets: LinkedHashMap<i64, Vec<TableBucket>> = LinkedHashMap::new();
        for bucket in bucket_to_status.keys() {
            table_to_buckets
                .entry(bucket.table_id())
                .or_default()
                .push(bucket.clone());
        }

        // Insert buckets grouped by table
        for (_, buckets) in table_to_buckets {
            for bucket in buckets {
                if let Some(status) = bucket_to_status.get(&bucket) {
                    self.map.insert(bucket, status.clone());
                }
            }
        }

        self.update_size();
    }

    fn update_size(&mut self) {
        self.size.store(self.map.len(), Ordering::Relaxed);
    }
}

impl<S> Default for FairBucketStatusMap<S> {
    fn default() -> Self {
        Self::new()
    }
}
