use std::sync::atomic::{AtomicI32, Ordering};
use rand::Rng;
use crate::cluster::Cluster;
use crate::metadata::TablePath;

pub trait BucketAssigner: Sync + Send {
    fn abort_if_batch_full(&self) -> bool;

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32);

    fn assign_bucket(&self, bucket_key: Option<&[u8]>, cluster: &Cluster) -> i32;
}

#[derive(Debug)]
pub struct StickyBucketAssigner {
    table_path: TablePath,
    current_bucket_id: AtomicI32,
}

impl StickyBucketAssigner {
    pub fn new(table_path: TablePath) -> Self {
        Self {
            table_path,
            current_bucket_id: AtomicI32::new(-1),
        }
    }

    fn next_bucket(&self, cluster: &Cluster, prev_bucket_id: i32) -> i32 {
        let old_bucket = self.current_bucket_id.load(Ordering::Relaxed);
        let mut new_bucket = old_bucket;
        if old_bucket < 0 || old_bucket == prev_bucket_id {
            let available_buckets = cluster.get_available_buckets_for_table_path(&self.table_path);
            if available_buckets.is_empty() {
                let mut rng = rand::rng();
                let mut random: i32 = rng.random();
                random &= i32::MAX;
                new_bucket = random % cluster.get_bucket_count(&self.table_path);
            } else if available_buckets.len() == 1 {
                new_bucket = available_buckets[0].table_bucket.bucket_id();
            } else {
                let mut rng = rand::rng();
                while new_bucket < 0 || new_bucket == old_bucket {
                    let mut random: i32 = rng.random();
                    random &= i32::MAX;
                    new_bucket = available_buckets
                        [(random % available_buckets.len() as i32) as usize]
                        .bucket_id();
                }
            }
        }

        if old_bucket < 0 {
            self.current_bucket_id.store(new_bucket, Ordering::Relaxed);
        } else {
            self.current_bucket_id
                .compare_and_swap(prev_bucket_id, new_bucket, Ordering::Relaxed);
        }
        self.current_bucket_id.load(Ordering::Relaxed)
    }
}

impl BucketAssigner for StickyBucketAssigner {
    fn abort_if_batch_full(&self) -> bool {
        true
    }

    fn on_new_batch(&self, cluster: &Cluster, prev_bucket_id: i32) {
        self.next_bucket(cluster, prev_bucket_id);
    }

    fn assign_bucket(&self, bucket_key: Option<&[u8]>, cluster: &Cluster) -> i32 {
        let bucket_id = self.current_bucket_id.load(Ordering::Relaxed);
        if bucket_id < 0 {
            self.next_bucket(cluster, bucket_id)
        } else {
            bucket_id
        }
    }
}