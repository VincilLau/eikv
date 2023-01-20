use std::{cmp::Ordering, sync::Arc};

pub trait Comparator {
    fn name(&self) -> &'static str;
    fn compare(&self, key1: &[u8], key2: &[u8]) -> Ordering;
}

pub struct DefaultComparator;

impl DefaultComparator {
    pub fn new() -> Arc<dyn Comparator> {
        Arc::new(DefaultComparator)
    }
}

impl Comparator for DefaultComparator {
    fn name(&self) -> &'static str {
        "eikv.default_comparator.v1"
    }

    fn compare(&self, key1: &[u8], key2: &[u8]) -> Ordering {
        key1.cmp(key2)
    }
}
