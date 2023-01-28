use std::error::Error;

pub trait Filter {
    fn add(&mut self, key: &[u8]);
    fn may_match(&self, key: &[u8]) -> bool;
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), Box<dyn Error>>;
}

pub trait FilterFactory: Send + Sync {
    fn create(&self) -> Box<dyn Filter>;
    fn decode(&self, buf: &[u8]) -> Result<Box<dyn Filter>, Box<dyn Error>>;
}
