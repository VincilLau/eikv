use std::error::Error;

pub trait Filter {
    fn add(&mut self, key: &[u8]);
    fn may_match(&self) -> bool;
    fn append_to(&self, buf: &mut Vec<u8>) -> Result<(), Box<dyn Error>>;
}

pub trait FilterFactory {
    fn name(&self) -> &'static str;
    fn create(&self) -> Box<dyn Filter>;
    fn parse(&self, buf: &[u8]) -> Result<Box<dyn Filter>, Box<dyn Error>>;
}
