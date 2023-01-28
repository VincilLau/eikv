use std::error::Error;

pub trait Key: Clone + Ord + Send + Sync {
    fn encode(self) -> Result<Vec<u8>, Box<dyn Error>>;
    fn decode(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>>;
}

impl Key for Vec<u8> {
    fn encode(self) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(self)
    }

    fn decode(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        Ok(bytes)
    }
}

impl Key for String {
    fn encode(self) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(self.into_bytes())
    }

    fn decode(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let s = String::from_utf8(bytes)?;
        Ok(s)
    }
}
