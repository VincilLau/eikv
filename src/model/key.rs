use std::error::Error;

pub trait Key: Clone + Ord {
    fn into_vec_u8(self) -> Result<Vec<u8>, Box<dyn Error>>;
    fn from_vec_u8(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>>;
}

impl Key for Vec<u8> {
    fn into_vec_u8(self) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(self)
    }

    fn from_vec_u8(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        Ok(bytes)
    }
}

impl Key for String {
    fn into_vec_u8(self) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(self.into_bytes())
    }

    fn from_vec_u8(bytes: Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let s = String::from_utf8(bytes)?;
        Ok(s)
    }
}
