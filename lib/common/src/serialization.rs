pub trait Writable {

    fn serialize(&self) -> Vec<u8>;
    
}

pub trait Readable {

    fn deserialize(buffer: &[u8]) -> Result<Self, String> where Self: Sized;
    
}