const BITS_PER_SEGMENT: u8 = 8;

#[derive(Debug, Clone)]
pub struct BitVector {
    num_bits: u64,
    vector: Vec<u8>,
}

impl BitVector {
    pub fn new(num_bits: u64) -> Self {
        let num_segments = number_of_segments(num_bits);
        Self {
            num_bits,
            vector: vec![0; num_segments as usize],
        }
    }

    pub fn from_bytes(num_bits: u64, vector: Vec<u8>) -> BitVector {
        Self {
            num_bits,
            vector,
        }
    }

    pub fn set_bit(&mut self, bit_index: u64) {
        if bit_index >= self.num_bits {
            return;
        }

        let (segment, position) = get_bit_coordinates(bit_index);
        if let Some(byte) = self.vector.get_mut(segment as usize) {
            *byte |= 1u8 << position;
        }
    }

    pub fn get_bit(&self, bit_index: u64) -> Option<u8> {
        if bit_index >= self.num_bits {
            return None;
        }

        let (segment, position) = get_bit_coordinates(bit_index);
        self.vector.get(segment as usize)
            .map(|byte| (byte >> position) & 1)
    }

    pub fn is_set(&self, bit_index: u64) -> bool {
        let Some(byte) = self.get_bit(bit_index) else {
            return false;
        };
        byte == 1
    }

    pub fn clear(&mut self, bit_index: u64) {
        let (segment, position) = get_bit_coordinates(bit_index);

        if segment as usize >= self.vector.len() {
            return;
        }

        self.vector[segment as usize] &= !(1u8 << position);
    }

    pub fn get_num_bits(&self) -> u64 {
        self.num_bits
    }

    pub fn vec_size(&self) -> usize {
        self.vector.len()
    }

    pub fn get_vector(&self) -> Vec<u8> {
        self.vector.clone()
    }

    pub fn clone(&self) -> BitVector {
        BitVector {
            num_bits: self.num_bits.clone(),
            vector: self.vector.clone(),
        }
    }
}

fn get_bit_coordinates(bit_index: u64) -> (u64, u64) {
    let segment = bit_index / BITS_PER_SEGMENT as u64;
    let position = bit_index & (BITS_PER_SEGMENT as u64 - 1);
    (segment, position)
}

fn number_of_segments(num_bits: u64) -> u64 {
    (num_bits + 7) / BITS_PER_SEGMENT as u64
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_number_of_segments() {
        assert_eq!(number_of_segments(3), 1);
        assert_eq!(number_of_segments(10), 2);
        assert_eq!(number_of_segments(1023), 128);
        assert_eq!(number_of_segments(1024), 128);
        assert_eq!(number_of_segments(1025), 129);
    }

    #[test]
    fn test_get_bit_coordinates() {
        let (segment, position) = get_bit_coordinates(5);
        assert_eq!(segment, 0);
        assert_eq!(position, 5);

        let (segment, position) = get_bit_coordinates(10);
        assert_eq!(segment, 1);
        assert_eq!(position, 2);
    }

    #[test]
    fn test_new_bitvector() {
        let num_bits = 10;
        let bv = BitVector::new(num_bits);
        let num_segments = number_of_segments(num_bits);
        assert_eq!(bv.num_bits, 10);
        assert_eq!(bv.vector.len(), num_segments as usize);
    }

    #[test]
    fn test_set_bit() {
        let num_bits = 1024;
        let bit_index = 512;
        let mut bv = BitVector::new(num_bits);
        bv.set_bit(bit_index);
        let (segment, position) = get_bit_coordinates(bit_index);
        let byte = bv.vector[segment as usize];
        
        assert_eq!((byte >> position) & 1, 1);
    }

    #[test]
    fn test_get_bit() {
        let num_bits = 1024;
        let mut bv = BitVector::new(num_bits);
        bv.set_bit(512);
        assert_eq!(bv.get_bit(512), Some(1u8));
    }

    #[test]
    fn test_is_set() {
        let num_bits = 1024;
        let mut bv = BitVector::new(num_bits);
        bv.set_bit(768);
        assert_eq!(bv.is_set(768), true);
    }

    #[test]
    fn test_clear() {
        let num_bits = 1024;
        let bit_index = 512;
        let mut bv = BitVector::new(num_bits);
        bv.set_bit(bit_index);
        assert_eq!(bv.num_bits, num_bits);
        assert_eq!(bv.is_set(bit_index), true);
        bv.clear(512);
        assert_eq!(bv.is_set(bit_index), false);
    }

}