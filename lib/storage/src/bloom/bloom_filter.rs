use crate::bloom::bit_vector::BitVector;
use common::serialization::{Writable};
use std::io::Cursor;

pub trait BloomFilter<T> {
    fn add(&mut self, value: &T);

    fn might_contain(&self, value: &T) -> bool;

    /// Vector size in bytes
    fn vec_size(&self) -> usize;

}

#[derive(Debug, Clone)]
pub struct BloomFilterImpl {
    vector: BitVector,
    num_hashes: u8,
}

impl BloomFilterImpl {
    pub fn new(size: usize, rate: f64) -> Self {
        let num_bits = number_of_bits(size, rate);
        let num_hashes = number_of_hash_functions(num_bits, size);
        Self {
            vector: BitVector::new(num_bits),
            num_hashes,
        }
    }

    pub fn from_parts(num_bits: u64, num_hashes: u8, bits: Vec<u8>) -> Self{
        Self {
            vector: BitVector::from_bytes(num_bits, bits),
            num_hashes,
        }
    }

    fn hash_codes<T: Writable>(&self, value: &T) -> Vec<u64>
    where
        T: ?Sized,
    {
        let value_bytes = value.serialize();
        let hash1 = BloomFilterImpl::murmur3_32_bytes(&value_bytes, 0);
        let hash2 = BloomFilterImpl::murmur3_32_bytes(&value_bytes, hash1);

        let mut hash_codes = Vec::with_capacity(self.num_hashes as usize);

        for i in 0..self.num_hashes {
            let combined = hash1.wrapping_add((i as u32).wrapping_mul(hash2));
            hash_codes.push(self.normalize_hash_code(combined));
        }

        hash_codes
    }

    pub fn get_num_hashes(&self) -> u8 {
        self.num_hashes
    }

    pub fn vec_size(&self) -> usize {
        self.vector.get_vector().len()
    }
    
    pub fn get_num_bits(&self) -> usize {
        self.vector.get_num_bits() as usize
    }

    fn normalize_hash_code(&self, hash_code: u32) -> u64 {
        let num_bits = self.vector.get_num_bits();
        (hash_code as u64) % num_bits
    }

    fn murmur3_32_bytes(bytes: &[u8], seed: u32) -> u32 {
        let mut cursor = Cursor::new(bytes);
        murmur3::murmur3_32(&mut cursor, seed).expect("Failed to calculate Murmur3 hash")
    }

}

impl<T: Writable> BloomFilter<T> for BloomFilterImpl {
    fn add(&mut self, value: &T) {
        let hashes = self.hash_codes(value);

        for hash_code in hashes {
            self.vector.set_bit(hash_code);
        }
    }

    fn might_contain(&self, value: &T) -> bool {
        let hashes = self.hash_codes(value);
        for hash_code in hashes {
            if !self.vector.is_set(hash_code) {
                return false;
            }
        }
        true
    }

    fn vec_size(&self) -> usize {
        self.vector.vec_size()
    }
}

impl Writable for BloomFilterImpl {

    fn serialize(&self) -> Vec<u8> {
        self.vector.get_vector()
    }
}

/// m = -(size * ln(rate)/ln(2)^2)
/// m - number of bits
/// size - number of elements
/// rate - false positive rate (e.g. 0.1)
fn number_of_bits(size: usize, rate: f64) -> u64 {
    let divider = f64::ln(2.0).powi(2);
    (-(size as f64 * rate.ln()) / divider).ceil() as u64
}

pub fn number_of_hash_functions(num_bits: u64, size: usize) -> u8 {
    ((num_bits as f64 / size as f64) * f64::ln(2.0)).round() as u8
}

#[cfg(test)]
mod tests {
    use crate::span::TraceId;
    use super::*;

    struct MockValue {
        value: i64,
    }

    impl MockValue {
        fn new(value: i64) -> Self {
            Self { value }
        }
    }

    impl Writable for MockValue {
        fn serialize(&self) -> Vec<u8> {
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend(&self.value.to_le_bytes());
            buffer
        }
    }

    #[test]
    fn test_bloom_filter_add() {
        let mut bloom_filter = BloomFilterImpl::new(1024, 0.1);
        let mock_value = MockValue::new(10);
        bloom_filter.add(&mock_value);

        let lookup_value = MockValue::new(10);
        let result = bloom_filter.might_contain(&lookup_value);
        assert_eq!(result, true);
    }

    #[test]
    fn test_bloom_filter_might_contain_positive() {
        let mut bloom_filter = BloomFilterImpl::new(1024, 0.1);
        let mut mock_values = Vec::new();

        for _ in 0..1000 {
            let id: u64 = rand::random();
            let mock_value = MockValue::new(id as i64);
            bloom_filter.add(&mock_value);
            mock_values.push(mock_value);
        }

        for value in mock_values {
            let result = bloom_filter.might_contain(&value);
            assert_eq!(result, true);
        }
    }

    #[test]
    fn test_bloom_filter_might_contain_positive_trace_id() {
        let mut bloom_filter = BloomFilterImpl::new(1024, 0.1);
        let mut mock_values = Vec::new();

        for _ in 0..13456 {
            let trace_id_str= generate_trace_id();
            let mock_value = TraceId::from_str(trace_id_str.as_str()).unwrap();
            bloom_filter.add(&mock_value);
            mock_values.push(mock_value);
        }

        for value in mock_values {
            let result = bloom_filter.might_contain(&value);
            assert_eq!(result, true);
        }
    }

    #[test]
    fn test_bloom_filter_might_contain_negative() {
        let mut bloom_filter = BloomFilterImpl::new(1024, 0.1);
        let mock_value = MockValue::new(4096);
        bloom_filter.add(&mock_value);

        let lookup_value = MockValue::new(2048);
        let result = bloom_filter.might_contain(&lookup_value);
        assert_eq!(result, false);
    }

    fn generate_span_id() -> String {
        let id: u64 = rand::random();
        format!("{:016x}", id)
    }

    fn generate_trace_id() -> String {
        let part_1 = generate_span_id();
        let part_2 = generate_span_id();
        format!("{}{}", part_1, part_2)
    }
}
