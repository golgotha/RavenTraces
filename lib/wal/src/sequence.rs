

pub struct Sequence {
    current: u32
}

impl Sequence {
    pub fn new(init_value: u32) -> Sequence {
        Sequence { current: init_value }
    }

    pub fn next(&mut self) -> Sequence {
        self.current += 1;
        Sequence { current: self.current }
    }
    
    pub fn current(&self) -> u32 {
        self.current
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Sequence::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_sequence() {
        let sequence = Sequence::new(1234);
        assert_eq!(sequence.current, 1234);
    }

    #[test]
    fn test_next_sequence() {
        let mut sequence = Sequence::new(0);
        let next_sequence = sequence.next();
        assert_eq!(next_sequence.current, 1);
    }
}