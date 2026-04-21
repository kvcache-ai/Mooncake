pub fn payload(seed: &str, size: usize) -> Vec<u8> {
    let mut state = 0u64;
    for byte in seed.as_bytes() {
        state = state.wrapping_mul(131).wrapping_add(*byte as u64 + 17);
    }
    let mut bytes = vec![0u8; size];
    for (index, byte) in bytes.iter_mut().enumerate() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(index as u64 + 1);
        *byte = ((state >> 24) % 251) as u8;
    }
    bytes
}

pub fn ensure_payload(label: &str, expected: &[u8], actual: &[u8]) -> Result<(), String> {
    if expected.len() != actual.len() {
        return Err(format!(
            "{label} length mismatch: actual={} expected={}",
            actual.len(),
            expected.len()
        ));
    }
    for (index, (left, right)) in actual.iter().zip(expected.iter()).enumerate() {
        if left != right {
            return Err(format!(
                "{label} payload mismatch at offset {index}: actual={left} expected={right}"
            ));
        }
    }
    Ok(())
}

pub fn make_key(prefix: &str, worker_id: usize, key_index: usize) -> String {
    format!("{prefix}-w{worker_id}-k{key_index}")
}

pub fn make_seed(global_seed: u64, key: &str, generation: u64) -> String {
    format!("{global_seed}-{key}-{generation}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_deterministic() {
        let a = payload("test-seed", 256);
        let b = payload("test-seed", 256);
        assert_eq!(a, b);
    }

    #[test]
    fn payload_different_seeds() {
        let a = payload("seed-a", 256);
        let b = payload("seed-b", 256);
        assert_ne!(a, b);
    }

    #[test]
    fn ensure_payload_match() {
        let data = payload("test", 128);
        assert!(ensure_payload("test", &data, &data).is_ok());
    }

    #[test]
    fn ensure_payload_mismatch() {
        let a = payload("seed-a", 128);
        let b = payload("seed-b", 128);
        assert!(ensure_payload("test", &a, &b).is_err());
    }

    #[test]
    fn ensure_payload_length_mismatch() {
        let a = payload("test", 64);
        let b = payload("test", 128);
        assert!(ensure_payload("test", &a, &b).is_err());
    }
}
