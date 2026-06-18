const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0001_0000_01b3;

pub(crate) fn stable_hash(parts: &[&str]) -> u64 {
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::stable_hash;

    #[test]
    fn stable_hash_is_order_and_boundary_sensitive() {
        assert_eq!(
            stable_hash(&["alpha", "beta"]),
            stable_hash(&["alpha", "beta"])
        );
        assert_ne!(
            stable_hash(&["alpha", "beta"]),
            stable_hash(&["beta", "alpha"])
        );
        assert_ne!(stable_hash(&["ab", "c"]), stable_hash(&["a", "bc"]));
    }
}
