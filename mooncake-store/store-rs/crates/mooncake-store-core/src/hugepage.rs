use crate::{Result, StoreError};

const HUGE_2MB: usize = 2 * 1024 * 1024;
const HUGE_1GB: usize = 1024 * 1024 * 1024;
const ENV_USE_HUGEPAGE: &str = "MC_STORE_USE_HUGEPAGE";
const ENV_HUGEPAGE_SIZE: &str = "MC_STORE_HUGEPAGE_SIZE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HugePageConfig {
    bytes: usize,
}

impl HugePageConfig {
    pub const DEFAULT_BYTES: usize = HUGE_2MB;

    pub fn new(bytes: usize) -> Result<Self> {
        match bytes {
            HUGE_2MB | HUGE_1GB => Ok(Self { bytes }),
            other => Err(StoreError::Allocator(format!(
                "unsupported hugepage size {other}; supported values are 2MB and 1GB"
            ))),
        }
    }

    pub fn bytes(self) -> usize {
        self.bytes
    }

    pub fn label(self) -> &'static str {
        match self.bytes {
            HUGE_2MB => "2MB",
            HUGE_1GB => "1GB",
            _ => "unknown",
        }
    }

    pub fn align_up(self, value: usize) -> usize {
        align_up(value, self.bytes)
    }

    pub fn resolve(
        enabled_override: Option<bool>,
        size_override: Option<usize>,
    ) -> Result<Option<Self>> {
        let env_enabled = std::env::var_os(ENV_USE_HUGEPAGE).is_some();
        let env_size = std::env::var(ENV_HUGEPAGE_SIZE).ok();
        Self::resolve_from_inputs(
            enabled_override,
            size_override,
            env_enabled,
            env_size.as_deref(),
        )
    }

    pub fn resolve_from_inputs(
        enabled_override: Option<bool>,
        size_override: Option<usize>,
        env_enabled: bool,
        env_size: Option<&str>,
    ) -> Result<Option<Self>> {
        let enabled = enabled_override.unwrap_or(env_enabled || size_override.is_some());
        if !enabled {
            return Ok(None);
        }

        let size = match size_override {
            Some(size) => size,
            None => env_size
                .map(parse_hugepage_size)
                .transpose()?
                .unwrap_or(Self::DEFAULT_BYTES),
        };
        Ok(Some(Self::new(size)?))
    }
}

pub fn parse_hugepage_size(input: &str) -> Result<usize> {
    let normalized = input.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "2m" | "2mb" | "2097152" => Ok(HUGE_2MB),
        "1g" | "1gb" | "1073741824" => Ok(HUGE_1GB),
        other => Err(StoreError::Allocator(format!(
            "unsupported hugepage size '{other}'; supported values are 2MB and 1GB"
        ))),
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_supported_hugepage_sizes() {
        assert_eq!(
            parse_hugepage_size("2MB").expect("2MB should parse"),
            HUGE_2MB
        );
        assert_eq!(
            parse_hugepage_size("2m").expect("2m should parse"),
            HUGE_2MB
        );
        assert_eq!(
            parse_hugepage_size("1GB").expect("1GB should parse"),
            HUGE_1GB
        );
        assert_eq!(
            parse_hugepage_size("1073741824").expect("1GB bytes should parse"),
            HUGE_1GB
        );
    }

    #[test]
    fn resolve_hugepage_prefers_explicit_values() {
        let resolved = HugePageConfig::resolve_from_inputs(Some(true), Some(HUGE_1GB), false, None)
            .expect("explicit hugepage config should resolve")
            .expect("explicit hugepage config should be enabled");
        assert_eq!(resolved.bytes(), HUGE_1GB);
    }

    #[test]
    fn resolve_hugepage_uses_env_defaults() {
        let resolved = HugePageConfig::resolve_from_inputs(None, None, true, Some("2MB"))
            .expect("env hugepage config should resolve")
            .expect("env hugepage config should be enabled");
        assert_eq!(resolved.bytes(), HUGE_2MB);
    }

    #[test]
    fn resolve_hugepage_can_disable_env() {
        let resolved = HugePageConfig::resolve_from_inputs(Some(false), None, true, Some("1GB"))
            .expect("explicit disable should resolve");
        assert!(resolved.is_none());
    }
}
