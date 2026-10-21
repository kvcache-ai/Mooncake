pub(crate) struct EnvOverrideGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvOverrideGuard {
    pub(crate) fn new() -> Self {
        Self { saved: Vec::new() }
    }

    pub(crate) fn set_optional(&mut self, key: &'static str, value: Option<&str>) {
        let previous = std::env::var(key).ok();
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
        self.saved.push((key, previous));
    }
}

impl Drop for EnvOverrideGuard {
    fn drop(&mut self) {
        while let Some((key, previous)) = self.saved.pop() {
            match previous {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::EnvOverrideGuard;

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn env_guard_restores_previous_values_and_removals() {
        let _guard = env_test_lock().lock().expect("env lock poisoned");
        std::env::set_var("MC_TEST_ENV_EXISTING", "old");
        std::env::remove_var("MC_TEST_ENV_MISSING");
        {
            let mut overrides = EnvOverrideGuard::new();
            overrides.set_optional("MC_TEST_ENV_EXISTING", Some("new"));
            overrides.set_optional("MC_TEST_ENV_MISSING", Some("value"));
            assert_eq!(std::env::var("MC_TEST_ENV_EXISTING").as_deref(), Ok("new"));
            assert_eq!(std::env::var("MC_TEST_ENV_MISSING").as_deref(), Ok("value"));
        }
        assert_eq!(std::env::var("MC_TEST_ENV_EXISTING").as_deref(), Ok("old"));
        assert!(std::env::var("MC_TEST_ENV_MISSING").is_err());
        std::env::remove_var("MC_TEST_ENV_EXISTING");
    }
}
