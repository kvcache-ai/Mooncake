#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierKind {
    Ssd,
    Nfs,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierSsdEngine {
    #[default]
    LocalDir,
    /// Experimental SSD engine with restart recovery for materialized extent locators.
    ExtentStore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Startup bootstrap config for a Mooncake cold tier device.
///
/// The `target` field uses `mooncake_store_core::ColdTierTargetSpec` to identify a disk device
/// (directory path or block device UUID). Supplying this config at startup is equivalent to
/// the Admin HTTP create/register/enable lifecycle for the initial device state.
pub struct ColdTierTargetConfig {
    pub cold_tier_id: String,
    pub kind: ColdTierKind,
    pub target: mooncake_store_core::ColdTierTargetSpec,
    pub ssd_engine: ColdTierSsdEngine,
    pub capacity_override_bytes: Option<u64>,
    pub tags: Vec<String>,
}

impl ColdTierTargetConfig {
    pub fn directory(
        cold_tier_id: impl Into<String>,
        kind: ColdTierKind,
        directory: impl Into<std::path::PathBuf>,
    ) -> Self {
        let path: std::path::PathBuf = directory.into();
        Self {
            cold_tier_id: cold_tier_id.into(),
            kind,
            target: mooncake_store_core::ColdTierTargetSpec::Directory {
                path: path.to_string_lossy().into_owned(),
            },
            ssd_engine: ColdTierSsdEngine::LocalDir,
            capacity_override_bytes: None,
            tags: Vec::new(),
        }
    }

    pub fn uuid(
        cold_tier_id: impl Into<String>,
        kind: ColdTierKind,
        uuid: impl Into<String>,
    ) -> Self {
        Self {
            cold_tier_id: cold_tier_id.into(),
            kind,
            target: mooncake_store_core::ColdTierTargetSpec::Uuid {
                uuid: uuid.into(),
            },
            ssd_engine: ColdTierSsdEngine::LocalDir,
            capacity_override_bytes: None,
            tags: Vec::new(),
        }
    }

    pub fn ssd_engine(mut self, ssd_engine: ColdTierSsdEngine) -> Self {
        self.ssd_engine = ssd_engine;
        self
    }

    /// Selects the experimental ExtentStore SSD engine.
    ///
    /// ExtentStore supports restart recovery for materialized extent locators, but remains
    /// experimental while operational coverage is expanded.
    pub fn extent_store_engine(self) -> Self {
        self.ssd_engine(ColdTierSsdEngine::ExtentStore)
    }

    pub fn capacity_override_bytes(mut self, capacity_override_bytes: u64) -> Self {
        self.capacity_override_bytes = Some(capacity_override_bytes.max(1));
        self
    }

    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierTargetSpec {
    pub cold_tier_id: String,
    pub kind: ColdTierKind,
    #[serde(default)]
    pub directory: Option<std::path::PathBuf>,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub ssd_engine: Option<ColdTierSsdEngine>,
    #[serde(default)]
    pub capacity_override_bytes: Option<u64>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl TryFrom<ColdTierTargetSpec> for ColdTierTargetConfig {
    type Error = StoreError;

    fn try_from(value: ColdTierTargetSpec) -> std::result::Result<Self, Self::Error> {
        let cold_tier_id = value.cold_tier_id.trim().to_string();
        if cold_tier_id.is_empty() {
            return Err(StoreError::InvalidState(
                "cold tier cold_tier_id must not be empty".to_string(),
            ));
        }
        let target = match (value.directory, value.uuid) {
            (Some(directory), None) => mooncake_store_core::ColdTierTargetSpec::Directory {
                path: directory.to_string_lossy().into_owned(),
            },
            (None, Some(uuid)) if !uuid.trim().is_empty() => {
                mooncake_store_core::ColdTierTargetSpec::Uuid {
                    uuid: uuid.trim().to_string(),
                }
            }
            (Some(_), Some(_)) => {
                return Err(StoreError::InvalidState(format!(
                    "cold tier {} must configure exactly one of directory or uuid",
                    value.cold_tier_id
                )))
            }
            _ => {
                return Err(StoreError::InvalidState(format!(
                    "cold tier {} must configure one of directory or uuid",
                    value.cold_tier_id
                )))
            }
        };
        if value.kind == ColdTierKind::Nfs
            && matches!(
                target,
                mooncake_store_core::ColdTierTargetSpec::Uuid { .. }
            )
        {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} does not support uuid targets for nfs backends",
                value.cold_tier_id
            )));
        }
        let ssd_engine = value.ssd_engine.unwrap_or_default();
        if value.kind != ColdTierKind::Ssd && ssd_engine != ColdTierSsdEngine::LocalDir {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} can only configure ssd_engine for ssd backends",
                value.cold_tier_id
            )));
        }
        Ok(ColdTierTargetConfig {
            cold_tier_id,
            kind: value.kind,
            target,
            ssd_engine,
            capacity_override_bytes: value.capacity_override_bytes,
            tags: value.tags,
        })
    }
}
