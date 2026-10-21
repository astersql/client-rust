// Copyright 2026 AsterSQL.
// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Resource-manager lookup and configuration portion of PD's RU controller.
//! Degraded metadata is synthesized here, never in the ordinary metadata cache.
//! This module does not implement the separately owned RU accounting algorithm.

use crate::proto::{meta_storagepb as meta, resource_manager as rm};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{Code, Status};

pub const CONTROLLER_CONFIG_PATH: &[u8] = b"resource_group/controller";

#[derive(Clone, Debug, thiserror::Error)]
pub enum LookupError {
    #[error("{0}")]
    Rpc(Status),
    #[error("context canceled")]
    CallerCanceled,
    #[error("context deadline exceeded")]
    CallerDeadlineExceeded,
    #[error("resource group {0}: resource group does not exist")]
    NotFound(String),
    #[error("{0}")]
    Other(String),
    #[error("get resource group {name}: {cause}")]
    GetResourceGroup {
        name: String,
        cause: Box<LookupError>,
    },
}
impl LookupError {
    fn root(&self) -> &Self {
        match self {
            Self::GetResourceGroup { cause, .. } => cause.root(),
            _ => self,
        }
    }
    fn normalized(self) -> Self {
        match self.root() {
            Self::CallerCanceled => Self::CallerCanceled,
            Self::CallerDeadlineExceeded => Self::CallerDeadlineExceeded,
            _ => self,
        }
    }
    fn transient(&self) -> bool {
        let message = self.to_string();
        if message.contains("PD:resourcemanager:ErrGroupNotExists")
            || message.contains("resource group does not exist")
        {
            return false;
        }
        if matches!(
            self.root(),
            Self::CallerCanceled | Self::CallerDeadlineExceeded
        ) {
            return false;
        }
        let cause = self.root().to_string();
        if ["no leader", "not leader", "is not served", "not primary"]
            .iter()
            .any(|message| cause.contains(message))
        {
            return true;
        }
        match self.root() {
            Self::Rpc(status) => {
                matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
            }
            _ => false,
        }
    }
}
impl From<Status> for LookupError {
    fn from(status: Status) -> Self {
        Self::Rpc(status)
    }
}

/// The network boundary shared by controller config and resource-group lookup.
pub trait ResourceGroupProvider: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<meta::GetResponse, LookupError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<meta::PutResponse, LookupError>;
    fn get_resource_group(&self, name: &str) -> Result<Option<rm::ResourceGroup>, LookupError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct TokenRpcParams {
    pub wait_retry_interval: String,
    pub wait_retry_times: u32,
}
impl Default for TokenRpcParams {
    fn default() -> Self {
        Self {
            wait_retry_interval: "50ms".into(),
            wait_retry_times: 20,
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuVersionPolicy {
    pub default: u32,
    #[serde(default)]
    pub overrides: HashMap<u32, u32>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ServerConfig {
    pub degraded_mode_wait_duration: String,
    pub ltb_max_wait_duration: String,
    pub ltb_token_rpc_max_delay: String,
    pub token_rpc_params: TokenRpcParams,
    pub ru_version_policy: Option<RuVersionPolicy>,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            degraded_mode_wait_duration: "0s".into(),
            ltb_max_wait_duration: "30s".into(),
            ltb_token_rpc_max_delay: "1s".into(),
            token_rpc_params: TokenRpcParams::default(),
            ru_version_policy: None,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControllerConfig {
    pub max_wait_duration: Duration,
    pub wait_retry_interval: Duration,
    pub wait_retry_times: u32,
    pub degraded_mode_wait_duration: Duration,
}
fn duration(raw: &str) -> Result<Duration, LookupError> {
    if raw == "0" {
        return Ok(Duration::ZERO);
    }
    let mut rest = raw;
    let mut seconds = 0.0;
    while !rest.is_empty() {
        let split = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| LookupError::Other(format!("invalid duration {raw}")))?;
        let value: f64 = rest[..split]
            .parse()
            .map_err(|_| LookupError::Other(format!("invalid duration {raw}")))?;
        rest = &rest[split..];
        let (unit, scale) = [
            ("ns", 1e-9),
            ("us", 1e-6),
            ("µs", 1e-6),
            ("μs", 1e-6),
            ("ms", 1e-3),
            ("s", 1.0),
            ("m", 60.0),
            ("h", 3600.0),
        ]
        .into_iter()
        .find(|(unit, _)| rest.starts_with(unit))
        .ok_or_else(|| LookupError::Other(format!("invalid duration {raw}")))?;
        seconds += value * scale;
        rest = &rest[unit.len()..];
    }
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| LookupError::Other(format!("invalid duration {raw}")))
}

#[derive(Clone, Debug)]
pub enum CreateOption {
    MaxWaitDuration(Duration),
    WaitRetryInterval(Duration),
    WaitRetryTimes(u32),
    DegradedModeWaitDuration(Duration),
    DegradedRuSettings(rm::GroupRequestUnitSettings),
}

pub struct ResourceGroupLookupController {
    provider: Arc<dyn ResourceGroupProvider>,
    config: ControllerConfig,
    degraded: Option<rm::GroupRequestUnitSettings>,
    groups: Mutex<HashMap<String, rm::ResourceGroup>>,
    ru_version: u32,
}
impl ResourceGroupLookupController {
    pub fn new(
        provider: Arc<dyn ResourceGroupProvider>,
        keyspace_id: u32,
        options: &[CreateOption],
    ) -> Result<Self, LookupError> {
        let response = provider.get(CONTROLLER_CONFIG_PATH)?;
        let server: ServerConfig = match response.kvs.first() {
            Some(kv) => {
                serde_json::from_slice(&kv.value).map_err(|e| LookupError::Other(e.to_string()))?
            }
            None => ServerConfig::default(),
        };
        let mut interval = duration(&server.token_rpc_params.wait_retry_interval)?;
        if interval.is_zero() {
            interval = Duration::from_millis(50);
        }
        let rpc_delay = duration(&server.ltb_token_rpc_max_delay)?;
        let mut config = ControllerConfig {
            max_wait_duration: duration(&server.ltb_max_wait_duration)?,
            wait_retry_interval: interval,
            wait_retry_times: (rpc_delay.as_nanos() / interval.as_nanos()).min(u32::MAX as u128)
                as u32,
            degraded_mode_wait_duration: duration(&server.degraded_mode_wait_duration)?,
        };
        if config.max_wait_duration.is_zero() {
            config.max_wait_duration = Duration::from_secs(30);
        }
        let ru_version = server.ru_version_policy.map_or(1, |policy| {
            policy
                .overrides
                .get(&keyspace_id)
                .copied()
                .unwrap_or(policy.default)
                .max(1)
        });
        let mut degraded = None;
        for option in options {
            match option {
                CreateOption::MaxWaitDuration(d) => config.max_wait_duration = *d,
                CreateOption::WaitRetryInterval(d) => config.wait_retry_interval = *d,
                CreateOption::WaitRetryTimes(n) => config.wait_retry_times = *n,
                CreateOption::DegradedModeWaitDuration(d) => {
                    config.degraded_mode_wait_duration = *d
                }
                CreateOption::DegradedRuSettings(s) => degraded = Some(s.clone()),
            }
        }
        Ok(Self {
            provider,
            config,
            degraded,
            groups: Mutex::new(HashMap::new()),
            ru_version,
        })
    }
    pub fn config(&self) -> &ControllerConfig {
        &self.config
    }
    pub fn ru_version(&self) -> u32 {
        self.ru_version
    }
    pub fn get_resource_group(&self, name: &str) -> Result<rm::ResourceGroup, LookupError> {
        if let Some(group) = self.groups.lock().unwrap().get(name).cloned() {
            return Ok(group);
        }
        let (group, degraded) = match self.provider.get_resource_group(name) {
            Ok(group) => (
                group.ok_or_else(|| LookupError::NotFound(name.into()))?,
                false,
            ),
            Err(error) if self.degraded.is_some() && error.transient() => (
                rm::ResourceGroup {
                    name: name.into(),
                    mode: rm::GroupMode::RuMode as i32,
                    r_u_settings: self.degraded.clone(),
                    ..Default::default()
                },
                true,
            ),
            Err(error) => return Err(error.normalized()),
        };
        let mut groups = self.groups.lock().unwrap();
        if let Some(cached) = groups.get(name) {
            return Ok(cached.clone());
        }
        if group.mode != rm::GroupMode::RuMode as i32
            || group
                .r_u_settings
                .as_ref()
                .and_then(|ru| ru.r_u.as_ref())
                .and_then(|bucket| bucket.settings.as_ref())
                .is_none()
        {
            return Err(LookupError::Other(
                "resource group configuration is unavailable".into(),
            ));
        }
        if !degraded {
            groups.insert(name.into(), group.clone());
        }
        Ok(group)
    }
    /// Called by a metadata watcher; deletions must not leave stale cache entries.
    pub fn invalidate(&self, name: &str) {
        self.groups.lock().unwrap().remove(name);
    }
}

#[cfg(test)]
#[path = "resource_group_lookup_test.rs"]
mod tests;
