// Copyright 2026 AsterSQL.
// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The paging-relevant projection of PD controller runtime state.
//! Configuration alone is not usable runtime state: the first token response
//! must have completed. This component deliberately owns no allocation loop.

use crate::proto::resource_manager::{ResourceGroup, TokenBucketResponse};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceGroupRuntimeState {
    pub has_limited_burst: bool,
}

#[derive(Debug)]
struct GroupState {
    burst: i64,
    initial_request_completed: bool,
    tombstone: bool,
}

#[derive(Debug, Default)]
pub struct ResourceGroupRuntimeStates {
    groups: RwLock<HashMap<String, GroupState>>,
}

impl ResourceGroupRuntimeStates {
    /// Install a newly created group controller. Before its first response,
    /// callers must fall back to schema metadata, even for bounded groups.
    pub fn register_group(&self, group: &ResourceGroup) {
        let burst = group
            .r_u_settings
            .as_ref()
            .and_then(|ru| ru.r_u.as_ref())
            .and_then(|bucket| bucket.settings.as_ref())
            .map_or(0, |settings| settings.burst_limit);
        self.groups
            .write()
            .expect("resource group state lock poisoned")
            .insert(
                group.name.clone(),
                GroupState {
                    burst,
                    initial_request_completed: false,
                    tombstone: false,
                },
            );
    }

    /// Process the actual PD response, in grant order, before publishing the
    /// derived state. Unknown group responses do not create controllers.
    pub fn handle_token_bucket_responses(&self, responses: &[TokenBucketResponse]) {
        let mut groups = self
            .groups
            .write()
            .expect("resource group state lock poisoned");
        for response in responses {
            let Some(group) = groups.get_mut(&response.resource_group_name) else {
                continue;
            };
            for grant in &response.granted_r_u_tokens {
                group.burst = grant
                    .granted_tokens
                    .as_ref()
                    .and_then(|bucket| bucket.settings.as_ref())
                    .map_or(0, |settings| settings.burst_limit);
            }
            group.initial_request_completed = true;
        }
    }

    /// Keep deletion visible while in-flight responses finish.
    pub fn tombstone_group(&self, name: &str) {
        if let Some(group) = self
            .groups
            .write()
            .expect("resource group state lock poisoned")
            .get_mut(name)
        {
            group.tombstone = true;
        }
    }

    pub fn get_resource_group_runtime_state(
        &self,
        name: &str,
    ) -> Option<ResourceGroupRuntimeState> {
        let groups = self
            .groups
            .read()
            .expect("resource group state lock poisoned");
        let group = groups.get(name)?;
        (!group.tombstone && group.initial_request_completed).then_some(ResourceGroupRuntimeState {
            has_limited_burst: group.burst >= 0,
        })
    }
}

#[cfg(test)]
#[path = "resource_group_runtime_test.rs"]
mod tests;
