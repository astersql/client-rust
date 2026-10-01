// Copyright 2026 AsterSQL.

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::proto::kvrpcpb;

/// Read-only RPC policy carried by the request plan, including spawned shards.
#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub timeout: Duration,
    pub stats: Arc<ReadStats>,
}

impl PartialEq for ReadOptions {
    fn eq(&self, other: &Self) -> bool {
        self.timeout == other.timeout && Arc::ptr_eq(&self.stats, &other.stats)
    }
}

impl ReadOptions {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            stats: Arc::new(ReadStats::default()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadAttempt {
    pub label: String,
    pub store_id: u64,
    pub elapsed: Duration,
    pub timed_out: bool,
}

/// Counts completed dispatch attempts, never inferred replica or region counts.
#[derive(Debug, Default)]
pub struct ReadStats {
    attempts: Mutex<Vec<ReadAttempt>>,
    point_responses: Mutex<PointResponseStats>,
}

impl ReadStats {
    pub fn point_response_stats(&self) -> PointResponseStats {
        *self.point_responses.lock().unwrap()
    }

    pub(crate) fn record_response(&self, response: &dyn Any) {
        let mut stats = self.point_responses.lock().unwrap();
        if let Some(r) = response.downcast_ref::<kvrpcpb::GetResponse>() {
            if r.region_error.is_some() {
                return;
            }
            let bytes = if r.error.is_none() && !r.not_found {
                r.value.len() as u64
            } else {
                0
            };
            stats.record_response(
                r.exec_details_v2
                    .as_ref()
                    .and_then(|d| d.scan_detail_v2.as_ref()),
                bytes,
            );
        } else if let Some(r) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
            if r.region_error.is_some() {
                return;
            }
            let bytes = if r.error.is_none() {
                r.pairs
                    .iter()
                    .filter(|p| p.error.is_none())
                    .map(|p| (p.key.len() + p.value.len()) as u64)
                    .sum()
            } else {
                0
            };
            stats.record_response(
                r.exec_details_v2
                    .as_ref()
                    .and_then(|d| d.scan_detail_v2.as_ref()),
                bytes,
            );
        } else if let Some(r) = response.downcast_ref::<kvrpcpb::BufferBatchGetResponse>() {
            if r.region_error.is_some() {
                return;
            }
            let bytes = if r.error.is_none() {
                r.pairs
                    .iter()
                    .filter(|p| p.error.is_none())
                    .map(|p| (p.key.len() + p.value.len()) as u64)
                    .sum()
            } else {
                0
            };
            stats.record_response(
                r.exec_details_v2
                    .as_ref()
                    .and_then(|d| d.scan_detail_v2.as_ref()),
                bytes,
            );
        }
    }

    pub fn snapshot(&self) -> Vec<ReadAttempt> {
        self.attempts.lock().unwrap().clone()
    }

    pub(crate) fn record(
        &self,
        label: &str,
        request: &dyn Any,
        elapsed: Duration,
        timed_out: bool,
    ) {
        let context = request
            .downcast_ref::<kvrpcpb::GetRequest>()
            .and_then(|r| r.context.as_ref())
            .or_else(|| {
                request
                    .downcast_ref::<kvrpcpb::BatchGetRequest>()
                    .and_then(|r| r.context.as_ref())
            })
            .or_else(|| {
                request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .and_then(|r| r.context.as_ref())
            });
        let store_id = context
            .and_then(|c| c.peer.as_ref())
            .map_or(0, |peer| peer.store_id);
        self.record_attempt(label, store_id, elapsed, timed_out);
    }

    pub fn record_attempt(&self, label: &str, store_id: u64, elapsed: Duration, timed_out: bool) {
        self.attempts.lock().unwrap().push(ReadAttempt {
            label: label.into(),
            store_id,
            elapsed,
            timed_out,
        });
    }
}

pub(crate) fn is_read_timeout(error: &crate::Error) -> bool {
    matches!(error, crate::Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded)
}

/// Value snapshot matching client-go util.PointResponseStats. Zero is valid,
/// but does not prove response coverage. Payload excludes protocol framing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PointResponseStats {
    pub total_keys: i64,
    pub processed_keys: i64,
    pub processed_bytes: i64,
    pub payload_bytes: u64,
    seen_response: bool,
    missing_scan_detail: bool,
    invalid: bool,
}
impl PointResponseStats {
    pub fn is_valid(&self) -> bool {
        !self.invalid
    }
    pub fn payload_complete(&self) -> bool {
        self.is_valid() && self.seen_response
    }
    pub fn scan_detail_complete(&self) -> bool {
        self.payload_complete() && !self.missing_scan_detail
    }
    pub fn invalidate(&mut self) {
        self.invalid = true;
    }
    pub fn record_response(&mut self, detail: Option<&kvrpcpb::ScanDetailV2>, payload_bytes: u64) {
        let mut delta = Self {
            payload_bytes,
            seen_response: true,
            missing_scan_detail: detail.is_none(),
            ..Self::default()
        };
        if let Some(d) = detail {
            delta.total_keys = d.total_versions as i64;
            delta.processed_keys = d.processed_versions as i64;
            delta.processed_bytes = d.processed_versions_size as i64;
        }
        self.merge(delta);
    }
    pub fn merge(&mut self, other: Self) {
        if !self.is_valid() || !other.is_valid() {
            self.invalidate();
            return;
        }
        self.total_keys = self.total_keys.wrapping_add(other.total_keys);
        self.processed_keys = self.processed_keys.wrapping_add(other.processed_keys);
        self.processed_bytes = self.processed_bytes.wrapping_add(other.processed_bytes);
        self.payload_bytes = self.payload_bytes.wrapping_add(other.payload_bytes);
        self.seen_response |= other.seen_response;
        self.missing_scan_detail |= other.missing_scan_detail;
    }
}

#[cfg(test)]
#[path = "point_response_stats_test.rs"]
mod point_response_stats_test;
