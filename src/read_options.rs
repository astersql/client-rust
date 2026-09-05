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
}

impl ReadStats {
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
