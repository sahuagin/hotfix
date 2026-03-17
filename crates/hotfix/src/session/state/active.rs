use crate::session::state::TestRequestId;
use crate::transport::writer::WriterRef;
use std::time::Duration;
use tokio::time::Instant;

pub(crate) struct ActiveState {
    /// The writer's reference to send messages to the counterparty
    pub(crate) writer: WriterRef,
    /// When we should send the next heartbeat message to the counterparty
    pub(crate) heartbeat_deadline: Instant,
    /// When the next message from the counterparty is expected at the latest
    pub(crate) peer_deadline: Instant,
    /// The ID of the test request we sent on peer timer expiry
    pub(crate) sent_test_request_id: Option<TestRequestId>,
}

impl ActiveState {
    pub(crate) fn heartbeat_deadline(&self) -> &Instant {
        &self.heartbeat_deadline
    }

    pub(crate) fn reset_heartbeat_timer(&mut self, heartbeat_interval: u64) {
        self.heartbeat_deadline = Instant::now() + Duration::from_secs(heartbeat_interval);
    }

    pub(crate) fn peer_deadline(&self) -> &Instant {
        &self.peer_deadline
    }

    pub(crate) fn reset_peer_timer(
        &mut self,
        heartbeat_interval: u64,
        test_request_id: Option<TestRequestId>,
    ) {
        let interval = calculate_peer_interval(heartbeat_interval);
        self.peer_deadline = Instant::now() + Duration::from_secs(interval);
        self.sent_test_request_id = test_request_id;
    }

    pub(crate) fn expected_test_response_id(&self) -> Option<&TestRequestId> {
        self.sent_test_request_id.as_ref()
    }
}

#[inline]
pub(crate) fn calculate_peer_interval(heartbeat_interval: u64) -> u64 {
    (heartbeat_interval as f64 * super::TEST_REQUEST_THRESHOLD).round() as u64
}
