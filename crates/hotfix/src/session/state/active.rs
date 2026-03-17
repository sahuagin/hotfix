use crate::message::heartbeat::Heartbeat;
use crate::message::logout::Logout;
use crate::message::test_request::TestRequest;
use crate::session::state::{SessionCtx, SessionState, TestRequestId};
use crate::transport::writer::WriterRef;
use hotfix_store::MessageStore;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{error, info, warn};

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

    pub(crate) async fn on_disconnect(&self, reason: &str) -> SessionState {
        self.writer.disconnect().await;
        SessionState::new_disconnected(true, reason)
    }

    pub(crate) async fn on_peer_timeout<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
    ) -> Option<SessionState> {
        if self.sent_test_request_id.is_some() {
            warn!("peer didn't respond, terminating..");
            let logout = Logout::with_reason("peer timeout".to_string());
            if let Ok(prepared) = ctx.prepare_message(logout).await {
                self.writer.send_raw_message(prepared.raw).await;
            }
            self.writer.disconnect().await;
            return Some(SessionState::new_disconnected(true, "peer timeout"));
        }

        let req_id = format!("TEST_{}", ctx.store.next_target_seq_number());
        info!("sending TestRequest due to peer timer expiring");
        let request = TestRequest::new(req_id.clone());
        match ctx.prepare_message(request).await {
            Ok(prepared) => {
                self.writer.send_raw_message(prepared.raw).await;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
            }
            Err(err) => {
                error!(err = ?err, "failed to send TestRequest");
            }
        }
        self.reset_peer_timer(ctx.config.heartbeat_interval, Some(req_id));
        None
    }

    pub(crate) async fn on_heartbeat_timeout<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
    ) {
        let prepared = match ctx.prepare_message(Heartbeat::default()).await {
            Ok(prepared) => prepared,
            Err(err) => {
                error!(err = ?err, "failed to send heartbeat message");
                return;
            }
        };
        self.writer.send_raw_message(prepared.raw).await;
        self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
    }
}

#[inline]
pub(crate) fn calculate_peer_interval(heartbeat_interval: u64) -> u64 {
    (heartbeat_interval as f64 * super::TEST_REQUEST_THRESHOLD).round() as u64
}
