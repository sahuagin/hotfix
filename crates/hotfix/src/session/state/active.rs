use crate::Application;
use crate::application::{InboundDecision, OutboundDecision};
use crate::message::business_reject::BusinessReject;
use crate::message::heartbeat::Heartbeat;
use crate::message::logon::Logon;
use crate::message::logout::Logout;
use crate::message::reject::Reject;
use crate::message::resend_request::ResendRequest;
use crate::message::sequence_reset::SequenceReset;
use crate::message::test_request::TestRequest;
use crate::session::error::{InternalSendResultExt, SendError, SendOutcome, SessionOperationError};
use crate::session::get_msg_seq_num;
use crate::session::message_handling;
use crate::session::state::{
    AwaitingResendState, SessionCtx, SessionState, TestRequestId, TransitionResult, VerifyResult,
};
use crate::transport::writer::WriterRef;
use hotfix_message::Part;
use hotfix_message::session_fields::{
    BEGIN_SEQ_NO, END_SEQ_NO, GAP_FILL_FLAG, MSG_TYPE, NEW_SEQ_NO, SessionRejectReason, TEST_REQ_ID,
};
use hotfix_store::MessageStore;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

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

    pub(crate) async fn on_fix_message<App: Application, Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        let message_type: &str = message
            .header()
            .get(MSG_TYPE)
            .map_err(|_| SessionOperationError::MissingField("MSG_TYPE"))?;

        match message_type {
            Heartbeat::MSG_TYPE => self.on_heartbeat(ctx, &message).await,
            TestRequest::MSG_TYPE => self.on_test_request(ctx, &message).await,
            ResendRequest::MSG_TYPE => self.on_resend_request(ctx, &message).await,
            Reject::MSG_TYPE => self.on_reject(ctx, &message).await,
            SequenceReset::MSG_TYPE => self.on_sequence_reset(ctx, &message).await,
            Logout::MSG_TYPE => self.on_logout(ctx, app, &message).await,
            Logon::MSG_TYPE => {
                error!("received unexpected logon message");
                Ok(TransitionResult::Stay)
            }
            _ => self.on_app_message(ctx, app, &message).await,
        }
    }

    async fn on_heartbeat<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, true, true).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self
                    .transition_to_awaiting_resend(ctx, expected, actual)
                    .await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        if let (Some(expected_req_id), Ok(message_req_id)) = (
            self.expected_test_response_id(),
            message.get::<&str>(TEST_REQ_ID),
        ) && expected_req_id.as_str() == message_req_id
        {
            debug!("received response for TestRequest, resetting timer");
            self.reset_peer_timer(ctx.config.heartbeat_interval, None);
        }

        ctx.store.increment_target_seq_number().await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_test_request<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, true, true).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self
                    .transition_to_awaiting_resend(ctx, expected, actual)
                    .await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        let req_id: &str = message.get(TEST_REQ_ID).unwrap_or_else(|_| {
            // TODO: send reject?
            todo!()
        });

        ctx.store.increment_target_seq_number().await?;

        ctx.send_message(&self.writer, Heartbeat::for_request(req_id.to_string()))
            .await
            .with_send_context("heartbeat response")?;
        self.reset_heartbeat_timer(ctx.config.heartbeat_interval);

        Ok(TransitionResult::Stay)
    }

    async fn on_resend_request<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, false, true).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { .. } => {
                // ResendRequest with check_too_high=false should never get SeqTooHigh,
                // but handle gracefully
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        let msg_seq_num = get_msg_seq_num(message);
        let expected = ctx.store.next_target_seq_number();

        let begin_seq_number: u64 = match message.get(BEGIN_SEQ_NO) {
            Ok(seq_number) => seq_number,
            Err(_) => {
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::RequiredTagMissing)
                    .text("missing begin sequence number for resend request");
                ctx.send_message(&self.writer, reject)
                    .await
                    .with_send_context("reject for missing BEGIN_SEQ_NO")?;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
                return Ok(TransitionResult::Stay);
            }
        };

        let end_seq_number: u64 = match message.get(END_SEQ_NO) {
            Ok(seq_number) => {
                let last_seq_number = ctx.store.next_sender_seq_number() - 1;
                if seq_number == 0 {
                    last_seq_number
                } else {
                    std::cmp::min(seq_number, last_seq_number)
                }
            }
            Err(_) => {
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::RequiredTagMissing)
                    .text("missing end sequence number for resend request");
                ctx.send_message(&self.writer, reject)
                    .await
                    .with_send_context("reject for missing END_SEQ_NO")?;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
                return Ok(TransitionResult::Stay);
            }
        };

        // Only increment target seq if seq matches expected
        if msg_seq_num == expected {
            ctx.store.increment_target_seq_number().await?;
        }

        message_handling::resend_messages(ctx, &self.writer, begin_seq_number, end_seq_number)
            .await?;
        self.reset_heartbeat_timer(ctx.config.heartbeat_interval);

        Ok(TransitionResult::Stay)
    }

    async fn on_reject<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, false, true).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self
                    .transition_to_awaiting_resend(ctx, expected, actual)
                    .await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        ctx.store.increment_target_seq_number().await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_sequence_reset<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        let msg_seq_num = get_msg_seq_num(message);
        let is_gap_fill: bool = message.get(GAP_FILL_FLAG).unwrap_or(false);
        match message_handling::verify_and_handle(
            ctx,
            &self.writer,
            message,
            is_gap_fill,
            is_gap_fill,
        )
        .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self
                    .transition_to_awaiting_resend(ctx, expected, actual)
                    .await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        let end: u64 = match message.get(NEW_SEQ_NO) {
            Ok(new_seq_no) => new_seq_no,
            Err(err) => {
                error!(
                    "received sequence reset message without new sequence number: {:?}",
                    err
                );
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::RequiredTagMissing)
                    .text("missing NewSeqNo tag in sequence reset message");
                ctx.send_message(&self.writer, reject)
                    .await
                    .with_send_context("reject for missing NEW_SEQ_NO")?;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
                return Ok(TransitionResult::Stay);
            }
        };

        if end <= ctx.store.next_target_seq_number() {
            error!(
                "received sequence reset message which would move target seq number backwards: {end}",
            );
            let text =
                format!("attempt to lower sequence number, invalid value NewSeqNo(36)={end}");
            let reject = Reject::new(msg_seq_num)
                .session_reject_reason(SessionRejectReason::ValueIsIncorrect)
                .text(&text);
            ctx.send_message(&self.writer, reject)
                .await
                .with_send_context("reject for invalid sequence reset")?;
            self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
            return Ok(TransitionResult::Stay);
        }

        ctx.store.set_target_seq_number(end - 1).await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_logout<App: Application, Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, false, false).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { .. } => {
                // verify with check_too_high=false, so this shouldn't happen
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        // We are logged on, send logout response
        let logout = Logout::with_reason("Logout acknowledged".to_string());
        match ctx.prepare_message(logout).await {
            Ok(prepared) => {
                self.writer.send_raw_message(prepared.raw).await;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
            }
            Err(err) => warn!("failed to send logout acknowledgement: {err}"),
        }

        app.on_logout("peer has logged us out").await;

        self.writer.disconnect().await;
        ctx.store.increment_target_seq_number().await?;

        Ok(TransitionResult::TransitionTo(
            SessionState::new_disconnected(true, "peer has logged us out"),
        ))
    }

    async fn on_app_message<App: Application, Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: &hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match message_handling::verify_and_handle(ctx, &self.writer, message, true, true).await? {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self
                    .transition_to_awaiting_resend(ctx, expected, actual)
                    .await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        match app.on_inbound_message(message).await {
            InboundDecision::Accept => {}
            InboundDecision::Reject { reason, text } => {
                let msg_type: &str = message
                    .header()
                    .get(MSG_TYPE)
                    .map_err(|_| SessionOperationError::MissingField("MSG_TYPE"))?;
                let mut reject =
                    BusinessReject::new(msg_type, reason).ref_seq_num(get_msg_seq_num(message));
                if let Some(text) = text {
                    reject = reject.text(&text);
                }
                ctx.send_message(&self.writer, reject)
                    .await
                    .with_send_context("business message reject")?;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
            }
            InboundDecision::TerminateSession => {
                error!("failed to send inbound message to application");
                self.writer.disconnect().await;
            }
        }
        ctx.store.increment_target_seq_number().await?;

        Ok(TransitionResult::Stay)
    }

    async fn transition_to_awaiting_resend<Store: MessageStore>(
        &self,
        ctx: &mut SessionCtx<'_, Store>,
        expected: u64,
        actual: u64,
    ) -> Result<TransitionResult, SessionOperationError> {
        debug!("we are behind target (ours: {expected}, theirs: {actual}), requesting resend.");
        let request = ResendRequest::new(expected, actual);
        ctx.send_message(&self.writer, request)
            .await
            .with_send_context("resend request")?;
        let new_state = SessionState::AwaitingResend(AwaitingResendState::new(
            self.writer.clone(),
            expected,
            actual,
        ));
        Ok(TransitionResult::TransitionTo(new_state))
    }

    pub(crate) async fn send_app_message<App: Application, Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: App::Outbound,
    ) -> Result<SendOutcome, SendError> {
        match app.on_outbound_message(&message).await {
            OutboundDecision::Send => {
                let seq_num =
                    ctx.send_message(&self.writer, message)
                        .await
                        .map_err(|e| match e {
                            crate::session::error::InternalSendError::Persist(e) => {
                                SendError::Persist(e)
                            }
                            crate::session::error::InternalSendError::SequenceNumber(e) => {
                                SendError::SequenceNumber(e)
                            }
                        })?;
                self.reset_heartbeat_timer(ctx.config.heartbeat_interval);
                Ok(SendOutcome::Sent {
                    sequence_number: seq_num,
                })
            }
            OutboundDecision::Drop => {
                debug!("dropped outbound message as instructed by the application");
                Ok(SendOutcome::Dropped)
            }
            OutboundDecision::TerminateSession => {
                warn!("the application indicated we should terminate the session");
                self.writer.disconnect().await;
                Err(SendError::SessionTerminated)
            }
        }
    }
}

#[inline]
pub(crate) fn calculate_peer_interval(heartbeat_interval: u64) -> u64 {
    (heartbeat_interval as f64 * super::TEST_REQUEST_THRESHOLD).round() as u64
}
