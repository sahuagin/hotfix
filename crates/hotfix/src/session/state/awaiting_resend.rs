use crate::Application;
use crate::application::InboundDecision;
use crate::message::business_reject::BusinessReject;
use crate::message::heartbeat::Heartbeat;
use crate::message::logon::Logon;
use crate::message::logout::Logout;
use crate::message::reject::Reject;
use crate::message::resend_request::ResendRequest;
use crate::message::sequence_reset::SequenceReset;
use crate::message::test_request::TestRequest;
use crate::session::error::{InternalSendResultExt, SessionOperationError};
use crate::session::get_msg_seq_num;
use crate::session::state::{SessionCtx, SessionState, TransitionResult, VerifyResult};
use crate::transport::writer::WriterRef;
use hotfix_message::Part;
use hotfix_message::message::Message;
use hotfix_message::session_fields::{
    BEGIN_SEQ_NO, END_SEQ_NO, GAP_FILL_FLAG, MSG_TYPE, NEW_SEQ_NO, SessionRejectReason, TEST_REQ_ID,
};
use hotfix_store::MessageStore;
use std::collections::VecDeque;
use tracing::{debug, error, warn};

const MAX_RESEND_ATTEMPTS: usize = 3;

/// Session state we're in while processing messages we requested to be resent.
pub(crate) struct AwaitingResendState {
    /// The reference to the writer loop.
    pub(crate) writer: WriterRef,
    /// The beginning of the gap we're waiting for the target to resend.
    pub(crate) begin_seq_number: u64,
    /// The end of the gap we're waiting for the target to resend.
    pub(crate) end_seq_number: u64,
    /// Inbound messages we receive while processing the resend.
    pub(crate) inbound_queue: VecDeque<Message>,
    /// The number of times we've attempted to ask the counterparty to resend the gap.
    pub(crate) resend_attempts: usize,
}

impl AwaitingResendState {
    pub(crate) async fn on_disconnect(&self, reason: &str) -> SessionState {
        self.writer.disconnect().await;
        SessionState::new_disconnected(true, reason)
    }

    pub(crate) fn new(writer: WriterRef, begin_seq_number: u64, end_seq_number: u64) -> Self {
        Self {
            writer,
            begin_seq_number,
            end_seq_number,
            inbound_queue: Default::default(),
            resend_attempts: 1,
        }
    }

    pub(crate) fn update(
        &mut self,
        begin_seq_number: u64,
        end_seq_number: u64,
    ) -> AwaitingResendTransitionOutcome {
        let resend_attempts = if self.begin_seq_number == begin_seq_number {
            if self.resend_attempts + 1 > MAX_RESEND_ATTEMPTS {
                return AwaitingResendTransitionOutcome::AttemptsExceeded;
            }
            self.resend_attempts + 1
        } else if begin_seq_number < self.begin_seq_number {
            return AwaitingResendTransitionOutcome::BeginSeqNumberTooLow;
        } else {
            1
        };

        self.resend_attempts = resend_attempts;
        self.begin_seq_number = begin_seq_number;
        self.end_seq_number = end_seq_number;

        AwaitingResendTransitionOutcome::Success
    }

    pub(crate) async fn on_fix_message<App: Application, Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        let message_type: &str = message
            .header()
            .get(MSG_TYPE)
            .map_err(|_| SessionOperationError::MissingField("MSG_TYPE"))?;

        let seq_number = get_msg_seq_num(&message);

        // If msg seq > end_seq_number AND not ResendRequest: queue it
        if seq_number > self.end_seq_number && message_type != ResendRequest::MSG_TYPE {
            self.inbound_queue.push_back(message);
            return Ok(TransitionResult::Stay);
        }

        // Dispatch by message type
        let result = match message_type {
            Heartbeat::MSG_TYPE => self.on_heartbeat(ctx, &message).await?,
            TestRequest::MSG_TYPE => self.on_test_request(ctx, &message).await?,
            ResendRequest::MSG_TYPE => self.on_resend_request(ctx, &message).await?,
            Reject::MSG_TYPE => self.on_reject(ctx, &message).await?,
            SequenceReset::MSG_TYPE => self.on_sequence_reset(ctx, &message).await?,
            Logout::MSG_TYPE => self.on_logout(ctx, app, &message).await?,
            Logon::MSG_TYPE => {
                error!("received unexpected logon message");
                TransitionResult::Stay
            }
            _ => self.on_app_message(ctx, app, &message).await?,
        };

        // If a transition happened, return it directly
        if !matches!(result, TransitionResult::Stay) {
            return Ok(result);
        }

        // Check if resend is done
        self.check_end_of_resend(ctx)
    }

    fn check_end_of_resend<Store: MessageStore>(
        &mut self,
        ctx: &SessionCtx<'_, Store>,
    ) -> Result<TransitionResult, SessionOperationError> {
        if ctx.store.next_target_seq_number() > self.end_seq_number {
            let new_state =
                SessionState::new_active(self.writer.clone(), ctx.config.heartbeat_interval);
            let backlog = std::mem::take(&mut self.inbound_queue);
            Ok(TransitionResult::TransitionWithBacklog { new_state, backlog })
        } else {
            Ok(TransitionResult::Stay)
        }
    }

    async fn on_heartbeat<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, true, true)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self.handle_seq_too_high(ctx, expected, actual).await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        ctx.store.increment_target_seq_number().await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_test_request<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, true, true)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self.handle_seq_too_high(ctx, expected, actual).await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        let req_id: &str = message.get(TEST_REQ_ID).unwrap_or_else(|_| todo!());

        ctx.store.increment_target_seq_number().await?;

        ctx.send_message(&self.writer, Heartbeat::for_request(req_id.to_string()))
            .await
            .with_send_context("heartbeat response")?;

        Ok(TransitionResult::Stay)
    }

    async fn on_resend_request<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, false, true)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { .. } => {
                // check_too_high=false, shouldn't happen
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        let msg_seq_num = get_msg_seq_num(message);
        let expected = ctx.store.next_target_seq_number();

        // If seq is too high, queue it for seq accounting when the gap fill catches up,
        // but still process the resend below.
        if msg_seq_num > expected {
            self.inbound_queue.push_back(message.clone());
        }

        let begin_seq_number: u64 = match message.get(BEGIN_SEQ_NO) {
            Ok(seq_number) => seq_number,
            Err(_) => {
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::RequiredTagMissing)
                    .text("missing begin sequence number for resend request");
                ctx.send_message(&self.writer, reject)
                    .await
                    .with_send_context("reject for missing BEGIN_SEQ_NO")?;
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
                return Ok(TransitionResult::Stay);
            }
        };

        if msg_seq_num == expected {
            ctx.store.increment_target_seq_number().await?;
        }

        ctx.resend_messages(&self.writer, begin_seq_number, end_seq_number)
            .await?;

        Ok(TransitionResult::Stay)
    }

    async fn on_reject<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, false, true)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self.handle_seq_too_high(ctx, expected, actual).await;
            }
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        ctx.store.increment_target_seq_number().await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_sequence_reset<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        let msg_seq_num = get_msg_seq_num(message);
        let is_gap_fill: bool = message.get(GAP_FILL_FLAG).unwrap_or(false);
        match ctx
            .verify_and_handle(&self.writer, message, is_gap_fill, is_gap_fill)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self.handle_seq_too_high(ctx, expected, actual).await;
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
            return Ok(TransitionResult::Stay);
        }

        ctx.store.set_target_seq_number(end - 1).await?;
        Ok(TransitionResult::Stay)
    }

    async fn on_logout<App: Application, Store: MessageStore>(
        &self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, false, false)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { .. } => {}
            VerifyResult::Handled(transition) => return Ok(transition),
        }

        // We are in AwaitingResend (logged on), send logout response
        let logout = Logout::with_reason("Logout acknowledged".to_string());
        match ctx.prepare_message(logout).await {
            Ok(prepared) => self.writer.send_raw_message(prepared.raw).await,
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
        message: &Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        match ctx
            .verify_and_handle(&self.writer, message, true, true)
            .await?
        {
            VerifyResult::Passed => {}
            VerifyResult::SeqTooHigh { expected, actual } => {
                return self.handle_seq_too_high(ctx, expected, actual).await;
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
            }
            InboundDecision::TerminateSession => {
                error!("failed to send inbound message to application");
                self.writer.disconnect().await;
            }
        }
        ctx.store.increment_target_seq_number().await?;

        Ok(TransitionResult::Stay)
    }

    async fn handle_seq_too_high<Store: MessageStore>(
        &mut self,
        ctx: &mut SessionCtx<'_, Store>,
        expected: u64,
        actual: u64,
    ) -> Result<TransitionResult, SessionOperationError> {
        match self.update(expected, actual) {
            AwaitingResendTransitionOutcome::Success => {
                debug!(
                    "we are behind target (ours: {expected}, theirs: {actual}), requesting resend."
                );
                let request = ResendRequest::new(expected, actual);
                ctx.send_message(&self.writer, request)
                    .await
                    .with_send_context("resend request")?;
                Ok(TransitionResult::Stay)
            }
            AwaitingResendTransitionOutcome::BeginSeqNumberTooLow => {
                self.writer.disconnect().await;
                Ok(TransitionResult::TransitionTo(
                    SessionState::new_disconnected(
                        false,
                        "awaiting resend begin seq number unexpectedly lower than the previous resend request's",
                    ),
                ))
            }
            AwaitingResendTransitionOutcome::AttemptsExceeded => {
                self.writer.disconnect().await;
                Ok(TransitionResult::TransitionTo(
                    SessionState::new_disconnected(
                        false,
                        "resend request attempts exceeded, manual intervention required",
                    ),
                ))
            }
        }
    }
}

pub(crate) enum AwaitingResendTransitionOutcome {
    Success,
    BeginSeqNumberTooLow,
    AttemptsExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn test_update_begin_seq_number_too_low() {
        let writer = create_writer_ref();
        let mut state = AwaitingResendState::new(writer, 1, 5);
        let result = state.update(0, 5);
        assert!(matches!(
            result,
            AwaitingResendTransitionOutcome::BeginSeqNumberTooLow
        ));
    }

    #[test]
    fn test_update_attempts_exceeded() {
        let writer = create_writer_ref();
        let mut state = AwaitingResendState::new(writer, 1, 5);

        // we can update twice more without hitting the limit
        let result = state.update(1, 5);
        assert!(matches!(result, AwaitingResendTransitionOutcome::Success));
        let result = state.update(1, 5);
        assert!(matches!(result, AwaitingResendTransitionOutcome::Success));

        // the fourth time with the same begin seq number, we get an error
        let result = state.update(1, 5);
        assert!(matches!(
            result,
            AwaitingResendTransitionOutcome::AttemptsExceeded
        ));
    }

    #[test]
    fn test_update_resets_attempts_on_new_begin_seq() {
        let writer = create_writer_ref();
        let mut state = AwaitingResendState::new(writer, 1, 5);

        // Use up attempts on begin=1
        let result = state.update(1, 5);
        assert!(matches!(result, AwaitingResendTransitionOutcome::Success));
        let result = state.update(1, 5);
        assert!(matches!(result, AwaitingResendTransitionOutcome::Success));

        // A new begin_seq resets the counter
        let result = state.update(3, 10);
        assert!(matches!(result, AwaitingResendTransitionOutcome::Success));
        assert_eq!(state.resend_attempts, 1);
    }

    fn create_writer_ref() -> WriterRef {
        let (sender, _) = mpsc::channel(10);
        WriterRef::new(sender)
    }
}
