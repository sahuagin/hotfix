mod active;
mod awaiting_logon;
mod awaiting_logout;
mod awaiting_resend;
mod disconnected;

pub(crate) use active::{ActiveState, calculate_peer_interval};
pub(crate) use awaiting_logon::AwaitingLogonState;
pub(crate) use awaiting_logout::AwaitingLogoutState;
pub(crate) use awaiting_resend::AwaitingResendState;
#[cfg(test)]
pub(crate) use awaiting_resend::AwaitingResendTransitionOutcome;
pub(crate) use disconnected::DisconnectedState;

use crate::config::SessionConfig;
use crate::message::logout::Logout;
use crate::message::parser::RawFixMessage;
use crate::message::reject::Reject;
use crate::message::sequence_reset::SequenceReset;
use crate::message::verification::verify_message as verify_message_impl;
use crate::message::verification_error::{CompIdType, MessageVerificationError};
use crate::message::{OutboundMessage, generate_message, is_admin, prepare_message_for_resend};
use crate::session::error::{InternalSendError, SessionOperationError};
use crate::session::event::AwaitingActiveSessionResponse;
use crate::session::get_msg_seq_num;
use crate::session::info::Status as SessionInfoStatus;
use crate::store::StoreError;
use crate::transport::writer::WriterRef;
use hotfix_message::message::{Config as MessageConfig, Message};
use hotfix_message::session_fields::SessionRejectReason;
use hotfix_message::{MessageBuilder, Part};
use hotfix_store::MessageStore;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, enabled, error, info, warn};

use hotfix_message::session_fields::{MSG_SEQ_NUM, MSG_TYPE};

pub(crate) struct SessionCtx<'a, Store> {
    pub config: &'a SessionConfig,
    pub store: &'a mut Store,
    pub message_builder: &'a MessageBuilder,
    pub message_config: &'a MessageConfig,
}

pub(crate) struct PreparedMessage {
    pub seq_num: u64,
    #[allow(dead_code)]
    pub msg_type: String,
    pub raw: RawFixMessage,
}

pub(crate) enum TransitionResult {
    Stay,
    TransitionTo(SessionState),
    TransitionWithBacklog {
        new_state: SessionState,
        backlog: VecDeque<Message>,
    },
}

pub(crate) enum VerifyResult {
    Passed,
    SeqTooHigh { expected: u64, actual: u64 },
    ErrorHandled(Option<SessionState>),
}

impl<Store: MessageStore> SessionCtx<'_, Store> {
    pub async fn prepare_message(
        &mut self,
        message: impl OutboundMessage,
    ) -> Result<PreparedMessage, InternalSendError> {
        let seq_num = self.store.next_sender_seq_number();
        let msg_type = message.message_type().to_string();
        let msg = generate_message(
            &self.config.begin_string,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            seq_num,
            message,
        )
        .map_err(|e| {
            InternalSendError::Persist(StoreError::PersistMessage {
                sequence_number: seq_num,
                source: e.into(),
            })
        })?;

        self.store
            .increment_sender_seq_number()
            .await
            .map_err(InternalSendError::SequenceNumber)?;
        self.store
            .add(seq_num, &msg)
            .await
            .map_err(InternalSendError::Persist)?;

        Ok(PreparedMessage {
            seq_num,
            msg_type,
            raw: RawFixMessage::new(msg),
        })
    }

    /// Prepare, persist, and send a message via the given writer.
    pub async fn send_message(
        &mut self,
        writer: &WriterRef,
        message: impl OutboundMessage,
    ) -> Result<u64, InternalSendError> {
        let prepared = self.prepare_message(message).await?;
        writer.send_raw_message(prepared.raw).await;
        Ok(prepared.seq_num)
    }

    pub fn verify_message(
        &self,
        message: &Message,
        check_too_high: bool,
        check_too_low: bool,
    ) -> Result<(), MessageVerificationError> {
        let expected_seq_number = if check_too_high || check_too_low {
            Some(self.store.next_target_seq_number())
        } else {
            None
        };
        verify_message_impl(
            message,
            self.config,
            expected_seq_number,
            check_too_high,
            check_too_low,
        )
    }

    /// Verify a message and handle the error if verification fails.
    /// For SeqNumberTooHigh, returns `VerifyResult::SeqTooHigh` instead of handling it,
    /// allowing the caller to handle the transition.
    pub async fn verify_and_handle(
        &mut self,
        writer: &WriterRef,
        message: &Message,
        check_too_high: bool,
        check_too_low: bool,
    ) -> Result<VerifyResult, SessionOperationError> {
        match self.verify_message(message, check_too_high, check_too_low) {
            Ok(()) => Ok(VerifyResult::Passed),
            Err(MessageVerificationError::SeqNumberTooHigh { expected, actual }) => {
                Ok(VerifyResult::SeqTooHigh { expected, actual })
            }
            Err(err) => {
                let transition = self.handle_verification_error(writer, err).await?;
                Ok(VerifyResult::ErrorHandled(transition))
            }
        }
    }

    /// Handle a verification error (excluding SeqNumberTooHigh which is returned separately).
    /// Returns `Some(new_state)` if a state transition is needed.
    pub async fn handle_verification_error(
        &mut self,
        writer: &WriterRef,
        error: MessageVerificationError,
    ) -> Result<Option<SessionState>, SessionOperationError> {
        match error {
            MessageVerificationError::SeqNumberTooLow {
                expected,
                actual,
                possible_duplicate,
            } => Ok(self
                .handle_sequence_number_too_low(writer, expected, actual, possible_duplicate)
                .await),
            MessageVerificationError::SeqNumberTooHigh { expected, actual } => {
                // This shouldn't be called for SeqTooHigh anymore (it's returned via VerifyResult),
                // but handle gracefully if it is.
                warn!(
                    "handle_verification_error called with SeqNumberTooHigh({expected}, {actual}) - caller should use verify_and_handle"
                );
                Ok(None)
            }
            MessageVerificationError::IncorrectBeginString(begin_string) => Ok(Some(
                self.handle_incorrect_begin_string(writer, begin_string)
                    .await,
            )),
            MessageVerificationError::IncorrectCompId {
                comp_id,
                comp_id_type,
                msg_seq_num,
            } => Ok(Some(
                self.handle_incorrect_comp_id(writer, comp_id, comp_id_type, msg_seq_num)
                    .await,
            )),
            MessageVerificationError::SendingTimeAccuracyIssue { msg_seq_num } => {
                self.handle_sending_time_accuracy_problem(
                    writer,
                    msg_seq_num,
                    "unexpected sending time",
                )
                .await;
                Ok(None)
            }
            MessageVerificationError::SendingTimeMissing { msg_seq_num } => {
                self.handle_sending_time_accuracy_problem(
                    writer,
                    msg_seq_num,
                    "sending time missing",
                )
                .await;
                Ok(None)
            }
            MessageVerificationError::OriginalSendingTimeMissing { msg_seq_num } => {
                self.handle_original_sending_time_missing(writer, msg_seq_num)
                    .await;
                Ok(None)
            }
            MessageVerificationError::OriginalSendingTimeAfterSendingTime {
                msg_seq_num, ..
            } => {
                self.handle_sending_time_accuracy_problem(
                    writer,
                    msg_seq_num,
                    "original sending time is after sending time",
                )
                .await;
                Ok(None)
            }
        }
    }

    async fn handle_incorrect_begin_string(
        &mut self,
        writer: &WriterRef,
        received_begin_string: String,
    ) -> SessionState {
        self.logout_and_terminate(
            writer,
            &format!("beginString={received_begin_string} is not supported"),
        )
        .await;
        SessionState::new_disconnected(true, "incorrect begin string")
    }

    async fn handle_incorrect_comp_id(
        &mut self,
        writer: &WriterRef,
        received_comp_id: String,
        comp_id_type: CompIdType,
        msg_seq_num: u64,
    ) -> SessionState {
        error!(
            "rejecting message with incorrect comp ID: {received_comp_id} (type: {comp_id_type:?})"
        );
        let reject = Reject::new(msg_seq_num)
            .session_reject_reason(SessionRejectReason::ValueIsIncorrect)
            .text(&format!("invalid comp ID {received_comp_id}"));
        if let Err(err) = self.send_message(writer, reject).await {
            error!("failed to send reject message with invalid comp ID: {err}");
        }
        self.logout_and_terminate(writer, "incorrect comp ID received")
            .await;
        SessionState::new_disconnected(true, "incorrect comp ID")
    }

    async fn handle_sequence_number_too_low(
        &mut self,
        writer: &WriterRef,
        expected: u64,
        actual: u64,
        possible_duplicate: bool,
    ) -> Option<SessionState> {
        if possible_duplicate {
            warn!(
                "sequence number too low (expected {expected}, actual {actual}, but counterparty indicated it's poss duplicate, ignoring"
            );
            return None;
        }
        error!(
            "we expected {expected} sequence number, but target sent lower ({actual}), terminating..."
        );
        let reason = format!("sequence number too low (actual {actual}, expected {expected})");
        self.logout_and_terminate(writer, &reason).await;
        Some(SessionState::new_disconnected(false, &reason))
    }

    async fn handle_sending_time_accuracy_problem(
        &mut self,
        writer: &WriterRef,
        msg_seq_num: u64,
        text: &str,
    ) {
        let reject = Reject::new(msg_seq_num)
            .session_reject_reason(SessionRejectReason::SendingtimeAccuracyProblem)
            .text(text);
        if let Err(err) = self.send_message(writer, reject).await {
            error!("failed to send reject for time accuracy problem: {err}");
        }
        if let Err(err) = self.store.increment_target_seq_number().await {
            error!("failed to increment target seq number: {:?}", err);
        }
    }

    async fn handle_original_sending_time_missing(&mut self, writer: &WriterRef, msg_seq_num: u64) {
        let reject = Reject::new(msg_seq_num)
            .session_reject_reason(SessionRejectReason::RequiredTagMissing)
            .text("original sending time is required");
        if let Err(err) = self.send_message(writer, reject).await {
            error!("failed to send reject for time missing tag: {err}");
        }
        if let Err(err) = self.store.increment_target_seq_number().await {
            error!("failed to increment target seq number: {:?}", err);
        }
    }

    /// Send a logout message and immediately disconnect.
    pub(crate) async fn logout_and_terminate(&mut self, writer: &WriterRef, reason: &str) {
        let logout = Logout::with_reason(reason.to_string());
        match self.prepare_message(logout).await {
            Ok(prepared) => writer.send_raw_message(prepared.raw).await,
            Err(err) => warn!("failed to send logout during session termination: {err}"),
        }
        writer.disconnect().await;
    }

    pub async fn resend_messages(
        &mut self,
        writer: &WriterRef,
        begin: u64,
        end: u64,
    ) -> Result<(), SessionOperationError> {
        info!(begin, end, "resending messages as requested");
        let messages = self.store.get_slice(begin as usize, end as usize).await?;

        let no = messages.len();
        debug!(number_of_messages = no, "number of messages");

        let mut reset_start: Option<u64> = None;
        let mut sequence_number = 0;

        for msg in messages {
            let mut message = self
                .message_builder
                .build(msg.as_slice())
                .into_message()
                .ok_or_else(|| {
                    SessionOperationError::StoredMessageParse(format!(
                        "failed to build message for raw message: {msg:?}"
                    ))
                })?;
            sequence_number = get_msg_seq_num(&message);
            let message_type: String = message
                .header()
                .get::<&str>(MSG_TYPE)
                .map_err(|_| SessionOperationError::MissingField("MSG_TYPE"))?
                .to_string();

            if is_admin(&message_type) {
                if reset_start.is_none() {
                    reset_start = Some(sequence_number);
                }
                continue;
            }

            if let Some(begin) = reset_start {
                let end = sequence_number;
                Self::log_skipped_admin_messages(begin, end);
                self.send_sequence_reset(writer, begin, end).await?;
                reset_start = None;
            }

            if let Err(e) = prepare_message_for_resend(&mut message) {
                error!(
                    error = e,
                    "failed to prepare message for resend, sending original"
                );
            }
            writer
                .send_raw_message(RawFixMessage::new(message.encode(self.message_config)?))
                .await;

            if enabled!(tracing::Level::DEBUG)
                && let Ok(m) = String::from_utf8(msg.clone())
            {
                debug!(sequence_number, message = m, "resent message");
            }
        }

        if let Some(begin) = reset_start {
            // the final reset if needed
            let end = sequence_number;
            Self::log_skipped_admin_messages(begin, end);
            self.send_sequence_reset(writer, begin, end).await?;
        }

        Ok(())
    }

    pub async fn send_sequence_reset(
        &mut self,
        writer: &WriterRef,
        begin: u64,
        end: u64,
    ) -> Result<(), SessionOperationError> {
        let sequence_reset = SequenceReset {
            gap_fill: true,
            new_seq_no: end,
        };
        let raw_message = generate_message(
            &self.config.begin_string,
            &self.config.sender_comp_id,
            &self.config.target_comp_id,
            begin,
            sequence_reset,
        )?;

        writer
            .send_raw_message(RawFixMessage::new(raw_message))
            .await;
        debug!(begin, end, "sent reset sequence");

        Ok(())
    }

    fn log_skipped_admin_messages(begin: u64, end: u64) {
        info!(
            begin,
            end, "skipped admin message(s) during resend, requesting reset for these"
        );
    }

    pub async fn handle_invalid_msg_type(
        &mut self,
        writer: &WriterRef,
        message: &Message,
        msg_type: &str,
    ) {
        match message.header().get(MSG_SEQ_NUM) {
            Ok(msg_seq_num) => {
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::InvalidMsgtype)
                    .text(&format!("invalid message type {msg_type}"));
                if let Err(err) = self.send_message(writer, reject).await {
                    error!("failed to send reject message for invalid msgtype: {err}");
                }

                #[allow(clippy::collapsible_if)]
                if let Ok(seq_num) = message.header().get::<u64>(MSG_SEQ_NUM)
                    && self.store.next_target_seq_number() == seq_num
                {
                    if let Err(err) = self.store.increment_target_seq_number().await {
                        error!("failed to increment target seq number: {:?}", err);
                    }
                }
            }
            Err(err) => {
                error!("failed to get message seq num: {:?}", err);
            }
        }
    }
}

const TEST_REQUEST_THRESHOLD: f64 = 1.2;

pub(crate) type TestRequestId = String;

pub enum SessionState {
    /// We have established a connection, sent a logon message and await a response.
    AwaitingLogon(AwaitingLogonState),
    /// We are awaiting the target to resend the gap we have.
    AwaitingResend(AwaitingResendState),
    /// We are in the process of gracefully logging out
    AwaitingLogout(AwaitingLogoutState),
    /// The session is active, we have connected and mutually logged on.
    Active(ActiveState),
    /// The TCP connection has been dropped.
    ///
    /// This is also the state we're in if we purposefully disconnected due to the current
    /// time being out of session hours.
    Disconnected(DisconnectedState),
}

impl SessionState {
    pub fn new_disconnected(reconnect: bool, reason: &str) -> Self {
        Self::Disconnected(DisconnectedState::new(reconnect, reason))
    }

    pub fn new_active(writer: WriterRef, heartbeat_interval: u64) -> Self {
        let peer_interval = calculate_peer_interval(heartbeat_interval);

        Self::Active(ActiveState {
            writer,
            heartbeat_deadline: Instant::now() + Duration::from_secs(heartbeat_interval),
            peer_deadline: Instant::now() + Duration::from_secs(peer_interval),
            sent_test_request_id: None,
        })
    }

    pub fn should_reconnect(&self) -> bool {
        match self {
            SessionState::Disconnected(state) => state.should_reconnect(),
            _ => true,
        }
    }

    pub(crate) fn get_writer(&self) -> Option<&WriterRef> {
        match self {
            Self::Active(ActiveState { writer, .. })
            | Self::AwaitingLogon(AwaitingLogonState { writer, .. })
            | Self::AwaitingLogout(AwaitingLogoutState { writer, .. })
            | Self::AwaitingResend(AwaitingResendState { writer, .. }) => Some(writer),
            _ => None,
        }
    }

    pub fn try_transition_to_awaiting_logout(
        &mut self,
        logout_timeout: Duration,
        reconnect: bool,
    ) -> bool {
        if matches!(self, SessionState::AwaitingLogout(_)) {
            debug!("already in awaiting logout state");
            return false;
        }

        if let Some(writer) = self.get_writer() {
            *self = SessionState::AwaitingLogout(AwaitingLogoutState {
                writer: writer.clone(),
                logout_timeout: Instant::now() + logout_timeout,
                reconnect,
            });
            true
        } else {
            error!("trying to transition to awaiting logout without an established connection");
            false
        }
    }

    #[cfg(test)]
    pub fn try_transition_to_awaiting_resend(
        &mut self,
        begin: u64,
        end: u64,
    ) -> AwaitingResendTransitionOutcome {
        match self {
            SessionState::AwaitingLogon(AwaitingLogonState { writer, .. })
            | SessionState::Active(ActiveState { writer, .. }) => {
                let awaiting_resend = AwaitingResendState::new(writer.to_owned(), begin, end);
                *self = SessionState::AwaitingResend(awaiting_resend);
                AwaitingResendTransitionOutcome::Success
            }
            SessionState::AwaitingResend(state) => state.update(begin, end),
            SessionState::AwaitingLogout(_) => AwaitingResendTransitionOutcome::InvalidState(
                "trying to request a resend while we are already logging out".to_string(),
            ),
            SessionState::Disconnected(_) => AwaitingResendTransitionOutcome::InvalidState(
                "trying to transition to awaiting resend without an established connection"
                    .to_string(),
            ),
        }
    }

    pub fn register_session_awaiter(
        &mut self,
        responder: oneshot::Sender<AwaitingActiveSessionResponse>,
    ) {
        match self {
            SessionState::Disconnected(state) => {
                if let Err(responder) = state.register_session_awaiter(responder) {
                    let reason = &state.reason;
                    error!(
                        "session awaiter already registered on state disconnected due to: {reason}"
                    );
                    if let Err(err) = responder.send(AwaitingActiveSessionResponse::Shutdown) {
                        error!("failed to send session awaiter response: {err:?}");
                    }
                }
            }
            _ => {
                error!("session awaiter can only be registered on disconnected sessions");
                if let Err(err) = responder.send(AwaitingActiveSessionResponse::Shutdown) {
                    error!("failed to send session awaiter response: {err:?}");
                }
            }
        }
    }

    pub fn notify_session_awaiter(&mut self) {
        if let SessionState::Disconnected(state) = self {
            state.notify_session_awaiter();
        }
    }

    pub fn heartbeat_deadline(&self) -> Option<&Instant> {
        match self {
            Self::Active(state) => Some(state.heartbeat_deadline()),
            _ => None,
        }
    }

    pub fn peer_deadline(&self) -> Option<&Instant> {
        match self {
            Self::Active(state) => Some(state.peer_deadline()),
            Self::AwaitingLogon(AwaitingLogonState { logon_timeout, .. }) => Some(logon_timeout),
            Self::AwaitingLogout(AwaitingLogoutState { logout_timeout, .. }) => {
                Some(logout_timeout)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn is_logged_on(&self) -> bool {
        matches!(self, SessionState::Active(_))
            || matches!(self, SessionState::AwaitingResend { .. })
    }

    pub fn as_status(&self) -> SessionInfoStatus {
        match self {
            SessionState::AwaitingLogon(_) => SessionInfoStatus::AwaitingLogon,
            SessionState::AwaitingResend(AwaitingResendState {
                begin_seq_number,
                end_seq_number,
                resend_attempts,
                ..
            }) => SessionInfoStatus::AwaitingResend {
                begin: *begin_seq_number,
                end: *end_seq_number,
                attempts: *resend_attempts,
            },
            SessionState::AwaitingLogout(_) => SessionInfoStatus::AwaitingLogout,
            SessionState::Active(_) => SessionInfoStatus::Active,
            SessionState::Disconnected(_) => SessionInfoStatus::Disconnected,
        }
    }
}
