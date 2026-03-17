use crate::config::SessionConfig;
use crate::message::logout::Logout;
use crate::message::parser::RawFixMessage;
use crate::message::reject::Reject;
use crate::message::sequence_reset::SequenceReset;
use crate::message::verification::verify_message as verify_message_impl;
use crate::message::verification_error::{CompIdType, MessageVerificationError};
use crate::message::{OutboundMessage, generate_message, is_admin, prepare_message_for_resend};
use crate::session::error::{InternalSendError, InternalSendResultExt, SessionOperationError};
use crate::session::get_msg_seq_num;
use crate::session::state::SessionState;
use crate::store::StoreError;
use crate::transport::writer::WriterRef;
use hotfix_message::message::{Config as MessageConfig, Message};
use hotfix_message::parsed_message::InvalidReason;
use hotfix_message::session_fields::{MSG_SEQ_NUM, MSG_TYPE, SessionRejectReason};
use hotfix_message::{MessageBuilder, Part};
use hotfix_store::MessageStore;
use std::collections::VecDeque;
use tracing::{debug, enabled, error, info, warn};

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
    Handled(TransitionResult),
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
                Ok(VerifyResult::Handled(transition))
            }
        }
    }

    /// Handle a verification error (excluding SeqNumberTooHigh which is returned separately).
    /// Returns the `TransitionResult` to use — either `Stay` (error was handled in-place)
    /// or `TransitionTo` (a state change is needed).
    pub async fn handle_verification_error(
        &mut self,
        writer: &WriterRef,
        error: MessageVerificationError,
    ) -> Result<TransitionResult, SessionOperationError> {
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
                Ok(TransitionResult::Stay)
            }
            MessageVerificationError::IncorrectBeginString(begin_string) => {
                let new_state = self
                    .handle_incorrect_begin_string(writer, begin_string)
                    .await;
                Ok(TransitionResult::TransitionTo(new_state))
            }
            MessageVerificationError::IncorrectCompId {
                comp_id,
                comp_id_type,
                msg_seq_num,
            } => {
                let new_state = self
                    .handle_incorrect_comp_id(writer, comp_id, comp_id_type, msg_seq_num)
                    .await;
                Ok(TransitionResult::TransitionTo(new_state))
            }
            MessageVerificationError::SendingTimeAccuracyIssue { msg_seq_num } => {
                self.handle_sending_time_accuracy_problem(
                    writer,
                    msg_seq_num,
                    "unexpected sending time",
                )
                .await;
                Ok(TransitionResult::Stay)
            }
            MessageVerificationError::SendingTimeMissing { msg_seq_num } => {
                self.handle_sending_time_accuracy_problem(
                    writer,
                    msg_seq_num,
                    "sending time missing",
                )
                .await;
                Ok(TransitionResult::Stay)
            }
            MessageVerificationError::OriginalSendingTimeMissing { msg_seq_num } => {
                self.handle_original_sending_time_missing(writer, msg_seq_num)
                    .await;
                Ok(TransitionResult::Stay)
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
                Ok(TransitionResult::Stay)
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
    ) -> TransitionResult {
        if possible_duplicate {
            warn!(
                "sequence number too low (expected {expected}, actual {actual}, but counterparty indicated it's poss duplicate, ignoring"
            );
            return TransitionResult::Stay;
        }
        error!(
            "we expected {expected} sequence number, but target sent lower ({actual}), terminating..."
        );
        let reason = format!("sequence number too low (actual {actual}, expected {expected})");
        self.logout_and_terminate(writer, &reason).await;
        TransitionResult::TransitionTo(SessionState::new_disconnected(false, &reason))
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

    pub async fn handle_invalid_parsed_message(
        &mut self,
        writer: &WriterRef,
        message: &Message,
        reason: InvalidReason,
    ) -> Result<(), SessionOperationError> {
        match reason {
            InvalidReason::InvalidField(tag) | InvalidReason::InvalidGroup(tag) => {
                if let Ok(msg_seq_num) = message.header().get(MSG_SEQ_NUM) {
                    let reject = Reject::new(msg_seq_num)
                        .session_reject_reason(SessionRejectReason::InvalidTagNumber)
                        .text(&format!("invalid field {tag}"));
                    self.send_message(writer, reject)
                        .await
                        .with_send_context("reject for invalid field")?;
                }
            }
            InvalidReason::InvalidComponent(_component_name) => {
                warn!("received invalid component");
            }
            InvalidReason::InvalidMsgType(msg_type) => {
                self.handle_invalid_msg_type(writer, message, &msg_type)
                    .await;
            }
            InvalidReason::InvalidOrderInGroup { tag, .. } => {
                if let Ok(msg_seq_num) = message.header().get(MSG_SEQ_NUM) {
                    let reject = Reject::new(msg_seq_num)
                        .session_reject_reason(SessionRejectReason::RepeatingGroupFieldsOutOfOrder)
                        .text(&format!("field appears in incorrect order:{tag}"));
                    self.send_message(writer, reject)
                        .await
                        .with_send_context("reject for invalid group order")?;
                }
            }
        }
        Ok(())
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
