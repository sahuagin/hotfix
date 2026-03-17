use crate::message::logout::Logout;
use crate::message::parser::RawFixMessage;
use crate::message::reject::Reject;
use crate::message::sequence_reset::SequenceReset;
use crate::message::verification::verify_message as verify_message_impl;
use crate::message::verification_error::{CompIdType, MessageVerificationError};
use crate::message::{generate_message, is_admin, prepare_message_for_resend};
use crate::session::ctx::{SessionCtx, TransitionResult, VerifyResult};
use crate::session::error::{InternalSendResultExt, SessionOperationError};
use crate::session::get_msg_seq_num;
use crate::session::state::SessionState;
use crate::transport::writer::WriterRef;
use hotfix_message::Part;
use hotfix_message::message::Message;
use hotfix_message::parsed_message::InvalidReason;
use hotfix_message::session_fields::{MSG_SEQ_NUM, MSG_TYPE, SessionRejectReason};
use hotfix_store::MessageStore;
use tracing::{debug, enabled, error, info, warn};

fn verify_message<Store: MessageStore>(
    ctx: &SessionCtx<'_, Store>,
    message: &Message,
    check_too_high: bool,
    check_too_low: bool,
) -> Result<(), MessageVerificationError> {
    let expected_seq_number = if check_too_high || check_too_low {
        Some(ctx.store.next_target_seq_number())
    } else {
        None
    };
    verify_message_impl(
        message,
        ctx.config,
        expected_seq_number,
        check_too_high,
        check_too_low,
    )
}

/// Verify a message and handle the error if verification fails.
/// For SeqNumberTooHigh, returns `VerifyResult::SeqTooHigh` instead of handling it,
/// allowing the caller to handle the transition.
pub async fn verify_and_handle<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    message: &Message,
    check_too_high: bool,
    check_too_low: bool,
) -> Result<VerifyResult, SessionOperationError> {
    match verify_message(ctx, message, check_too_high, check_too_low) {
        Ok(()) => Ok(VerifyResult::Passed),
        Err(MessageVerificationError::SeqNumberTooHigh { expected, actual }) => {
            Ok(VerifyResult::SeqTooHigh { expected, actual })
        }
        Err(err) => {
            let transition = handle_verification_error(ctx, writer, err).await?;
            Ok(VerifyResult::Handled(transition))
        }
    }
}

/// Handle a verification error (excluding SeqNumberTooHigh which is returned separately).
/// Returns the `TransitionResult` to use — either `Stay` (error was handled in-place)
/// or `TransitionTo` (a state change is needed).
async fn handle_verification_error<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    error: MessageVerificationError,
) -> Result<TransitionResult, SessionOperationError> {
    match error {
        MessageVerificationError::SeqNumberTooLow {
            expected,
            actual,
            possible_duplicate,
        } => Ok(
            handle_sequence_number_too_low(ctx, writer, expected, actual, possible_duplicate).await,
        ),
        MessageVerificationError::SeqNumberTooHigh { expected, actual } => {
            // This shouldn't be called for SeqTooHigh anymore (it's returned via VerifyResult),
            // but handle gracefully if it is.
            warn!(
                "handle_verification_error called with SeqNumberTooHigh({expected}, {actual}) - caller should use verify_and_handle"
            );
            Ok(TransitionResult::Stay)
        }
        MessageVerificationError::IncorrectBeginString(begin_string) => {
            let new_state = handle_incorrect_begin_string(ctx, writer, begin_string).await;
            Ok(TransitionResult::TransitionTo(new_state))
        }
        MessageVerificationError::IncorrectCompId {
            comp_id,
            comp_id_type,
            msg_seq_num,
        } => {
            let new_state =
                handle_incorrect_comp_id(ctx, writer, comp_id, comp_id_type, msg_seq_num).await;
            Ok(TransitionResult::TransitionTo(new_state))
        }
        MessageVerificationError::SendingTimeAccuracyIssue { msg_seq_num } => {
            handle_sending_time_accuracy_problem(
                ctx,
                writer,
                msg_seq_num,
                "unexpected sending time",
            )
            .await;
            Ok(TransitionResult::Stay)
        }
        MessageVerificationError::SendingTimeMissing { msg_seq_num } => {
            handle_sending_time_accuracy_problem(ctx, writer, msg_seq_num, "sending time missing")
                .await;
            Ok(TransitionResult::Stay)
        }
        MessageVerificationError::OriginalSendingTimeMissing { msg_seq_num } => {
            handle_original_sending_time_missing(ctx, writer, msg_seq_num).await;
            Ok(TransitionResult::Stay)
        }
        MessageVerificationError::OriginalSendingTimeAfterSendingTime { msg_seq_num, .. } => {
            handle_sending_time_accuracy_problem(
                ctx,
                writer,
                msg_seq_num,
                "original sending time is after sending time",
            )
            .await;
            Ok(TransitionResult::Stay)
        }
    }
}

async fn handle_incorrect_begin_string<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    received_begin_string: String,
) -> SessionState {
    logout_and_terminate(
        ctx,
        writer,
        &format!("beginString={received_begin_string} is not supported"),
    )
    .await;
    SessionState::new_disconnected(true, "incorrect begin string")
}

async fn handle_incorrect_comp_id<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    received_comp_id: String,
    comp_id_type: CompIdType,
    msg_seq_num: u64,
) -> SessionState {
    error!("rejecting message with incorrect comp ID: {received_comp_id} (type: {comp_id_type:?})");
    let reject = Reject::new(msg_seq_num)
        .session_reject_reason(SessionRejectReason::ValueIsIncorrect)
        .text(&format!("invalid comp ID {received_comp_id}"));
    if let Err(err) = ctx.send_message(writer, reject).await {
        error!("failed to send reject message with invalid comp ID: {err}");
    }
    logout_and_terminate(ctx, writer, "incorrect comp ID received").await;
    SessionState::new_disconnected(true, "incorrect comp ID")
}

async fn handle_sequence_number_too_low<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
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
    logout_and_terminate(ctx, writer, &reason).await;
    TransitionResult::TransitionTo(SessionState::new_disconnected(false, &reason))
}

async fn handle_sending_time_accuracy_problem<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    msg_seq_num: u64,
    text: &str,
) {
    let reject = Reject::new(msg_seq_num)
        .session_reject_reason(SessionRejectReason::SendingtimeAccuracyProblem)
        .text(text);
    if let Err(err) = ctx.send_message(writer, reject).await {
        error!("failed to send reject for time accuracy problem: {err}");
    }
    if let Err(err) = ctx.store.increment_target_seq_number().await {
        error!("failed to increment target seq number: {:?}", err);
    }
}

async fn handle_original_sending_time_missing<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    msg_seq_num: u64,
) {
    let reject = Reject::new(msg_seq_num)
        .session_reject_reason(SessionRejectReason::RequiredTagMissing)
        .text("original sending time is required");
    if let Err(err) = ctx.send_message(writer, reject).await {
        error!("failed to send reject for time missing tag: {err}");
    }
    if let Err(err) = ctx.store.increment_target_seq_number().await {
        error!("failed to increment target seq number: {:?}", err);
    }
}

/// Send a logout message and immediately disconnect.
pub(crate) async fn logout_and_terminate<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    reason: &str,
) {
    let logout = Logout::with_reason(reason.to_string());
    match ctx.prepare_message(logout).await {
        Ok(prepared) => writer.send_raw_message(prepared.raw).await,
        Err(err) => warn!("failed to send logout during session termination: {err}"),
    }
    writer.disconnect().await;
}

pub async fn resend_messages<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    begin: u64,
    end: u64,
) -> Result<(), SessionOperationError> {
    info!(begin, end, "resending messages as requested");
    let messages = ctx.store.get_slice(begin as usize, end as usize).await?;

    let no = messages.len();
    debug!(number_of_messages = no, "number of messages");

    let mut reset_start: Option<u64> = None;
    let mut sequence_number = 0;

    for msg in messages {
        let mut message = ctx
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
            log_skipped_admin_messages(begin, end);
            send_sequence_reset(ctx, writer, begin, end).await?;
            reset_start = None;
        }

        if let Err(e) = prepare_message_for_resend(&mut message) {
            error!(
                error = e,
                "failed to prepare message for resend, sending original"
            );
        }
        writer
            .send_raw_message(RawFixMessage::new(message.encode(ctx.message_config)?))
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
        log_skipped_admin_messages(begin, end);
        send_sequence_reset(ctx, writer, begin, end).await?;
    }

    Ok(())
}

async fn send_sequence_reset<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    begin: u64,
    end: u64,
) -> Result<(), SessionOperationError> {
    let sequence_reset = SequenceReset {
        gap_fill: true,
        new_seq_no: end,
    };
    let raw_message = generate_message(
        &ctx.config.begin_string,
        &ctx.config.sender_comp_id,
        &ctx.config.target_comp_id,
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

pub async fn handle_invalid_parsed_message<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
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
                ctx.send_message(writer, reject)
                    .await
                    .with_send_context("reject for invalid field")?;
            }
        }
        InvalidReason::InvalidComponent(_component_name) => {
            warn!("received invalid component");
        }
        InvalidReason::InvalidMsgType(msg_type) => {
            handle_invalid_msg_type(ctx, writer, message, &msg_type).await;
        }
        InvalidReason::InvalidOrderInGroup { tag, .. } => {
            if let Ok(msg_seq_num) = message.header().get(MSG_SEQ_NUM) {
                let reject = Reject::new(msg_seq_num)
                    .session_reject_reason(SessionRejectReason::RepeatingGroupFieldsOutOfOrder)
                    .text(&format!("field appears in incorrect order:{tag}"));
                ctx.send_message(writer, reject)
                    .await
                    .with_send_context("reject for invalid group order")?;
            }
        }
    }
    Ok(())
}

async fn handle_invalid_msg_type<Store: MessageStore>(
    ctx: &mut SessionCtx<'_, Store>,
    writer: &WriterRef,
    message: &Message,
    msg_type: &str,
) {
    match message.header().get(MSG_SEQ_NUM) {
        Ok(msg_seq_num) => {
            let reject = Reject::new(msg_seq_num)
                .session_reject_reason(SessionRejectReason::InvalidMsgtype)
                .text(&format!("invalid message type {msg_type}"));
            if let Err(err) = ctx.send_message(writer, reject).await {
                error!("failed to send reject message for invalid msgtype: {err}");
            }

            #[allow(clippy::collapsible_if)]
            if let Ok(seq_num) = message.header().get::<u64>(MSG_SEQ_NUM)
                && ctx.store.next_target_seq_number() == seq_num
            {
                if let Err(err) = ctx.store.increment_target_seq_number().await {
                    error!("failed to increment target seq number: {:?}", err);
                }
            }
        }
        Err(err) => {
            error!("failed to get message seq num: {:?}", err);
        }
    }
}
