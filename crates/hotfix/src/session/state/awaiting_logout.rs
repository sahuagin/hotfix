use crate::Application;
use crate::message::verification::VerificationFlags;
use crate::session::ctx::{SessionCtx, TransitionResult, VerificationResult};
use crate::session::error::SessionOperationError;
use crate::session::inbound::{self, VerificationOutcome};
use crate::transport::writer::WriterRef;
use hotfix_message::message::Message;
use hotfix_store::MessageStore;
use tokio::time::Instant;
use tracing::warn;

pub(crate) struct AwaitingLogoutState {
    /// The writer's reference to send messages to the counterparty
    pub(crate) writer: WriterRef,
    /// When we are expecting the Logout response at the latest. `None` once
    /// the timeout has fired and the disconnect is under way (see
    /// `AwaitingLogonState::logon_timeout`).
    pub(crate) logout_timeout: Option<Instant>,
    /// Indicates whether we should attempt to reconnect after we've fully logged out
    pub(crate) reconnect: bool,
}

impl AwaitingLogoutState {
    pub(crate) async fn handle_verification_issue<A: Application, S: MessageStore>(
        &self,
        ctx: &mut SessionCtx<A, S>,
        message: &Message,
        flags: VerificationFlags,
    ) -> Result<VerificationResult, SessionOperationError> {
        match inbound::verify_and_handle_errors(ctx, &self.writer, message, flags).await {
            VerificationOutcome::Ok => Ok(VerificationResult::Passed),
            VerificationOutcome::Handled(result) => Ok(VerificationResult::Issue(result)),
            VerificationOutcome::SequenceGap { expected, actual } => {
                warn!(
                    "sequence gap detected while awaiting logout (expected {expected}, actual {actual}), ignoring"
                );
                Ok(VerificationResult::Issue(TransitionResult::Stay))
            }
        }
    }
}
