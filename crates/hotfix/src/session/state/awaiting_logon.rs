use crate::Application;
use crate::message::logon::Logon;
use crate::session::error::SessionOperationError;
use crate::session::message_handling;
use crate::session::state::{SessionCtx, SessionState, TransitionResult, VerifyResult};
use crate::transport::writer::WriterRef;
use hotfix_message::Part;
use hotfix_message::session_fields::MSG_TYPE;
use hotfix_store::MessageStore;
use tokio::time::Instant;
use tracing::warn;

pub(crate) struct AwaitingLogonState {
    pub(crate) writer: WriterRef,
    pub(crate) logon_timeout: Instant,
}

impl AwaitingLogonState {
    pub(crate) async fn on_disconnect(&self, reason: &str) -> SessionState {
        self.writer.disconnect().await;
        SessionState::new_disconnected(true, reason)
    }

    pub(crate) async fn on_peer_timeout(&self) {
        warn!("peer didn't respond to our Logon, disconnecting..");
        self.writer.disconnect().await;
    }

    pub(crate) async fn on_fix_message<App: Application, Store: MessageStore>(
        &self,
        ctx: &mut SessionCtx<'_, Store>,
        app: &mut App,
        message: hotfix_message::message::Message,
    ) -> Result<TransitionResult, SessionOperationError> {
        let message_type: &str = message
            .header()
            .get(MSG_TYPE)
            .map_err(|_| SessionOperationError::MissingField("MSG_TYPE"))?;

        if message_type != Logon::MSG_TYPE {
            self.writer.disconnect().await;
            return Ok(TransitionResult::Stay);
        }

        // process logon
        match message_handling::verify_and_handle(ctx, &self.writer, &message, true, true).await? {
            VerifyResult::Passed => {
                // happy logon flow, the session is now active
                let new_state =
                    SessionState::new_active(self.writer.clone(), ctx.config.heartbeat_interval);
                app.on_logon().await;
                ctx.store.increment_target_seq_number().await?;
                Ok(TransitionResult::TransitionTo(new_state))
            }
            VerifyResult::SeqTooHigh { expected, actual } => {
                // Unusual during logon, but handle it
                use crate::message::resend_request::ResendRequest;
                use crate::session::error::InternalSendResultExt;
                use crate::session::state::AwaitingResendState;
                use tracing::debug;

                debug!(
                    "we are behind target during logon (ours: {expected}, theirs: {actual}), requesting resend."
                );
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
            VerifyResult::Handled(transition) => Ok(transition),
        }
    }
}
