use crate::Application;
use crate::message::logout::Logout;
use crate::session::error::SessionOperationError;
use crate::session::state::{SessionCtx, SessionState, TransitionResult, VerifyResult};
use crate::transport::writer::WriterRef;
use hotfix_message::Part;
use hotfix_message::session_fields::MSG_TYPE;
use hotfix_store::MessageStore;
use tokio::time::Instant;
use tracing::warn;

pub(crate) struct AwaitingLogoutState {
    pub(crate) writer: WriterRef,
    pub(crate) logout_timeout: Instant,
    pub(crate) reconnect: bool,
}

impl AwaitingLogoutState {
    pub(crate) fn on_disconnect(&self, reason: &str) -> SessionState {
        SessionState::new_disconnected(self.reconnect, reason)
    }

    pub(crate) async fn on_peer_timeout(&self) -> SessionState {
        warn!("peer didn't respond to our Logout, disconnecting..");
        self.writer.disconnect().await;
        SessionState::new_disconnected(self.reconnect, "logout timeout")
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

        if message_type == Logout::MSG_TYPE {
            // Process the logout
            match ctx
                .verify_and_handle(&self.writer, &message, false, false)
                .await?
            {
                VerifyResult::Passed => {}
                VerifyResult::SeqTooHigh { .. } => {
                    // verify with check_too_high=false, shouldn't happen
                }
                VerifyResult::ErrorHandled(Some(new_state)) => {
                    return Ok(TransitionResult::TransitionTo(new_state));
                }
                VerifyResult::ErrorHandled(None) => return Ok(TransitionResult::Stay),
            }

            app.on_logout("peer has logged us out").await;
            self.writer.disconnect().await;
            ctx.store.increment_target_seq_number().await?;

            Ok(TransitionResult::TransitionTo(
                SessionState::new_disconnected(self.reconnect, "logout completed"),
            ))
        } else {
            // Other messages during logout: increment target seq and stay
            ctx.store.increment_target_seq_number().await?;
            Ok(TransitionResult::Stay)
        }
    }
}
