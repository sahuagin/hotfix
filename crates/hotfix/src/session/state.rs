mod active;
mod awaiting_logon;
mod awaiting_logout;
mod awaiting_resend;
mod disconnected;

pub(crate) use crate::session::ctx::{SessionCtx, TransitionResult, VerifyResult};
pub(crate) use active::{ActiveState, calculate_peer_interval};
pub(crate) use awaiting_logon::AwaitingLogonState;
pub(crate) use awaiting_logout::AwaitingLogoutState;
pub(crate) use awaiting_resend::AwaitingResendState;
pub(crate) use disconnected::DisconnectedState;

use crate::session::event::AwaitingActiveSessionResponse;
use crate::session::info::Status as SessionInfoStatus;
use crate::transport::writer::WriterRef;
use hotfix_store::MessageStore;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::error;

pub(crate) const TEST_REQUEST_THRESHOLD: f64 = 1.2;

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

    /// Send a logout message and immediately disconnect, if connected.
    pub(crate) async fn logout_and_terminate<Store: MessageStore>(
        &self,
        ctx: &mut SessionCtx<'_, Store>,
        reason: &str,
    ) {
        if let Some(writer) = self.get_writer() {
            ctx.logout_and_terminate(writer, reason).await;
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
