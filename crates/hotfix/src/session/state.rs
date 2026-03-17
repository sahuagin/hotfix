mod active;
mod awaiting_logon;
mod awaiting_logout;
mod awaiting_resend;
mod disconnected;

pub(crate) use active::{ActiveState, calculate_peer_interval};
pub(crate) use awaiting_logon::AwaitingLogonState;
pub(crate) use awaiting_logout::AwaitingLogoutState;
pub(crate) use awaiting_resend::{AwaitingResendState, AwaitingResendTransitionOutcome};
pub(crate) use disconnected::DisconnectedState;

use crate::config::SessionConfig;
use crate::message::logon::Logon;
use crate::message::logout::Logout;
use crate::message::parser::RawFixMessage;
use crate::message::{OutboundMessage, generate_message};
use crate::session::error::InternalSendError;
use crate::session::event::AwaitingActiveSessionResponse;
use crate::session::info::Status as SessionInfoStatus;
use crate::store::StoreError;
use crate::transport::writer::WriterRef;
use hotfix_store::MessageStore;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, error};

pub(crate) struct SessionCtx<'a, Store> {
    pub config: &'a SessionConfig,
    pub store: &'a mut Store,
}

#[allow(dead_code)] // fields used in later sub-phases
pub(crate) struct PreparedMessage {
    pub seq_num: u64,
    pub msg_type: String,
    pub raw: RawFixMessage,
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

    pub async fn send_message(&mut self, message_type: &str, message: RawFixMessage) {
        match self {
            Self::Active(ActiveState { writer, .. })
            | Self::AwaitingResend(AwaitingResendState { writer, .. }) => {
                if message_type == Logon::MSG_TYPE {
                    error!("logon message is invalid for active sessions")
                } else {
                    writer.send_raw_message(message).await
                }
            }
            Self::AwaitingLogon(AwaitingLogonState {
                writer, logon_sent, ..
            }) => match message_type {
                Logon::MSG_TYPE => {
                    if *logon_sent {
                        error!("trying to send logon twice");
                    } else {
                        writer.send_raw_message(message).await;
                        *logon_sent = true;
                    }
                }
                Logout::MSG_TYPE => {
                    writer.send_raw_message(message).await;
                }
                _ => error!("invalid outgoing message for AwaitingLogon state"),
            },
            Self::AwaitingLogout(AwaitingLogoutState { writer, .. }) => {
                // Logout messages are allowed because we first transition into AwaitingLogout
                // and only then send the logout message
                if message_type == Logout::MSG_TYPE {
                    writer.send_raw_message(message).await
                }
            }
            _ => error!("trying to write without an established connection"),
        }
    }

    pub async fn disconnect_writer(&self) {
        match self {
            Self::Active(ActiveState { writer, .. })
            | Self::AwaitingLogon(AwaitingLogonState { writer, .. })
            | Self::AwaitingLogout(AwaitingLogoutState { writer, .. })
            | Self::AwaitingResend(AwaitingResendState { writer, .. }) => writer.disconnect().await,
            _ => debug!("disconnecting an already disconnected session"),
        }
    }

    fn get_writer(&self) -> Option<&WriterRef> {
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

    pub fn reset_heartbeat_timer(&mut self, heartbeat_interval: u64) {
        if let Self::Active(state) = self {
            state.reset_heartbeat_timer(heartbeat_interval);
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

    pub fn reset_peer_timer(
        &mut self,
        heartbeat_interval: u64,
        test_request_id: Option<TestRequestId>,
    ) {
        if let Self::Active(state) = self {
            state.reset_peer_timer(heartbeat_interval, test_request_id);
        }
    }

    pub fn expected_test_response_id(&self) -> Option<&TestRequestId> {
        match self {
            Self::Active(state) => state.expected_test_response_id(),
            _ => None,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.get_writer().is_some()
    }

    pub fn is_logged_on(&self) -> bool {
        matches!(self, SessionState::Active(_))
            || matches!(self, SessionState::AwaitingResend { .. })
    }

    pub fn is_expecting_test_response(&self) -> bool {
        self.expected_test_response_id().is_some()
    }

    pub fn is_awaiting_logon(&self) -> bool {
        matches!(self, SessionState::AwaitingLogon(_))
    }

    pub fn is_awaiting_logout(&self) -> bool {
        matches!(self, SessionState::AwaitingLogout(_))
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
