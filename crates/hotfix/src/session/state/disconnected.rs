use crate::session::event::AwaitingActiveSessionResponse;
use crate::session::state::AwaitingLogonState;
use crate::transport::writer::WriterRef;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, error};

pub(crate) struct DisconnectedState {
    pub(crate) reconnect: bool,
    session_awaiter: Option<oneshot::Sender<AwaitingActiveSessionResponse>>,
    pub(crate) reason: String,
}

impl DisconnectedState {
    pub(crate) fn new(reconnect: bool, reason: &str) -> Self {
        Self {
            reconnect,
            session_awaiter: None,
            reason: reason.to_string(),
        }
    }

    pub(crate) fn set_session_awaiter(
        &mut self,
        responder: oneshot::Sender<AwaitingActiveSessionResponse>,
    ) {
        self.session_awaiter = Some(responder);
    }

    pub(crate) fn has_session_awaiter(&self) -> bool {
        self.session_awaiter.is_some()
    }

    pub(crate) fn take_session_awaiter(
        &mut self,
    ) -> Option<oneshot::Sender<AwaitingActiveSessionResponse>> {
        self.session_awaiter.take()
    }

    pub(crate) fn on_connect(
        &self,
        writer: WriterRef,
        logon_timeout: Duration,
    ) -> super::SessionState {
        super::SessionState::AwaitingLogon(AwaitingLogonState {
            writer,
            logon_sent: false,
            logon_timeout: Instant::now() + logon_timeout,
        })
    }

    pub(crate) fn should_reconnect(&self) -> bool {
        self.reconnect
    }

    pub(crate) fn register_session_awaiter(
        &mut self,
        responder: oneshot::Sender<AwaitingActiveSessionResponse>,
    ) -> Result<(), oneshot::Sender<AwaitingActiveSessionResponse>> {
        if self.has_session_awaiter() {
            Err(responder)
        } else {
            self.set_session_awaiter(responder);
            Ok(())
        }
    }

    pub(crate) fn notify_session_awaiter(&mut self) {
        if let Some(awaiter) = self.take_session_awaiter() {
            if let Err(err) = awaiter.send(AwaitingActiveSessionResponse::Active) {
                error!("failed to send session awaiter response: {err:?}");
            } else {
                debug!("notified session awaiter");
            }
        }
    }
}
