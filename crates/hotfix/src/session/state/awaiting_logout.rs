use crate::transport::writer::WriterRef;
use tokio::time::Instant;
use tracing::warn;

pub(crate) struct AwaitingLogoutState {
    pub(crate) writer: WriterRef,
    pub(crate) logout_timeout: Instant,
    pub(crate) reconnect: bool,
}

impl AwaitingLogoutState {
    pub(crate) fn on_disconnect(&self, reason: &str) -> super::SessionState {
        super::SessionState::new_disconnected(self.reconnect, reason)
    }

    pub(crate) async fn on_peer_timeout(&self) -> super::SessionState {
        warn!("peer didn't respond to our Logout, disconnecting..");
        self.writer.disconnect().await;
        super::SessionState::new_disconnected(self.reconnect, "logout timeout")
    }
}
