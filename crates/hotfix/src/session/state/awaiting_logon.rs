use crate::transport::writer::WriterRef;
use tokio::time::Instant;
use tracing::warn;

pub(crate) struct AwaitingLogonState {
    pub(crate) writer: WriterRef,
    pub(crate) logon_sent: bool,
    pub(crate) logon_timeout: Instant,
}

impl AwaitingLogonState {
    pub(crate) async fn on_disconnect(&self, reason: &str) -> super::SessionState {
        self.writer.disconnect().await;
        super::SessionState::new_disconnected(true, reason)
    }

    pub(crate) async fn on_peer_timeout(&self) {
        warn!("peer didn't respond to our Logon, disconnecting..");
        self.writer.disconnect().await;
    }
}
