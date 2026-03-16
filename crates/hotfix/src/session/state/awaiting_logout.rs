use crate::transport::writer::WriterRef;
use tokio::time::Instant;

pub(crate) struct AwaitingLogoutState {
    pub(crate) writer: WriterRef,
    pub(crate) logout_timeout: Instant,
    pub(crate) reconnect: bool,
}
