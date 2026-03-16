use crate::transport::writer::WriterRef;
use tokio::time::Instant;

pub(crate) struct AwaitingLogonState {
    pub(crate) writer: WriterRef,
    pub(crate) logon_sent: bool,
    pub(crate) logon_timeout: Instant,
}
