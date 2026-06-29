pub mod socket_reader;
pub mod socket_writer;
pub mod tcp;
pub mod tls;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::message::OutboundMessage;
use crate::session::InternalSessionRef;
use crate::transport::error::ConnectionResult;
use crate::{
    config::SessionConfig,
    transport::{
        FixConnection, socket_reader::spawn_socket_reader, socket_writer::spawn_socket_writer,
        tcp::create_tcp_connection, tls::create_tcp_over_tls_connection,
    },
};

/// Connect over TCP/TLS and return a FixConnection
pub async fn connect(
    config: &SessionConfig,
    session_ref: InternalSessionRef<impl OutboundMessage>,
) -> ConnectionResult<FixConnection> {
    let conn = if let Some(tls_config) = config.tls_config.as_ref() {
        let stream = create_tcp_over_tls_connection(
            config.connection_host.to_owned(),
            config.connection_port,
            tls_config,
        )
        .await?;
        connect_over_stream(session_ref.clone(), stream).await
    } else {
        let stream = create_tcp_connection(&config.connection_host, config.connection_port).await?;
        connect_over_stream(session_ref.clone(), stream).await
    };

    Ok(conn)
}

/// Wire a [`FixConnection`] over an already-established byte stream: split it,
/// spawn the reader/writer actors, and return the connection.
///
/// This is the transport-agnostic half of [`connect`] — it accepts any
/// `AsyncRead + AsyncWrite`, so it serves both a live TCP/TLS socket and a
/// replay source (a capture or pcap fed through an in-memory stream). Exposed
/// so callers can drive a session over a provided stream without opening a
/// network connection (decode/replay tooling).
pub async fn connect_over_stream<Outbound, Stream>(
    session_ref: InternalSessionRef<Outbound>,
    stream: Stream,
) -> FixConnection
where
    Outbound: OutboundMessage,
    Stream: AsyncRead + AsyncWrite + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);

    let observer = session_ref.wire_observer.clone();
    let (writer_ref, writer_exit) = spawn_socket_writer(writer, observer);
    let reader_ref = spawn_socket_reader(reader, session_ref);

    FixConnection::new(writer_ref, reader_ref, writer_exit)
}
