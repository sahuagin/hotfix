//! FIX acceptor implementation — the venue/exchange side.
//!
//! The mirror of [`crate::initiator::Initiator`]: instead of opening the
//! connection and speaking first, the acceptor **binds** a listener, waits
//! for the initiator to connect, and answers its Logon (adopting the peer's
//! HeartBtInt(108) and honoring ResetSeqNumFlag(141)=Y by resetting both
//! sequence streams). After logon the session machinery is fully symmetric —
//! heartbeats, test requests, sequence verification, resend, and logout are
//! the same state machine the initiator runs.
//!
//! One `Acceptor` serves ONE session (the comp IDs in its `SessionConfig`,
//! from the acceptor's own perspective): connections are accepted one at a
//! time, and when the peer drops, the acceptor returns to accepting — the
//! peer may reconnect (a fresh Logon with 141=Y starts the day over; one
//! without resumes sequence numbers, same as any FIX session resume).
//!
//! Transport is plain TCP. TLS termination belongs in front (the same
//! posture as the production tunnels, where `socat` owns TLS and the engine
//! sees loopback plaintext).

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::application::Application;
use crate::config::{SessionConfig, SessionRole};
use crate::message::OutboundMessage;
use crate::session::error::{SendError, SendOutcome, SessionCreationError};
use crate::session::event::ScheduleResponse;
use crate::session::{InternalSessionRef, SessionHandle};
use crate::store::MessageStore;
use crate::transport::connect_over_stream;
use crate::wire_observer::WireObserverHandle;

#[derive(Clone)]
pub struct Acceptor<Outbound> {
    pub config: SessionConfig,
    session_handle: SessionHandle<Outbound>,
    completion_rx: watch::Receiver<bool>,
    local_addr: SocketAddr,
}

impl<Outbound: OutboundMessage> Acceptor<Outbound> {
    /// Bind `connection_host:connection_port` and serve the configured
    /// session to whichever initiator connects. The session task starts
    /// immediately; the returned acceptor is listening (see
    /// [`Acceptor::local_addr`] for the bound address — port 0 works).
    pub async fn start(
        config: SessionConfig,
        application: impl Application<Outbound = Outbound>,
        store: impl MessageStore + 'static,
    ) -> Result<Self, SessionCreationError> {
        Self::start_with_observer(config, application, store, None).await
    }

    /// Variant of [`Acceptor::start`] that installs a [`crate::WireObserver`]
    /// (fires for every message crossing the session boundary, admin
    /// included — same contract as the initiator's).
    pub async fn start_with_observer(
        mut config: SessionConfig,
        application: impl Application<Outbound = Outbound>,
        store: impl MessageStore + 'static,
        wire_observer: WireObserverHandle,
    ) -> Result<Self, SessionCreationError> {
        // Constructing an Acceptor IS the role — don't ask callers to keep
        // two fields consistent.
        config.role = SessionRole::Acceptor;

        let listener = TcpListener::bind((config.connection_host.as_str(), config.connection_port))
            .await
            .map_err(SessionCreationError::Bind)?;
        let local_addr = listener.local_addr().map_err(SessionCreationError::Bind)?;

        let session_ref = InternalSessionRef::new_with_observer(
            config.clone(),
            application,
            store,
            wire_observer,
        )?;
        let (completion_tx, completion_rx) = watch::channel(false);

        tokio::spawn(accept_loop(listener, session_ref.clone(), completion_tx));

        Ok(Self {
            config,
            session_handle: session_ref.into(),
            completion_rx,
            local_addr,
        })
    }

    /// The bound listener address — the port the initiator should dial
    /// (useful with port 0 in tests).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Sends a message and waits for confirmation that it was persisted.
    pub async fn send(&self, msg: Outbound) -> Result<SendOutcome, SendError> {
        self.session_handle.send(msg).await
    }

    /// Sends a message without waiting for confirmation.
    pub async fn send_forget(&self, msg: Outbound) -> Result<(), SendError> {
        self.session_handle.send_forget(msg).await
    }

    pub fn is_interested(&self, sender_comp_id: &str, target_comp_id: &str) -> bool {
        self.config.sender_comp_id == sender_comp_id && self.config.target_comp_id == target_comp_id
    }

    pub fn session_handle(&self) -> SessionHandle<Outbound> {
        self.session_handle.clone()
    }

    pub async fn shutdown(self, reconnect: bool) -> Result<(), SendError> {
        self.session_handle.shutdown(reconnect).await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), self.wait_for_shutdown())
            .await
            .map_err(|_| SendError::SessionGone)?;
        Ok(())
    }

    pub async fn wait_for_shutdown(&self) {
        let mut rx = self.completion_rx.clone();
        loop {
            if *rx.borrow_and_update() {
                break;
            } else if let Err(err) = rx.changed().await {
                warn!("accept loop has exited but completion signal was not sent: {err}");
                break;
            }
        }
    }

    pub fn is_shutdown(&self) -> bool {
        *self.completion_rx.borrow()
    }
}

/// The acceptor's mirror of the initiator's `establish_connection` loop:
/// accept, serve until disconnect, ask the session whether to keep going,
/// accept again. Sequence state lives in the session (not the connection),
/// so a resumed session continues its numbering and a 141=Y re-logon resets
/// it — both through the normal session paths.
async fn accept_loop<Outbound: OutboundMessage>(
    listener: TcpListener,
    session_ref: InternalSessionRef<Outbound>,
    completion_tx: watch::Sender<bool>,
) {
    loop {
        match session_ref.await_in_schedule().await {
            Ok(ScheduleResponse::InSchedule) => {
                debug!("resuming accept as schedule is active");
            }
            Ok(ScheduleResponse::Shutdown) => {
                warn!("session indicated shutdown during schedule check");
                break;
            }
            Err(_) => {
                warn!("session task terminated when checking schedule");
                break;
            }
        }

        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(%peer, "accepted FIX connection");
                let conn = connect_over_stream(session_ref.clone(), stream).await;
                if session_ref
                    .register_writer(conn.get_writer())
                    .await
                    .is_err()
                {
                    warn!("session task terminated when trying to register writer");
                    break;
                }
                conn.run_until_disconnect().await;
                warn!("session connection dropped, returning to accept");
            }
            Err(err) => {
                warn!("failed to accept connection: {err}");
            }
        }

        match session_ref.should_reconnect().await {
            Ok(true) => {
                debug!("session indicated we should keep accepting");
            }
            Ok(false) => {
                warn!("session indicated we shouldn't keep accepting");
                break;
            }
            Err(_) => {
                warn!("session task terminated when making decision to keep accepting");
                break;
            }
        }
    }

    completion_tx.send_replace(true);
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::application::{Application, InboundDecision, OutboundDecision};
    use crate::initiator::Initiator;
    use crate::session::Status;
    use crate::store::in_memory::InMemoryMessageStore;
    use hotfix_message::Part;
    use hotfix_message::message::Message;
    use hotfix_message::session_fields::MSG_TYPE;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// A "news"-shaped app message (35=B) so the app path is exercised with a
    /// non-admin type.
    #[derive(Clone)]
    struct News;

    impl OutboundMessage for News {
        fn write(&self, _msg: &mut Message) {}
        fn message_type(&self) -> &str {
            "B"
        }
    }

    /// Reports logons and inbound app message types over channels.
    struct ProbeApp {
        logons: mpsc::UnboundedSender<()>,
        inbound: mpsc::UnboundedSender<String>,
    }

    #[async_trait::async_trait]
    impl Application for ProbeApp {
        type Outbound = News;

        async fn on_outbound_message(&self, _msg: &News) -> OutboundDecision {
            OutboundDecision::Send
        }
        async fn on_inbound_message(&self, msg: &Message) -> InboundDecision {
            let t: &str = msg.header().get(MSG_TYPE).unwrap_or("?");
            let _ = self.inbound.send(t.to_string());
            InboundDecision::Accept
        }
        async fn on_logout(&mut self, _reason: &str) {}
        async fn on_logon(&mut self) {
            let _ = self.logons.send(());
        }
        async fn on_state_change(&self, _from: &Status, _to: &Status) {}
    }

    struct Probe {
        app: ProbeApp,
        logons: mpsc::UnboundedReceiver<()>,
        inbound: mpsc::UnboundedReceiver<String>,
    }

    fn probe() -> Probe {
        let (logon_tx, logons) = mpsc::unbounded_channel();
        let (inbound_tx, inbound) = mpsc::unbounded_channel();
        Probe {
            app: ProbeApp {
                logons: logon_tx,
                inbound: inbound_tx,
            },
            logons,
            inbound,
        }
    }

    fn config(sender: &str, target: &str, port: u16, reset_on_logon: bool) -> SessionConfig {
        SessionConfig {
            begin_string: "FIX.4.4".to_string(),
            sender_comp_id: sender.to_string(),
            target_comp_id: target.to_string(),
            data_dictionary_path: None,
            connection_host: "127.0.0.1".to_string(),
            connection_port: port,
            tls_config: None,
            heartbeat_interval: 30,
            logon_timeout: 5,
            logout_timeout: 2,
            reconnect_interval: 1,
            reset_on_logon,
            schedule: None,
            role: Default::default(),
        }
    }

    async fn recv_within<T>(rx: &mut mpsc::UnboundedReceiver<T>, what: &str) -> T {
        tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for {what}"))
            .unwrap_or_else(|| panic!("channel closed waiting for {what}"))
    }

    /// The symmetric-engine proof: a real Initiator logs onto a real Acceptor
    /// (141=Y, the production shape), app messages flow BOTH directions, and
    /// the initiator-driven logout handshake completes.
    #[tokio::test]
    async fn initiator_logs_onto_acceptor_and_messages_flow_both_ways() {
        let mut venue = probe();
        let acceptor = Acceptor::start(
            config("VENUE", "CLIENT", 0, false),
            venue.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("acceptor starts");
        let port = acceptor.local_addr().port();

        let mut client = probe();
        let initiator = Initiator::start(
            config("CLIENT", "VENUE", port, true), // reset_on_logon: production shape
            client.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("initiator starts");

        // Both sides observe logon.
        recv_within(&mut venue.logons, "acceptor logon").await;
        recv_within(&mut client.logons, "initiator logon").await;

        // Client -> venue app message.
        initiator.send(News).await.expect("client send");
        assert_eq!(recv_within(&mut venue.inbound, "venue inbound").await, "B");

        // Venue -> client app message (the acceptor can originate too).
        acceptor.send(News).await.expect("venue send");
        assert_eq!(
            recv_within(&mut client.inbound, "client inbound").await,
            "B"
        );

        // Initiator-driven logout handshake completes cleanly.
        initiator.shutdown(false).await.expect("clean shutdown");
    }

    /// The daemon's real reconnect shape: drop the client, bring up a FRESH
    /// initiator (new store, 141=Y, seq 1) against the SAME acceptor session.
    /// The acceptor must honor the reset and log on again — without the
    /// store reset on inbound 141=Y this reads as a sequence regression.
    #[tokio::test]
    async fn acceptor_accepts_a_reset_relogon_after_disconnect() {
        let mut venue = probe();
        let acceptor = Acceptor::start(
            config("VENUE", "CLIENT", 0, false),
            venue.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("acceptor starts");
        let port = acceptor.local_addr().port();

        let client1 = probe();
        let first = Initiator::start(
            config("CLIENT", "VENUE", port, true),
            client1.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("first initiator");
        recv_within(&mut venue.logons, "first logon").await;
        // Advance the acceptor's sequence numbers past 1 so the re-logon
        // genuinely exercises the reset (not a coincidentally-fresh store).
        first.send(News).await.expect("first send");
        assert_eq!(recv_within(&mut venue.inbound, "first inbound").await, "B");
        first.shutdown(false).await.expect("first shutdown");

        let mut client2 = probe();
        let second = Initiator::start(
            config("CLIENT", "VENUE", port, true),
            client2.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("second initiator");
        recv_within(&mut venue.logons, "re-logon after reset").await;
        recv_within(&mut client2.logons, "second client logon").await;

        second.send(News).await.expect("second send");
        assert_eq!(recv_within(&mut venue.inbound, "second inbound").await, "B");
        second.shutdown(false).await.expect("second shutdown");
    }

    /// Session-layer discipline: the first message on a connection must be a
    /// Logon; anything else disconnects (AwaitingLogon::pre_process_inbound).
    #[tokio::test]
    async fn acceptor_disconnects_when_first_message_is_not_logon() {
        use crate::message::generate_message;
        use crate::message::heartbeat::Heartbeat;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let venue = probe();
        let acceptor = Acceptor::start(
            config("VENUE", "CLIENT", 0, false),
            venue.app,
            InMemoryMessageStore::default(),
        )
        .await
        .expect("acceptor starts");
        let port = acceptor.local_addr().port();

        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect");
        let heartbeat = generate_message("FIX.4.4", "CLIENT", "VENUE", 1, Heartbeat::default())
            .expect("generate heartbeat");
        stream.write_all(&heartbeat).await.expect("write");

        // The acceptor drops the connection: read reaches EOF.
        let mut buf = [0u8; 256];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            match tokio::time::timeout_at(deadline, stream.read(&mut buf)).await {
                Ok(Ok(0)) => break, // EOF — disconnected, as required
                Ok(Ok(_)) => {}     // drain anything in flight
                Ok(Err(_)) => break,
                Err(_) => panic!("acceptor did not disconnect a non-Logon first message"),
            }
        }
    }
}
