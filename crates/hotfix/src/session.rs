pub(crate) mod admin_request;
pub mod error;
pub(crate) mod event;
mod info;
mod session_handle;
pub mod session_ref;
mod state;

use chrono::Utc;
use hotfix_message::dict::Dictionary;
use hotfix_message::message::{Config as MessageConfig, Message};
use hotfix_message::{MessageBuilder, Part};
use std::future::Future;
use std::pin::Pin;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, Sleep, sleep, sleep_until};
use tracing::{debug, error, info, warn};

use crate::Application;
use crate::config::SessionConfig;
use crate::message::OutboundMessage;
use crate::message::generate_message;
use crate::message::logon::{Logon, ResetSeqNumConfig};
use crate::message::logout::Logout;
use crate::message::parser::RawFixMessage;
use crate::message::reject::Reject;
use crate::message::resend_request::ResendRequest;
use crate::session::admin_request::AdminRequest;
use crate::session::error::SessionCreationError;
use crate::session::error::{InternalSendError, InternalSendResultExt, SessionOperationError};
pub use crate::session::error::{SendError, SendOutcome};
pub use crate::session::info::{SessionInfo, Status};
pub use crate::session::session_handle::SessionHandle;
#[cfg(not(feature = "test-utils"))]
pub(crate) use crate::session::session_ref::InternalSessionRef;
#[cfg(feature = "test-utils")]
pub use crate::session::session_ref::InternalSessionRef;
use crate::session::session_ref::OutboundRequest;
use crate::session::state::SessionState;
use crate::session::state::{SessionCtx, TestRequestId, TransitionResult};
use crate::session_schedule::{SessionPeriodComparison, SessionSchedule};
use crate::store::MessageStore;
use crate::transport::writer::WriterRef;
use event::SessionEvent;
use hotfix_message::parsed_message::{InvalidReason, ParsedMessage};
use hotfix_message::session_fields::{MSG_SEQ_NUM, SessionRejectReason};

const SCHEDULE_CHECK_INTERVAL: u64 = 1;

struct Session<A, S> {
    message_config: MessageConfig,
    config: SessionConfig,
    schedule: SessionSchedule,
    message_builder: MessageBuilder,
    state: SessionState,
    application: A,
    store: S,
    schedule_check_timer: Pin<Box<Sleep>>,
    reset_on_next_logon: bool,
}

impl<App, Store> Session<App, Store>
where
    App: Application,
    Store: MessageStore,
{
    fn new(
        config: SessionConfig,
        application: App,
        store: Store,
    ) -> Result<Session<App, Store>, SessionCreationError> {
        let schedule_check_timer = sleep(Duration::from_secs(SCHEDULE_CHECK_INTERVAL));

        let dictionary = Self::get_data_dictionary(&config)?;
        let message_config = MessageConfig::default();
        let message_builder = MessageBuilder::new(dictionary, message_config)?;
        let schedule = config.schedule.as_ref().try_into()?;

        let session = Self {
            config,
            schedule,
            message_config,
            message_builder,
            state: SessionState::new_disconnected(true, "initialising"),
            application,
            store,
            schedule_check_timer: Box::pin(schedule_check_timer),
            reset_on_next_logon: false,
        };

        Ok(session)
    }

    fn get_data_dictionary(config: &SessionConfig) -> Result<Dictionary, SessionCreationError> {
        match &config.data_dictionary_path {
            None => match config.begin_string.as_str() {
                #[cfg(feature = "fix44")]
                "FIX.4.4" => Ok(Dictionary::fix44()),
                _ => Err(SessionCreationError::UnsupportedBeginString(
                    config.begin_string.to_string(),
                )),
            },
            Some(dictionary_path) => Ok(Dictionary::load_from_file(dictionary_path)?),
        }
    }

    async fn on_incoming(
        &mut self,
        raw_message: RawFixMessage,
    ) -> Result<(), SessionOperationError> {
        debug!("received message: {}", raw_message);

        // Reset peer timer before dispatching (if not expecting test response)
        if !self.state.is_expecting_test_response() {
            self.reset_peer_timer(None);
        }

        match self.message_builder.build(raw_message.as_bytes()) {
            ParsedMessage::Valid(message) => {
                self.dispatch_valid_message(message).await?;
            }
            ParsedMessage::Garbled(r) => {
                let message = raw_message.to_string();
                let reason = format!("{r:?}");
                error!(message, reason, "received garbled message");
            }
            ParsedMessage::Invalid { message, reason } => {
                self.handle_invalid_parsed_message(message, reason).await?;
            }
            ParsedMessage::UnexpectedError(err) => {
                error!("unexpected error: {:?}", err);
            }
        }

        Ok(())
    }

    async fn handle_invalid_parsed_message(
        &mut self,
        message: Message,
        reason: InvalidReason,
    ) -> Result<(), SessionOperationError> {
        match reason {
            InvalidReason::InvalidField(tag) | InvalidReason::InvalidGroup(tag) => {
                match message.header().get(MSG_SEQ_NUM) {
                    Ok(msg_seq_num) => {
                        let reject = Reject::new(msg_seq_num)
                            .session_reject_reason(SessionRejectReason::InvalidTagNumber)
                            .text(&format!("invalid field {tag}"));
                        self.send_message(reject)
                            .await
                            .with_send_context("reject for invalid field")?;
                    }
                    Err(err) => {
                        error!("failed to get message seq num: {:?}", err);
                    }
                }
            }
            InvalidReason::InvalidComponent(_component_name) => {
                warn!("received invalid component");
            }
            InvalidReason::InvalidMsgType(msg_type) => {
                let Session {
                    ref state,
                    ref mut store,
                    ref config,
                    ref message_builder,
                    ref message_config,
                    ..
                } = *self;
                let mut ctx = SessionCtx {
                    config,
                    store,
                    message_builder,
                    message_config,
                };
                if let Some(writer) = state.get_writer() {
                    ctx.handle_invalid_msg_type(writer, &message, &msg_type)
                        .await;
                }
            }
            InvalidReason::InvalidOrderInGroup { tag, .. } => {
                match message.header().get(MSG_SEQ_NUM) {
                    Ok(msg_seq_num) => {
                        let reject = Reject::new(msg_seq_num)
                            .session_reject_reason(
                                SessionRejectReason::RepeatingGroupFieldsOutOfOrder,
                            )
                            .text(&format!("field appears in incorrect order:{tag}"));
                        self.send_message(reject)
                            .await
                            .with_send_context("reject for invalid group order")?;
                    }
                    Err(err) => {
                        error!("failed to get message seq num: {:?}", err);
                    }
                }
            }
        }
        Ok(())
    }

    fn dispatch_valid_message(
        &mut self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), SessionOperationError>> + Send + '_>> {
        Box::pin(self.dispatch_valid_message_inner(message))
    }

    async fn dispatch_valid_message_inner(
        &mut self,
        message: Message,
    ) -> Result<(), SessionOperationError> {
        let Session {
            ref mut state,
            ref mut store,
            ref config,
            ref message_builder,
            ref message_config,
            ref mut application,
            ..
        } = *self;

        let mut ctx = SessionCtx {
            config,
            store,
            message_builder,
            message_config,
        };

        let transition = match state {
            SessionState::Active(s) => s.on_fix_message(&mut ctx, application, message).await?,
            SessionState::AwaitingLogon(s) => {
                s.on_fix_message(&mut ctx, application, message).await?
            }
            SessionState::AwaitingResend(s) => {
                s.on_fix_message(&mut ctx, application, message).await?
            }
            SessionState::AwaitingLogout(s) => {
                s.on_fix_message(&mut ctx, application, message).await?
            }
            SessionState::Disconnected(_) => TransitionResult::Stay,
        };

        // Let ctx go out of scope before we can mutate self.state
        let _ = ctx;

        self.apply_transition(transition).await
    }

    async fn apply_transition(
        &mut self,
        transition: TransitionResult,
    ) -> Result<(), SessionOperationError> {
        match transition {
            TransitionResult::Stay => {}
            TransitionResult::TransitionTo(new_state) => {
                self.state = new_state;
            }
            TransitionResult::TransitionWithBacklog {
                new_state,
                mut backlog,
            } => {
                self.state = new_state;
                debug!("resend is done, processing backlog");
                while let Some(msg) = backlog.pop_front() {
                    let seq_number: u64 = msg.get(MSG_SEQ_NUM).unwrap_or_else(|e| {
                        error!("failed to get seq number: {:?}", e);
                        0
                    });
                    let msg_type: &str = msg
                        .header()
                        .get(hotfix_message::session_fields::MSG_TYPE)
                        .unwrap_or("");
                    debug!(seq_number, msg_type, "processing queued message");

                    if msg_type == ResendRequest::MSG_TYPE {
                        // ResendRequest was already processed when it arrived (it bypasses
                        // the queue). Just increment the target seq number
                        // for sequence accounting purposes.
                        self.store.increment_target_seq_number().await?;
                    } else {
                        self.dispatch_valid_message(msg).await?;
                    }
                }
                debug!("resend backlog is cleared, resuming normal operation");
            }
        }
        Ok(())
    }

    async fn on_connect(&mut self, writer: WriterRef) -> Result<(), SessionOperationError> {
        if let SessionState::Disconnected(s) = &self.state {
            self.state = s.on_connect(writer, Duration::from_secs(self.config.logon_timeout));
        }
        self.reset_peer_timer(None);
        self.send_logon().await?;

        Ok(())
    }

    async fn on_disconnect(&mut self, reason: String) {
        let transition = match &self.state {
            SessionState::Active(s) => Some(s.on_disconnect(&reason).await),
            SessionState::AwaitingLogon(s) => Some(s.on_disconnect(&reason).await),
            SessionState::AwaitingResend(s) => Some(s.on_disconnect(&reason).await),
            SessionState::AwaitingLogout(s) => Some(s.on_disconnect(&reason)),
            SessionState::Disconnected(_) => {
                warn!("disconnect message was received, but the session is already disconnected");
                None
            }
        };
        if let Some(new_state) = transition {
            self.state = new_state;
        }
    }

    fn reset_heartbeat_timer(&mut self) {
        self.state
            .reset_heartbeat_timer(self.config.heartbeat_interval);
    }

    fn reset_peer_timer(&mut self, test_request_id: Option<TestRequestId>) {
        self.state
            .reset_peer_timer(self.config.heartbeat_interval, test_request_id);
    }

    /// Legacy send_app_message for non-Active connected states.
    async fn send_app_message_legacy(
        &mut self,
        message: App::Outbound,
    ) -> Result<SendOutcome, SendError> {
        use crate::application::OutboundDecision;

        match self.application.on_outbound_message(&message).await {
            OutboundDecision::Send => {
                let sequence_number = self.send_message(message).await?;
                Ok(SendOutcome::Sent { sequence_number })
            }
            OutboundDecision::Drop => {
                debug!("dropped outbound message as instructed by the application");
                Ok(SendOutcome::Dropped)
            }
            OutboundDecision::TerminateSession => {
                warn!("the application indicated we should terminate the session");
                self.state.disconnect_writer().await;
                Err(SendError::SessionTerminated)
            }
        }
    }

    /// Legacy send_message used by send_logon, send_logout, and error handling paths.
    async fn send_message(
        &mut self,
        message: impl OutboundMessage,
    ) -> Result<u64, InternalSendError> {
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
            InternalSendError::Persist(crate::store::StoreError::PersistMessage {
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

        self.send_raw(&msg_type, msg).await;

        Ok(seq_num)
    }

    async fn send_raw(&mut self, message_type: &str, data: Vec<u8>) {
        self.state
            .send_message(message_type, RawFixMessage::new(data))
            .await;
        self.reset_heartbeat_timer();
    }

    async fn send_logon(&mut self) -> Result<(), SessionOperationError> {
        let reset_config = if self.config.reset_on_logon || self.reset_on_next_logon {
            self.store.reset().await?;
            ResetSeqNumConfig::Reset
        } else {
            ResetSeqNumConfig::NoReset(Some(self.store.next_target_seq_number()))
        };
        self.reset_on_next_logon = false;

        let logon = Logon::new(self.config.heartbeat_interval, reset_config);
        self.send_message(logon).await.with_send_context("logon")?;
        Ok(())
    }

    async fn send_logout(&mut self, reason: &str) -> Result<(), SessionOperationError> {
        let logout = Logout::with_reason(reason.to_string());
        self.send_message(logout)
            .await
            .with_send_context("logout")?;
        Ok(())
    }

    /// Sends a logout message and immediately disconnects the counterparty.
    async fn logout_and_terminate(&mut self, reason: &str) {
        if let Err(err) = self.send_logout(reason).await {
            warn!("failed to send logout during session termination: {}", err);
        }
        self.state.disconnect_writer().await;
    }

    /// Sends a logout message and puts the session state into an AwaitingLogout state.
    async fn initiate_graceful_logout(
        &mut self,
        reason: &str,
        reconnect: bool,
    ) -> Result<(), SessionOperationError> {
        if self.state.try_transition_to_awaiting_logout(
            Duration::from_secs(self.config.logout_timeout),
            reconnect,
        ) {
            self.send_logout(reason).await?;
        }

        Ok(())
    }

    async fn handle_session_event(&mut self, event: SessionEvent) {
        self.handle_schedule_check().await;

        match event {
            SessionEvent::FixMessageReceived(fix_message) => {
                if let Err(err) = self.on_incoming(fix_message).await {
                    let reason = err.to_string();
                    error!(reason, "fatal error in message processing");
                    self.logout_and_terminate("internal error").await;
                    self.state = SessionState::new_disconnected(true, &reason);
                }
            }
            SessionEvent::Disconnected(reason) => {
                warn!(reason, "disconnected from peer");
                self.on_disconnect(reason).await;
            }
            SessionEvent::Connected(w) => {
                if let Err(err) = self.on_connect(w).await {
                    error!(err = ?err, "failed to establish logon after connecting");
                }
            }
            SessionEvent::ShouldReconnect(responder) => {
                if responder.send(self.state.should_reconnect()).is_err() {
                    warn!("tried to respond to ShouldReconnect query but the receiver is gone");
                }
            }
            SessionEvent::AwaitingActiveSession(responder) => {
                self.state.register_session_awaiter(responder);
            }
        }
    }

    async fn handle_outbound_message(&mut self, request: OutboundRequest<App::Outbound>) {
        let OutboundRequest { message, confirm } = request;

        let is_active = matches!(self.state, SessionState::Active(_));
        let is_connected = self.state.is_connected();

        let result = if !is_connected {
            Err(SendError::Disconnected)
        } else if is_active {
            let Session {
                ref mut state,
                ref mut store,
                ref config,
                ref message_builder,
                ref message_config,
                ref mut application,
                ..
            } = *self;

            if let SessionState::Active(s) = state {
                let mut ctx = SessionCtx {
                    config,
                    store,
                    message_builder,
                    message_config,
                };
                s.send_app_message(&mut ctx, application, message).await
            } else {
                unreachable!()
            }
        } else {
            // Legacy path: session is connected but not Active (e.g. AwaitingLogon).
            self.send_app_message_legacy(message).await
        };

        match confirm {
            Some(tx) => {
                let _ = tx.send(result);
            }
            None => {
                if let Err(err) = result {
                    error!(err = ?err, "failed to send app message: {err}");
                }
            }
        }
    }

    async fn handle_admin_request(&mut self, request: AdminRequest) {
        match request {
            AdminRequest::InitiateGracefulShutdown { reconnect } => {
                warn!("initiating shutdown on request from admin..");
                if let Err(err) = self
                    .initiate_graceful_logout("explicitly requested", reconnect)
                    .await
                {
                    error!(err = ?err, "initiating graceful shutdown");
                }
            }
            AdminRequest::RequestSessionInfo(responder) => {
                info!("session info requested");
                if responder.send(self.get_session_info()).is_err() {
                    error!("failed to respond to session info request");
                }
            }
            AdminRequest::ResetSequenceNumbersOnNextLogon => {
                warn!("resetting sequence numbers on next logon");
                self.reset_on_next_logon = true;
            }
        }
    }

    async fn handle_heartbeat_timeout(&mut self) {
        let Session {
            ref mut state,
            ref mut store,
            ref config,
            ref message_builder,
            ref message_config,
            ..
        } = *self;
        if let SessionState::Active(active) = state {
            let mut ctx = SessionCtx {
                config,
                store,
                message_builder,
                message_config,
            };
            active.on_heartbeat_timeout(&mut ctx).await;
        }
    }

    async fn handle_peer_timeout(&mut self) {
        let Session {
            ref mut state,
            ref mut store,
            ref config,
            ref message_builder,
            ref message_config,
            ..
        } = *self;
        let transition = match state {
            SessionState::Active(active) => {
                let mut ctx = SessionCtx {
                    config,
                    store,
                    message_builder,
                    message_config,
                };
                active.on_peer_timeout(&mut ctx).await
            }
            SessionState::AwaitingLogon(awaiting_logon) => {
                awaiting_logon.on_peer_timeout().await;
                None
            }
            SessionState::AwaitingLogout(awaiting_logout) => {
                Some(awaiting_logout.on_peer_timeout().await)
            }
            _ => None,
        };
        if let Some(new_state) = transition {
            self.state = new_state;
        }
    }

    async fn handle_schedule_check(&mut self) {
        let now = Utc::now();
        let is_active = self.schedule.is_active_at(&now);

        if is_active {
            self.state.notify_session_awaiter();
            match self
                .schedule
                .is_same_session_period(&self.store.creation_time(), &now)
            {
                Ok(SessionPeriodComparison::SamePeriod) => {
                    // we are in the same period, nothing needs to be done
                }
                Ok(SessionPeriodComparison::DifferentPeriod) => {
                    self.logout_and_terminate("session period changed").await;
                    if let Err(err) = self.store.reset().await {
                        error!("error resetting session store: {err:}");
                        self.state =
                            SessionState::new_disconnected(false, "unexpected error in reset");
                    }
                }
                Ok(SessionPeriodComparison::OutsideSessionTime { .. }) => {
                    warn!("store creation time is outside session schedule, resetting store");
                    self.logout_and_terminate("creation time outside schedule")
                        .await;
                    if let Err(err) = self.store.reset().await {
                        error!("error resetting session store: {err:}");
                        self.state =
                            SessionState::new_disconnected(false, "unexpected error in reset");
                    }
                }
                Err(err) => {
                    error!("error checking session period: {err:?}");
                    self.logout_and_terminate("internal error").await;
                }
            }
        } else if self.state.is_connected()
            && let Err(err) = self
                .initiate_graceful_logout("End of session time", true)
                .await
        {
            error!(err = ?err, "failed to initiate graceful logout");
        }

        let deadline = Instant::now() + Duration::from_secs(SCHEDULE_CHECK_INTERVAL);
        self.schedule_check_timer.as_mut().reset(deadline);
    }

    fn get_session_info(&self) -> SessionInfo {
        SessionInfo {
            next_sender_seq_number: self.store.next_sender_seq_number(),
            next_target_seq_number: self.store.next_target_seq_number(),
            status: self.state.as_status(),
        }
    }
}

/// Extracts MsgSeqNum from a message header.
///
/// # Panics
/// Panics if the message does not contain a valid MsgSeqNum field.
/// This should never happen for messages that have passed validation.
#[allow(clippy::expect_used)]
pub(crate) fn get_msg_seq_num(message: &Message) -> u64 {
    message
        .header()
        .get(MSG_SEQ_NUM)
        .expect("MsgSeqNum missing from validated message - parser bug")
}

async fn run_session<App, Store>(
    mut session: Session<App, Store>,
    mut event_receiver: mpsc::Receiver<SessionEvent>,
    mut outbound_message_receiver: mpsc::Receiver<OutboundRequest<App::Outbound>>,
    mut admin_request_receiver: mpsc::Receiver<AdminRequest>,
) where
    App: Application,
    Store: MessageStore + Send + 'static,
{
    loop {
        select! {
            biased;
            next_admin_request = admin_request_receiver.recv() => {
                match next_admin_request {
                    Some(request) => session.handle_admin_request(request).await,
                    None => break,
                }
            }
            next_event = event_receiver.recv()=> {
                match next_event {
                    Some(event) => {
                        session.handle_session_event(event).await
                    }
                    None => break,
                }
            }
            next_outbound_request = outbound_message_receiver.recv() => {
                match next_outbound_request {
                    Some(request) => session.handle_outbound_message(request).await,
                    None => break,
                }
            }
            () = async {
                if let Some(deadline) = session.state.heartbeat_deadline() {
                    sleep_until(*deadline).await
                } else {
                    std::future::pending().await
                }
            } => {
                session.handle_heartbeat_timeout().await;
            }
            () = async {
                if let Some(deadline) = session.state.peer_deadline() {
                    sleep_until(*deadline).await
                } else {
                    std::future::pending().await
                }
            } => {
                session.handle_peer_timeout().await;
            }
            () = &mut session.schedule_check_timer.as_mut() => {
                session.handle_schedule_check().await;
            }
        }
    }

    debug!("session is shutting down")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{InboundDecision, OutboundDecision};
    use crate::message::OutboundMessage;
    use crate::store::{Result as StoreResult, StoreError};
    use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeDelta, Timelike};
    use chrono_tz::Tz;
    use hotfix_message::message::Message;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc;

    /// A controllable store for testing that implements MessageStore
    #[derive(Clone)]
    struct TestStore {
        creation_time: DateTime<Utc>,
        fail_reset: Arc<AtomicBool>,
        reset_called: Arc<AtomicBool>,
        sender_seq: u64,
        target_seq: u64,
    }

    impl TestStore {
        fn new(creation_time: DateTime<Utc>) -> Self {
            Self {
                creation_time,
                fail_reset: Arc::new(AtomicBool::new(false)),
                reset_called: Arc::new(AtomicBool::new(false)),
                sender_seq: 1,
                target_seq: 1,
            }
        }

        fn set_fail_reset(&self) {
            self.fail_reset.store(true, Ordering::SeqCst);
        }

        fn was_reset_called(&self) -> bool {
            self.reset_called.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl MessageStore for TestStore {
        async fn add(&mut self, _sequence_number: u64, _message: &[u8]) -> StoreResult<()> {
            Ok(())
        }

        async fn get_slice(&self, _begin: usize, _end: usize) -> StoreResult<Vec<Vec<u8>>> {
            Ok(vec![])
        }

        fn next_sender_seq_number(&self) -> u64 {
            self.sender_seq
        }

        fn next_target_seq_number(&self) -> u64 {
            self.target_seq
        }

        async fn increment_sender_seq_number(&mut self) -> StoreResult<()> {
            self.sender_seq += 1;
            Ok(())
        }

        async fn increment_target_seq_number(&mut self) -> StoreResult<()> {
            self.target_seq += 1;
            Ok(())
        }

        async fn set_target_seq_number(&mut self, seq_number: u64) -> StoreResult<()> {
            self.target_seq = seq_number;
            Ok(())
        }

        async fn reset(&mut self) -> StoreResult<()> {
            self.reset_called.store(true, Ordering::SeqCst);
            if self.fail_reset.load(Ordering::SeqCst) {
                return Err(StoreError::Reset("simulated reset failure".into()));
            }
            self.creation_time = Utc::now();
            Ok(())
        }

        fn creation_time(&self) -> DateTime<Utc> {
            self.creation_time
        }
    }

    /// Dummy message type for testing that implements required traits
    #[derive(Clone)]
    struct DummyMessage;

    impl OutboundMessage for DummyMessage {
        fn write(&self, _msg: &mut Message) {}
        fn message_type(&self) -> &str {
            "0"
        }
    }

    /// Minimal no-op application for testing
    struct NoOpApp;

    #[async_trait::async_trait]
    impl Application for NoOpApp {
        type Outbound = DummyMessage;

        async fn on_outbound_message(&self, _: &DummyMessage) -> OutboundDecision {
            OutboundDecision::Send
        }
        async fn on_inbound_message(&self, _: &Message) -> InboundDecision {
            InboundDecision::Accept
        }
        async fn on_logout(&mut self, _: &str) {}
        async fn on_logon(&mut self) {}
    }

    fn create_writer_ref() -> WriterRef {
        let (sender, _) = mpsc::channel(10);
        WriterRef::new(sender)
    }

    fn create_test_config() -> SessionConfig {
        SessionConfig {
            begin_string: "FIX.4.4".to_string(),
            sender_comp_id: "SENDER".to_string(),
            target_comp_id: "TARGET".to_string(),
            data_dictionary_path: None,
            connection_host: "localhost".to_string(),
            connection_port: 9876,
            tls_config: None,
            heartbeat_interval: 30,
            logon_timeout: 10,
            logout_timeout: 2,
            reconnect_interval: 30,
            reset_on_logon: false,
            schedule: None,
        }
    }

    fn create_test_session(
        schedule: SessionSchedule,
        state: SessionState,
        store: TestStore,
    ) -> Session<NoOpApp, TestStore> {
        let config = create_test_config();
        let message_config = MessageConfig::default();
        let dictionary = Dictionary::fix44();
        let message_builder = MessageBuilder::new(dictionary, message_config).unwrap();

        Session {
            message_config,
            config,
            schedule,
            message_builder,
            state,
            application: NoOpApp,
            store,
            schedule_check_timer: Box::pin(sleep(Duration::from_secs(1))),
            reset_on_next_logon: false,
        }
    }

    /// Creates a Daily schedule that is active at the current time
    fn create_active_schedule() -> SessionSchedule {
        // Use a 24-hour window that's definitely active
        SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
            timezone: Tz::UTC,
        }
    }

    /// Creates a Daily schedule that is inactive at the current time
    fn create_inactive_schedule() -> SessionSchedule {
        let now = Utc::now();
        let current_hour = now.time().hour();
        // Create a 1-hour window that's 12 hours from now (definitely not the current hour)
        let start_hour = (current_hour + 12) % 24;
        let end_hour = (start_hour + 1) % 24;
        SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(start_hour, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(end_hour, 0, 0).unwrap(),
            timezone: Tz::UTC,
        }
    }

    #[tokio::test]
    async fn test_handle_schedule_check_active_same_period() {
        // Use NonStop schedule - always active, always same period
        let schedule = SessionSchedule::NonStop;
        let writer = create_writer_ref();
        let state = SessionState::new_active(writer, 30);
        let store = TestStore::new(Utc::now());

        let mut session = create_test_session(schedule, state, store);

        session.handle_schedule_check().await;

        // State should remain Active (no logout triggered)
        assert!(
            session.state.is_logged_on(),
            "State should remain logged on for same period"
        );
        assert!(
            !session.store.was_reset_called(),
            "Store reset should not be called for same period"
        );
    }

    #[tokio::test]
    async fn test_handle_schedule_check_active_different_period() {
        use crate::session_schedule::SessionPeriodComparison;

        // Use a Daily schedule that's currently active
        let schedule = create_active_schedule();
        let writer = create_writer_ref();
        let state = SessionState::new_active(writer, 30);
        // Creation time is yesterday - different session period
        let yesterday = Utc::now() - TimeDelta::days(1);
        let store = TestStore::new(yesterday);

        let mut session = create_test_session(schedule, state, store);

        // Verify the schedule correctly identifies different periods
        let now = Utc::now();
        let creation_time = session.store.creation_time();
        let same_period = session
            .schedule
            .is_same_session_period(&creation_time, &now);
        assert!(
            matches!(same_period, Ok(SessionPeriodComparison::DifferentPeriod)),
            "Schedule should identify different periods"
        );

        session.handle_schedule_check().await;

        // Store reset should have been called (indicates DifferentPeriod branch was taken)
        assert!(
            session.store.was_reset_called(),
            "Store reset should be called for different period"
        );
    }

    #[tokio::test]
    async fn test_handle_schedule_check_active_reset_fails() {
        // Use a Daily schedule that's currently active
        let schedule = create_active_schedule();
        let writer = create_writer_ref();
        let state = SessionState::new_active(writer, 30);
        // Creation time is yesterday - different session period
        let yesterday = Utc::now() - TimeDelta::days(1);
        let store = TestStore::new(yesterday);
        store.set_fail_reset();

        let mut session = create_test_session(schedule, state, store);

        session.handle_schedule_check().await;

        // Store reset should have been attempted
        assert!(
            session.store.was_reset_called(),
            "Store reset should be called"
        );
        // When reset fails, state is explicitly set to Disconnected(reconnect=false)
        assert!(
            matches!(session.state, SessionState::Disconnected(_)),
            "State should be Disconnected after reset failure"
        );
        // Should NOT reconnect since reset failed
        assert!(
            !session.state.should_reconnect(),
            "Should not reconnect after failed reset"
        );
    }

    #[tokio::test]
    async fn test_handle_schedule_check_active_creation_time_outside_schedule() {
        use crate::session_schedule::SessionPeriodComparison;

        // Use a narrow schedule that's currently active but creation_time is outside
        let now = Utc::now();
        let current_hour = now.time().hour();

        // Create a 2-hour window around current time
        let start_hour = if current_hour == 0 {
            23
        } else {
            current_hour - 1
        };
        let end_hour = (current_hour + 2) % 24;

        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(start_hour, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(end_hour, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        let writer = create_writer_ref();
        let state = SessionState::new_active(writer, 30);

        // Creation time is today but at a time outside the schedule window
        let outside_hour = (current_hour + 6) % 24;
        let creation_time = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(now.year(), now.month(), now.day())
                .unwrap()
                .and_hms_opt(outside_hour, 30, 0)
                .unwrap(),
            Utc,
        );

        let store = TestStore::new(creation_time);

        let mut session = create_test_session(schedule, state, store);

        // Verify that is_same_session_period returns OutsideSessionTime
        let same_period = session
            .schedule
            .is_same_session_period(&creation_time, &now);
        assert!(
            matches!(
                same_period,
                Ok(SessionPeriodComparison::OutsideSessionTime { .. })
            ),
            "Schedule should return OutsideSessionTime when creation_time is outside active window"
        );

        session.handle_schedule_check().await;

        // The OutsideSessionTime branch now triggers a store reset (same as DifferentPeriod)
        assert!(
            session.store.was_reset_called(),
            "Store reset should be called when creation_time is outside schedule"
        );
    }

    #[tokio::test]
    async fn test_handle_schedule_check_inactive_connected() {
        // Use a schedule that's currently inactive
        let schedule = create_inactive_schedule();
        let writer = create_writer_ref();
        let state = SessionState::new_active(writer, 30);
        let store = TestStore::new(Utc::now());

        let mut session = create_test_session(schedule, state, store);

        session.handle_schedule_check().await;

        // State should be AwaitingLogout (graceful logout initiated)
        assert!(
            matches!(session.state, SessionState::AwaitingLogout(_)),
            "State should be AwaitingLogout when schedule is inactive and was connected"
        );
    }

    #[tokio::test]
    async fn test_handle_schedule_check_inactive_disconnected() {
        // Use a schedule that's currently inactive
        let schedule = create_inactive_schedule();
        let state = SessionState::new_disconnected(true, "test");
        let store = TestStore::new(Utc::now());

        let mut session = create_test_session(schedule, state, store);

        session.handle_schedule_check().await;

        // State should remain Disconnected (no action taken)
        assert!(
            matches!(session.state, SessionState::Disconnected(_)),
            "State should remain Disconnected when schedule is inactive and was disconnected"
        );
        // Reconnect flag should be preserved
        assert!(
            session.state.should_reconnect(),
            "Reconnect flag should be preserved"
        );
    }
}
