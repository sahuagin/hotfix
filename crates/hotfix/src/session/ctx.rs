use crate::config::SessionConfig;
use crate::message::parser::RawFixMessage;
use crate::message::{OutboundMessage, generate_message};
use crate::session::error::InternalSendError;
use crate::session::state::SessionState;
use crate::store::StoreError;
use crate::transport::writer::WriterRef;
use hotfix_message::MessageBuilder;
use hotfix_message::message::{Config as MessageConfig, Message};
use hotfix_store::MessageStore;
use std::collections::VecDeque;

pub(crate) struct SessionCtx<'a, Store> {
    pub config: &'a SessionConfig,
    pub store: &'a mut Store,
    pub message_builder: &'a MessageBuilder,
    pub message_config: &'a MessageConfig,
}

pub(crate) struct PreparedMessage {
    pub seq_num: u64,
    #[allow(dead_code)]
    pub msg_type: String,
    pub raw: RawFixMessage,
}

pub(crate) enum TransitionResult {
    Stay,
    TransitionTo(SessionState),
    TransitionWithBacklog {
        new_state: SessionState,
        backlog: VecDeque<Message>,
    },
}

pub(crate) enum VerifyResult {
    Passed,
    SeqTooHigh { expected: u64, actual: u64 },
    Handled(TransitionResult),
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

    /// Prepare, persist, and send a message via the given writer.
    pub async fn send_message(
        &mut self,
        writer: &WriterRef,
        message: impl OutboundMessage,
    ) -> Result<u64, InternalSendError> {
        let prepared = self.prepare_message(message).await?;
        writer.send_raw_message(prepared.raw).await;
        Ok(prepared.seq_num)
    }
}
