use serde::{Deserialize, Serialize};

pub const SOCKET_PATH: &str = "/tmp/vigil.sock";

#[derive(Serialize, Deserialize)]
pub enum MessageKind {
    Query,
    QueryResponse,
    QueryError,
    Shutdown,
    ShutdownAck,
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    message_kind: MessageKind,
    data: String,
}

impl Message {
    pub fn new(message_kind: MessageKind, data: String) -> Message {
        Message { message_kind, data }
    }

    pub fn get_message_kind(&self) -> &MessageKind {
        &self.message_kind
    }

    pub fn get_message_data(&self) -> &String {
        &self.data
    }
}
