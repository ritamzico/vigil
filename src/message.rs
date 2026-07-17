use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const SOCKET_PATH: &str = "/tmp/vigil.sock";

#[derive(Serialize, Deserialize)]
pub enum MessageKind {
    Query,
    QueryResponse,
    QueryError,
    Shutdown,
    ShutdownAck,
    Watch,
    WatchAck,
    Follow,
}

#[derive(Serialize, Deserialize)]
pub struct WatchPayload {
    pub path: PathBuf,
    pub time_field: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub message_kind: MessageKind,
    pub data: String,
}

impl Message {
    pub fn new(message_kind: MessageKind, data: String) -> Message {
        Message { message_kind, data }
    }
}
