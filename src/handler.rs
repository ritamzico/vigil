use crate::event::Event;
use crate::index::{event_matches, Index};
use crate::message::{Message, MessageKind, WatchPayload};
use crate::parser::parse_query;
use serde_json::from_slice;
use serde_json::to_vec;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncReadExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

// Requests are queries or tiny control messages; anything bigger is a client
// bug or an attempt to exhaust the daemon's memory.
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;

pub async fn handle_client(
    stream: UnixStream,
    index: Arc<RwLock<Index>>,
    shutdown_tx: mpsc::Sender<()>,
    watch_tx: mpsc::Sender<(PathBuf, Option<String>)>,
    time_field: Option<String>,
    follow_tx: broadcast::Sender<Arc<Event>>,
) -> Result<(), HandlerError> {
    let (reader, mut writer) = stream.into_split();

    let mut buf: Vec<u8> = vec![];
    reader
        .take(MAX_REQUEST_BYTES + 1)
        .read_to_end(&mut buf)
        .await?;
    if buf.len() as u64 > MAX_REQUEST_BYTES {
        return Err(HandlerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "request exceeds maximum size",
        )));
    }
    let message: Message =
        from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let response = match message.message_kind {
        MessageKind::Query => match parse_query(&message.data, time_field.as_deref()) {
            Err(e) => Message::new(MessageKind::QueryError, e.to_string()),
            Ok(query_plan) => {
                let index_guard = index.read().unwrap();
                match index_guard.apply_query_plan(&query_plan) {
                    Err(e) => Message::new(MessageKind::QueryError, e.to_string()),
                    Ok(result) => Message::new(MessageKind::QueryResponse, result.to_string()),
                }
            }
        },
        MessageKind::Shutdown => {
            let _ = shutdown_tx.send(()).await;
            Message::new(MessageKind::ShutdownAck, String::new())
        }
        MessageKind::Watch => {
            let payload: WatchPayload = serde_json::from_str(&message.data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let _ = watch_tx.send((payload.path, payload.time_field)).await;
            Message::new(MessageKind::WatchAck, String::new())
        }
        MessageKind::Follow => {
            // Follow streams raw lines, not a framed Message response.
            return handle_follow(writer, &message.data, time_field, follow_tx).await;
        }
        // Response kinds are never valid requests — tell the client, don't panic.
        _ => Message::new(MessageKind::QueryError, "invalid request kind".to_string()),
    };

    let bytes = to_vec(&response).unwrap();

    match writer.write_all(&bytes).await {
        Ok(()) => {
            if let Err(e) = writer.shutdown().await {
                eprintln!("Failed to close client connection: {e}");
            }
        }
        Err(_) => eprintln!("Failed to write to client."),
    }

    Ok(())
}

/// Stream raw lines of newly ingested events matching `raw_query` until the
/// client disconnects. Follow supports filters only — no `|` pipeline stages.
async fn handle_follow(
    mut writer: OwnedWriteHalf,
    raw_query: &str,
    time_field: Option<String>,
    follow_tx: broadcast::Sender<Arc<Event>>,
) -> Result<(), HandlerError> {
    let query = if raw_query.contains('|') {
        Err("follow supports filters only, not '|' stages".to_string())
    } else {
        match parse_query(raw_query, time_field.as_deref()) {
            Err(e) => Err(e.to_string()),
            Ok(plan) if plan.aggregation.is_some() => {
                Err("follow supports filters only, not '|' stages".to_string())
            }
            Ok(plan) => Ok(plan.query),
        }
    };

    let query = match query {
        Ok(query) => query,
        Err(e) => {
            let _ = writer.write_all(format!("{e}\n").as_bytes()).await;
            let _ = writer.shutdown().await;
            return Ok(());
        }
    };

    let mut rx = follow_tx.subscribe();
    // Drop our sender clone so the channel can close (and this loop end)
    // once the daemon-side senders are gone.
    drop(follow_tx);
    loop {
        match rx.recv().await {
            Ok(event) => {
                if !event_matches(&event, &query) {
                    continue;
                }
                if writer.write_all(event.raw.as_bytes()).await.is_err()
                    || writer.write_all(b"\n").await.is_err()
                {
                    break; // client went away
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::value::Value;
    use serde_json::{from_slice, to_vec};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn make_index(events: Vec<Event>) -> Arc<RwLock<Index>> {
        let index = Arc::new(RwLock::new(Index::new(None)));
        let mut guard = index.write().unwrap();
        for event in events {
            guard.push_event(event);
        }
        drop(guard);
        index
    }

    fn make_event(raw: &str, fields: Vec<(&str, Value)>) -> Event {
        let map = fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Event::new(None, raw.to_string(), map)
    }

    async fn roundtrip(index: Arc<RwLock<Index>>, msg: Message) -> (Message, mpsc::Receiver<()>) {
        let (client, server) = UnixStream::pair().unwrap();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        let (watch_tx, _watch_rx) = mpsc::channel(1);
        let (follow_tx, _) = broadcast::channel(16);
        tokio::spawn(handle_client(
            server,
            index,
            shutdown_tx,
            watch_tx,
            None,
            follow_tx,
        ));

        let (mut reader, mut writer) = client.into_split();
        writer.write_all(&to_vec(&msg).unwrap()).await.unwrap();
        writer.shutdown().await.unwrap();

        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();
        let response: Message = from_slice(&buf).unwrap();

        (response, shutdown_rx)
    }

    #[tokio::test]
    async fn test_query_returns_matching_events() {
        let index = make_index(vec![
            make_event("e1", vec![("level", Value::String("ERROR".into()))]),
            make_event("e2", vec![("level", Value::String("INFO".into()))]),
        ]);

        let (response, _) = roundtrip(index, Message::new(MessageKind::Query, "level = ERROR".into())).await;

        assert!(matches!(response.message_kind, MessageKind::QueryResponse));
        assert_eq!(response.data, "e1");
    }

    #[tokio::test]
    async fn test_query_no_matches_returns_empty() {
        let index = make_index(vec![]);

        let (response, _) = roundtrip(index, Message::new(MessageKind::Query, "level = ERROR".into())).await;

        assert!(matches!(response.message_kind, MessageKind::QueryResponse));
        assert!(response.data.is_empty());
    }

    #[tokio::test]
    async fn test_query_parse_error_returns_query_error() {
        let index = make_index(vec![]);

        let (response, _) = roundtrip(index, Message::new(MessageKind::Query, "level ???".into())).await;

        assert!(matches!(response.message_kind, MessageKind::QueryError));
    }

    #[tokio::test]
    async fn test_query_aggregation_error_returns_query_error() {
        let index = make_index(vec![
            make_event("e1", vec![("status", Value::Number(500.0))]),
        ]);

        let (response, _) = roundtrip(index, Message::new(MessageKind::Query, "status = 500 | avg latency_ms".into())).await;

        assert!(matches!(response.message_kind, MessageKind::QueryError));
    }

    #[tokio::test]
    async fn test_count_aggregation() {
        let index = make_index(vec![
            make_event("e1", vec![("status", Value::Number(500.0))]),
            make_event("e2", vec![("status", Value::Number(500.0))]),
            make_event("e3", vec![("status", Value::Number(200.0))]),
        ]);

        let (response, _) = roundtrip(index, Message::new(MessageKind::Query, "status = 500 | count".into())).await;

        assert!(matches!(response.message_kind, MessageKind::QueryResponse));
        assert_eq!(response.data, "2");
    }

    #[tokio::test]
    async fn test_response_kind_request_returns_error_not_panic() {
        // A malicious or confused client sending a response-kind message must
        // get an error back — it previously panicked the handler task.
        let index = make_index(vec![]);

        let (response, _) = roundtrip(
            index,
            Message::new(MessageKind::ShutdownAck, String::new()),
        )
        .await;

        assert!(matches!(response.message_kind, MessageKind::QueryError));
    }

    #[tokio::test]
    async fn test_oversized_request_is_rejected() {
        let index = make_index(vec![]);
        let (client, server) = UnixStream::pair().unwrap();
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let (watch_tx, _watch_rx) = mpsc::channel(1);
        let (follow_tx, _) = broadcast::channel(16);

        let writer_task = tokio::spawn(async move {
            let (_reader, mut writer) = client.into_split();
            let chunk = vec![b'x'; 64 * 1024];
            // 2 MiB of garbage, well past the 1 MiB request cap.
            for _ in 0..32 {
                if writer.write_all(&chunk).await.is_err() {
                    return;
                }
            }
            let _ = writer.shutdown().await;
        });

        let result = handle_client(server, index, shutdown_tx, watch_tx, None, follow_tx).await;
        assert!(result.is_err());
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn test_shutdown_returns_ack_and_signals_channel() {
        let index = make_index(vec![]);

        let (response, mut shutdown_rx) = roundtrip(index, Message::new(MessageKind::Shutdown, String::new())).await;

        assert!(matches!(response.message_kind, MessageKind::ShutdownAck));
        assert!(shutdown_rx.try_recv().is_ok());
    }

    // --- Follow ---

    /// Send a Follow message and return the client's read half plus the
    /// broadcast sender feeding the handler.
    async fn start_follow(
        query: &str,
    ) -> (
        tokio::net::unix::OwnedReadHalf,
        broadcast::Sender<Arc<Event>>,
    ) {
        let index = make_index(vec![]);
        let (client, server) = UnixStream::pair().unwrap();
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let (watch_tx, _watch_rx) = mpsc::channel(1);
        let (follow_tx, _) = broadcast::channel(16);
        tokio::spawn(handle_client(
            server,
            index,
            shutdown_tx,
            watch_tx,
            None,
            follow_tx.clone(),
        ));

        let (reader, mut writer) = client.into_split();
        let msg = Message::new(MessageKind::Follow, query.to_string());
        writer.write_all(&to_vec(&msg).unwrap()).await.unwrap();
        writer.shutdown().await.unwrap();

        (reader, follow_tx)
    }

    #[tokio::test]
    async fn test_follow_streams_only_matching_raw_lines() {
        let (mut reader, follow_tx) = start_follow("level = ERROR").await;

        // Wait for the handler to subscribe so the events aren't dropped.
        while follow_tx.receiver_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        follow_tx
            .send(Arc::new(make_event(
                "matching",
                vec![("level", Value::String("ERROR".into()))],
            )))
            .unwrap();
        follow_tx
            .send(Arc::new(make_event(
                "non-matching",
                vec![("level", Value::String("INFO".into()))],
            )))
            .unwrap();

        // Drop the sender so the handler sees Closed and ends the stream.
        drop(follow_tx);

        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "matching\n");
    }

    #[tokio::test]
    async fn test_follow_rejects_pipeline_stages() {
        let (mut reader, follow_tx) = start_follow("level = ERROR | count").await;

        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            text.contains("follow supports filters only"),
            "unexpected response: {text}"
        );
        assert_eq!(follow_tx.receiver_count(), 0);
    }

    #[tokio::test]
    async fn test_follow_parse_error_writes_error_and_closes() {
        let (mut reader, follow_tx) = start_follow("level ???").await;

        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert_eq!(follow_tx.receiver_count(), 0);
    }
}
