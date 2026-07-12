use clap::Parser;
use message::{Message, MessageKind, WatchPayload};
use serde_json::{from_slice, to_vec};
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::index::Index;
use crate::recover::data_dir;
use crate::recover::recover;
use crate::wal::WAL;

mod checkpoint;
mod event;
mod handler;
mod index;
mod message;
mod parser;
mod query;
mod recover;
mod snapshot;
mod tailer;
mod value;
mod wal;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    watch: Option<PathBuf>,

    #[arg(long)]
    time_field: Option<String>,

    #[arg(short, long)]
    detach: bool,

    #[arg(long)]
    stop: bool,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long, default_value_t = 500)]
    fsync_interval_ms: u64,

    #[arg(long, default_value_t = 60)]
    checkpoint_interval_secs: u64,

    #[arg(long)]
    no_persist: bool,

    query: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let time_field = args.time_field;

    match (args.watch, args.query, args.stop, args.detach) {
        (_, _, true, _) => run_stop().await,
        (Some(path), None, false, detach) => {
            run_daemon(
                path,
                detach,
                time_field,
                args.data_dir,
                Duration::from_millis(args.fsync_interval_ms),
                Duration::from_secs(args.checkpoint_interval_secs),
                args.no_persist,
            )
            .await
        }
        (None, Some(q), false, _) => run_query(q).await,
        _ => {
            eprintln!("Usage: vigil --watch <file> [--time-field <field>] [-d]  |  vigil \"<query>\"  |  vigil --stop");
            std::process::exit(1);
        }
    }
}

async fn run_daemon(
    path: PathBuf,
    detach: bool,
    time_field: Option<String>,
    data_dir_override: Option<PathBuf>,
    fsync_interval: Duration,
    checkpoint_interval: Duration,
    no_persist: bool,
) {
    let file_path = match File::open(&path) {
        Ok(_) => path,
        Err(e) => {
            match e.kind() {
                io::ErrorKind::NotFound => eprintln!("File not found: {}", path.display()),
                io::ErrorKind::PermissionDenied => {
                    eprintln!("Permission denied: {}", path.display())
                }
                _ => eprintln!("Unexpected error: {}", e),
            }
            std::process::exit(1);
        }
    };

    if detach {
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.arg("--watch").arg(&file_path);
        if let Some(ref tf) = time_field {
            cmd.arg("--time-field").arg(tf);
        }
        if let Some(ref dir) = data_dir_override {
            cmd.arg("--data-dir").arg(dir);
        }
        cmd.arg("--fsync-interval-ms")
            .arg(fsync_interval.as_millis().to_string());
        cmd.arg("--checkpoint-interval-secs")
            .arg(checkpoint_interval.as_secs().to_string());
        if no_persist {
            cmd.arg("--no-persist");
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        return;
    }

    if Path::new(message::SOCKET_PATH).exists() {
        match send_watch(file_path.clone(), time_field.clone()).await {
            Ok(()) => return,
            Err(_) => std::fs::remove_file(message::SOCKET_PATH).ok(),
        };
    }

    let (index_inner, wal_inner, byte_offset, mut resolved_dir) =
        resolve_state(&file_path, data_dir_override.clone(), no_persist).await;

    let index = Arc::new(RwLock::new(index_inner));
    let mut wal: Option<Arc<Mutex<WAL>>> = wal_inner.map(|w| Arc::new(Mutex::new(w)));

    let listener = UnixListener::bind(message::SOCKET_PATH).unwrap();

    let mut tailer_handle = tokio::spawn(tailer::run_tailer(
        file_path,
        index.clone(),
        time_field.clone(),
        wal.clone(),
        byte_offset,
    ));
    let mut flush_handle = spawn_flush_task(&wal, fsync_interval);
    let mut checkpoint_handle = spawn_checkpoint_task(
        &resolved_dir,
        &index,
        &wal,
        &time_field,
        checkpoint_interval,
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let (watch_tx, mut watch_rx) = mpsc::channel::<(PathBuf, Option<String>)>(1);
    let mut current_time_field = time_field;

    loop {
        tokio::select! {
            result = &mut tailer_handle => {
                if let Err(e) = result.unwrap() {
                    eprintln!("Tailer error: {e}");
                    std::process::exit(1);
                }
                break;
            }
            result = &mut flush_handle => {
                if let Err(e) = result.unwrap() {
                    eprintln!("Flush task error: {e}");
                    std::process::exit(1);
                }
                break;
            }
            result = &mut checkpoint_handle => {
                if let Err(e) = result.unwrap() {
                    eprintln!("Checkpoint task error (continuing): {e}");
                }
                checkpoint_handle = spawn_checkpoint_task(
                    &resolved_dir,
                    &index,
                    &wal,
                    &current_time_field,
                    checkpoint_interval,
                );
            }
            Ok((stream, _)) = listener.accept() => {
                let client_handle = tokio::spawn(handler::handle_client(
                    stream,
                    index.clone(),
                    shutdown_tx.clone(),
                    watch_tx.clone(),
                    current_time_field.clone(),
                ));
                if let Err(e) = client_handle.await.unwrap() {
                    eprintln!("Client error: {e}");
                }
            }
            _ = shutdown_rx.recv() => {
                flush_handle.abort();
                checkpoint_handle.abort();
                let _ = flush_handle.await;
                let _ = checkpoint_handle.await;

                if let (Some(dir), Some(w)) = (&resolved_dir, &wal) {
                    if let Err(e) = checkpoint::checkpoint(dir, &index, w, &current_time_field).await {
                        eprintln!("Final checkpoint failed: {e}");
                    }
                }
                break;
            }
            Some((new_path, new_time_field)) = watch_rx.recv() => {
                tailer_handle.abort();
                let _ = tailer_handle.await;
                flush_handle.abort();
                checkpoint_handle.abort();
                let _ = flush_handle.await;
                let _ = checkpoint_handle.await;

                if let (Some(dir), Some(w)) = (&resolved_dir, &wal) {
                    if let Err(e) = checkpoint::checkpoint(dir, &index, w, &current_time_field).await {
                        eprintln!("Checkpoint before re-watch failed: {e}");
                    }
                }

                current_time_field = new_time_field.clone();

                let (new_index, new_wal, new_byte_offset, new_dir) =
                    resolve_state(&new_path, data_dir_override.clone(), no_persist).await;

                *index.write().unwrap() = new_index;
                wal = new_wal.map(|w| Arc::new(Mutex::new(w)));
                resolved_dir = new_dir;

                tailer_handle = tokio::spawn(tailer::run_tailer(
                    new_path,
                    index.clone(),
                    new_time_field,
                    wal.clone(),
                    new_byte_offset,
                ));
                flush_handle = spawn_flush_task(&wal, fsync_interval);
                checkpoint_handle = spawn_checkpoint_task(
                    &resolved_dir,
                    &index,
                    &wal,
                    &current_time_field,
                    checkpoint_interval,
                );
            }
        }
    }

    std::fs::remove_file(message::SOCKET_PATH).ok();
}

async fn resolve_state(
    path: &Path,
    data_dir_override: Option<PathBuf>,
    no_persist: bool,
) -> (Index, Option<WAL>, u64, Option<PathBuf>) {
    if no_persist {
        return (Index::new(), None, 0, None);
    }

    let dir = match data_dir(path, data_dir_override).await {
        Ok(dir) => dir,
        Err(e) => {
            eprintln!("Failed to resolve data directory: {e}");
            std::process::exit(1);
        }
    };

    let recovered = match recover(&dir).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!(
                "Failed to recover persisted state for {}: {e}",
                dir.display()
            );
            std::process::exit(1);
        }
    };

    (
        recovered.index,
        Some(recovered.wal),
        recovered.byte_offset,
        Some(dir),
    )
}

fn spawn_flush_task(
    wal: &Option<Arc<Mutex<WAL>>>,
    interval: Duration,
) -> tokio::task::JoinHandle<io::Result<()>> {
    match wal {
        Some(w) => tokio::spawn(checkpoint::run_flush_task(w.clone(), interval)),
        None => tokio::spawn(std::future::pending()),
    }
}

fn spawn_checkpoint_task(
    dir: &Option<PathBuf>,
    index: &Arc<RwLock<Index>>,
    wal: &Option<Arc<Mutex<WAL>>>,
    time_field: &Option<String>,
    interval: Duration,
) -> tokio::task::JoinHandle<io::Result<()>> {
    match (dir, wal) {
        (Some(dir), Some(w)) => tokio::spawn(checkpoint::run_checkpoint_task(
            dir.clone(),
            index.clone(),
            w.clone(),
            time_field.clone(),
            interval,
        )),
        _ => tokio::spawn(std::future::pending()),
    }
}

async fn send_watch(path: PathBuf, time_field: Option<String>) -> Result<(), io::Error> {
    let stream = UnixStream::connect(message::SOCKET_PATH).await?;
    let (mut reader, mut writer) = stream.into_split();
    let payload = WatchPayload { path, time_field };
    let msg = Message::new(MessageKind::Watch, serde_json::to_string(&payload).unwrap());
    writer.write_all(&to_vec(&msg).unwrap()).await?;
    writer.shutdown().await?;
    let mut buf = vec![];
    reader.read_to_end(&mut buf).await?;
    let response: Message =
        from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    match response.message_kind {
        MessageKind::WatchAck => Ok(()),
        _ => Err(io::Error::new(io::ErrorKind::Other, "unexpected response")),
    }
}

async fn run_stop() {
    let stream = match UnixStream::connect(message::SOCKET_PATH).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("Failed to connect to daemon. Check if the daemon is running.");
            std::process::exit(1);
        }
    };

    let (mut reader, mut writer) = stream.into_split();
    let message = Message::new(MessageKind::Shutdown, String::new());
    let bytes = to_vec(&message).unwrap();

    writer.write_all(&bytes).await.unwrap();
    writer.shutdown().await.unwrap();

    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf).await.unwrap();
    let response: Message = match from_slice(&buf) {
        Ok(m) => m,
        Err(_) => {
            eprintln!("Failed to parse daemon response.");
            std::process::exit(1);
        }
    };

    match response.message_kind {
        MessageKind::ShutdownAck => println!("Daemon stopped."),
        _ => {
            eprintln!("Unexpected response from daemon.");
            std::process::exit(1);
        }
    }
}

async fn run_query(raw_query: String) {
    let stream = match UnixStream::connect(message::SOCKET_PATH).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("Failed to connect to daemon. Check if the daemon is running.");
            std::process::exit(1);
        }
    };

    let (mut reader, mut writer) = stream.into_split();
    let message = Message::new(MessageKind::Query, raw_query);
    let bytes = to_vec(&message).unwrap();

    match writer.write_all(&bytes).await {
        Ok(()) => {
            writer.shutdown().await.unwrap();
        }
        Err(_) => {
            eprintln!("Failed to write to daemon.");
            std::process::exit(1);
        }
    }

    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf).await.unwrap();
    let response: Message = match from_slice(&buf) {
        Ok(m) => m,
        Err(_) => {
            eprintln!("Failed to parse daemon response.");
            std::process::exit(1);
        }
    };

    match response.message_kind {
        MessageKind::QueryResponse => println!("{}", response.data),
        MessageKind::QueryError => {
            eprintln!("Error: {}", response.data);
            std::process::exit(1);
        }
        _ => {
            eprintln!("Unexpected response from daemon.");
            std::process::exit(1);
        }
    }
}
