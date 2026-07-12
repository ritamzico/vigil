use crate::event::PersistedEvent;
use crate::index::Index;
use crate::snapshot::Snapshot;
use crate::wal::WAL;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::time::sleep;
use tokio::{io, sync::Mutex};

pub async fn checkpoint(
    dir: &Path,
    index: &Arc<RwLock<Index>>,
    wal: &Arc<Mutex<WAL>>,
    time_field: &Option<String>,
) -> io::Result<()> {
    let mut wal_guard = wal.lock().await;

    let snapshot = {
        let index_guard = index.read().unwrap();
        Snapshot::new(
            time_field.clone(),
            wal_guard.last_byte_offset,
            wal_guard.next_seq,
            index_guard
                .events
                .iter()
                .map(|event| PersistedEvent::from_event(event))
                .collect(),
        )
    };

    snapshot.save(dir).await?;
    wal_guard.truncate().await?;

    Ok(())
}

pub async fn run_flush_task(wal: Arc<Mutex<WAL>>, interval: Duration) -> io::Result<()> {
    loop {
        sleep(interval).await;
        wal.lock().await.flush_fsync().await?;
    }
}

pub async fn run_checkpoint_task(
    dir: PathBuf,
    index: Arc<RwLock<Index>>,
    wal: Arc<Mutex<WAL>>,
    time_field: Option<String>,
    interval: Duration,
) -> io::Result<()> {
    loop {
        sleep(interval).await;
        checkpoint(&dir, &index, &wal, &time_field).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn make_event(raw: &str) -> Event {
        Event::new(None, raw.to_string(), HashMap::new())
    }

    async fn make_wal(dir: &Path) -> Arc<Mutex<WAL>> {
        let wal = WAL::open(&dir.join("wal.log"), None).await.unwrap();
        Arc::new(Mutex::new(wal))
    }

    #[tokio::test]
    async fn test_checkpoint_saves_snapshot_from_index_and_wal_state() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(Index::new()));
        index.write().unwrap().push_event(make_event("a"));
        index.write().unwrap().push_event(make_event("b"));

        let wal = make_wal(dir.path()).await;
        wal.lock()
            .await
            .append(5, PersistedEvent::from_event(&make_event("a")))
            .await
            .unwrap();
        wal.lock()
            .await
            .append(15, PersistedEvent::from_event(&make_event("b")))
            .await
            .unwrap();

        checkpoint(dir.path(), &index, &wal, &Some("ts".to_string()))
            .await
            .unwrap();

        let snapshot = Snapshot::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(snapshot.next_seq, 2);
        assert_eq!(snapshot.byte_offset, 15);
        assert_eq!(snapshot.events.len(), 2);
    }

    #[tokio::test]
    async fn test_checkpoint_truncates_wal_after_saving() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(Index::new()));
        index.write().unwrap().push_event(make_event("a"));

        let wal = make_wal(dir.path()).await;
        wal.lock()
            .await
            .append(5, PersistedEvent::from_event(&make_event("a")))
            .await
            .unwrap();

        checkpoint(dir.path(), &index, &wal, &None).await.unwrap();

        let records = wal.lock().await.replay().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_checkpoint_on_empty_index_and_wal() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(Index::new()));
        let wal = make_wal(dir.path()).await;

        checkpoint(dir.path(), &index, &wal, &None).await.unwrap();

        let snapshot = Snapshot::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(snapshot.next_seq, 0);
        assert_eq!(snapshot.byte_offset, 0);
        assert_eq!(snapshot.events.len(), 0);
    }

    #[tokio::test]
    async fn test_run_flush_task_flushes_unsynced_appends() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let wal = Arc::new(Mutex::new(WAL::open(tmp.path(), None).await.unwrap()));

        wal.lock()
            .await
            .append(0, PersistedEvent::from_event(&make_event("hello")))
            .await
            .unwrap();

        let handle = tokio::spawn(run_flush_task(wal.clone(), Duration::from_millis(20)));
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.abort();
        let _ = handle.await;

        let bytes = tokio::fs::read(tmp.path()).await.unwrap();
        assert!(!bytes.is_empty());
    }

    #[tokio::test]
    async fn test_run_checkpoint_task_checkpoints_periodically() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(Index::new()));
        index.write().unwrap().push_event(make_event("a"));

        let wal = make_wal(dir.path()).await;
        wal.lock()
            .await
            .append(5, PersistedEvent::from_event(&make_event("a")))
            .await
            .unwrap();

        let handle = tokio::spawn(run_checkpoint_task(
            dir.path().to_path_buf(),
            index.clone(),
            wal.clone(),
            None,
            Duration::from_millis(20),
        ));
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.abort();
        let _ = handle.await;

        let snapshot = Snapshot::load(dir.path()).await.unwrap();
        assert!(snapshot.is_some());

        let records = wal.lock().await.replay().await.unwrap();
        assert!(records.is_empty());
    }
}
