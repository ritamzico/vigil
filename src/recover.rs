use std::path::{Path, PathBuf};
use tokio::fs::create_dir_all;
use tokio::io;

use crate::index::Index;
use crate::snapshot::Snapshot;
use crate::wal::{WALRecord, WAL};

const DATA_DIR_SUFFIX: &str = ".local/share/vigil";
const WAL_FILE_NAME: &str = "wal.log";

pub struct Recovered {
    pub index: Index,
    pub wal: WAL,
    pub byte_offset: u64,
}

impl Recovered {
    pub fn new(index: Index, wal: WAL, byte_offset: u64) -> Recovered {
        Recovered {
            index,
            wal,
            byte_offset,
        }
    }
}

pub async fn recover(dir: &Path) -> io::Result<Recovered> {
    let mut index = Index::new();
    let mut next_seq: u64 = 0;
    let mut byte_offset: u64 = 0;

    if let Some(snapshot) = Snapshot::load(dir).await? {
        next_seq = snapshot.next_seq;
        byte_offset = snapshot.byte_offset;

        for event in snapshot.events {
            index.push_event(event.into_event());
        }
    };

    let mut wal = WAL::open(&dir.join(WAL_FILE_NAME), None).await?;
    let records = wal.replay().await?;

    let records: Vec<WALRecord> = records.into_iter().filter(|r| r.seq >= next_seq).collect();

    if let Some(last) = records.last() {
        next_seq = last.seq + 1;
        byte_offset = last.byte_offset;
    }

    for record in records {
        index.push_event(record.event.into_event());
    }

    wal.next_seq = next_seq;
    wal.last_byte_offset = byte_offset;

    Ok(Recovered::new(index, wal, byte_offset))
}

pub async fn data_dir(source: &Path, override_dir: Option<PathBuf>) -> io::Result<PathBuf> {
    let prefix = match override_dir {
        Some(dir) => dir,
        None => {
            let home = std::env::var("HOME")
                .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "HOME not set"))?;
            PathBuf::from(home).join(DATA_DIR_SUFFIX)
        }
    };

    let canonical = source.canonicalize()?;
    let source_str = canonical.to_string_lossy();
    let bytes = source_str.as_bytes();
    let suffix = crc32fast::hash(bytes).to_string();
    let data_dir = prefix.join(suffix);

    create_dir_all(&data_dir).await?;

    Ok(data_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PersistedEvent;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn make_event(raw: &str) -> PersistedEvent {
        PersistedEvent::new(None, raw.to_string(), HashMap::new())
    }

    async fn write_wal_records(dir: &Path, start_seq: u64, raws: &[&str]) {
        let mut wal = WAL::open(&dir.join(WAL_FILE_NAME), Some(start_seq))
            .await
            .unwrap();
        for (i, raw) in raws.iter().enumerate() {
            let byte_offset = (start_seq + i as u64 + 1) * 10;
            wal.append(byte_offset, make_event(raw)).await.unwrap();
        }
        wal.flush_fsync().await.unwrap();
    }

    #[tokio::test]
    async fn test_recover_empty_dir_returns_empty_index() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path()).await.unwrap();

        assert_eq!(recovered.index.event_count(), 0);
        assert_eq!(recovered.byte_offset, 0);
        assert_eq!(recovered.wal.next_seq, 0);
        assert_eq!(recovered.wal.last_byte_offset, 0);
    }

    #[tokio::test]
    async fn test_recover_replays_wal_when_no_snapshot() {
        let dir = tempdir().unwrap();
        write_wal_records(dir.path(), 0, &["a", "b", "c"]).await;

        let recovered = recover(dir.path()).await.unwrap();

        assert_eq!(recovered.index.event_count(), 3);
        assert_eq!(recovered.wal.next_seq, 3);
        assert_eq!(recovered.byte_offset, 30);
    }

    #[tokio::test]
    async fn test_recover_loads_snapshot_with_no_new_wal_records() {
        let dir = tempdir().unwrap();
        let snapshot = Snapshot::new(1, None, 50, 5, vec![make_event("from-snapshot")]);
        snapshot.save(dir.path()).await.unwrap();
        // touch an empty wal.log so WAL::open succeeds
        WAL::open(&dir.path().join(WAL_FILE_NAME), None)
            .await
            .unwrap();

        let recovered = recover(dir.path()).await.unwrap();

        assert_eq!(recovered.index.event_count(), 1);
        assert_eq!(recovered.byte_offset, 50);
        assert_eq!(recovered.wal.next_seq, 5);
    }

    #[tokio::test]
    async fn test_recover_merges_snapshot_and_new_wal_records() {
        let dir = tempdir().unwrap();
        let snapshot_events = vec![make_event("s0"), make_event("s1"), make_event("s2")];
        Snapshot::new(1, None, 30, 3, snapshot_events)
            .save(dir.path())
            .await
            .unwrap();

        write_wal_records(dir.path(), 3, &["w3", "w4"]).await;

        let recovered = recover(dir.path()).await.unwrap();

        assert_eq!(recovered.index.event_count(), 5);
        assert_eq!(recovered.wal.next_seq, 5);
    }

    #[tokio::test]
    async fn test_recover_skips_wal_records_already_in_snapshot() {
        // Simulates a crash between the snapshot write and the WAL truncate:
        // snapshot already covers seq 0-4 (next_seq=5), but the WAL still
        // holds 3-6 because truncation never happened.
        let dir = tempdir().unwrap();
        let snapshot_events: Vec<_> = (0..5).map(|i| make_event(&format!("s{i}"))).collect();
        Snapshot::new(1, None, 40, 5, snapshot_events)
            .save(dir.path())
            .await
            .unwrap();

        write_wal_records(dir.path(), 3, &["s3", "s4", "w5", "w6"]).await;

        let recovered = recover(dir.path()).await.unwrap();

        // 5 from the snapshot + only seq 5 and 6 from the WAL (3 and 4 filtered out)
        assert_eq!(recovered.index.event_count(), 7);
        assert_eq!(recovered.wal.next_seq, 7);
    }

    #[tokio::test]
    async fn test_recover_seeds_wal_to_continue_appending() {
        let dir = tempdir().unwrap();
        write_wal_records(dir.path(), 0, &["a", "b"]).await;

        let mut recovered = recover(dir.path()).await.unwrap();
        assert_eq!(recovered.wal.next_seq, 2);

        recovered.wal.append(999, make_event("c")).await.unwrap();
        assert_eq!(recovered.wal.next_seq, 3);
        assert_eq!(recovered.wal.last_byte_offset, 999);
    }

    #[tokio::test]
    async fn test_data_dir_different_sources_produce_different_dirs() {
        let sources = tempdir().unwrap();
        let file_a = sources.path().join("a.log");
        let file_b = sources.path().join("b.log");
        std::fs::write(&file_a, "").unwrap();
        std::fs::write(&file_b, "").unwrap();

        let override_dir = tempdir().unwrap().path().to_path_buf();
        let dir_a = data_dir(&file_a, Some(override_dir.clone())).await.unwrap();
        let dir_b = data_dir(&file_b, Some(override_dir)).await.unwrap();

        assert_ne!(dir_a, dir_b);
    }

    #[tokio::test]
    async fn test_data_dir_same_source_produces_same_dir() {
        let sources = tempdir().unwrap();
        let file_a = sources.path().join("a.log");
        std::fs::write(&file_a, "").unwrap();

        let override_dir = tempdir().unwrap().path().to_path_buf();
        let dir_1 = data_dir(&file_a, Some(override_dir.clone())).await.unwrap();
        let dir_2 = data_dir(&file_a, Some(override_dir)).await.unwrap();

        assert_eq!(dir_1, dir_2);
    }

    #[tokio::test]
    async fn test_data_dir_creates_directory_on_demand() {
        let sources = tempdir().unwrap();
        let file_a = sources.path().join("a.log");
        std::fs::write(&file_a, "").unwrap();

        let override_dir = tempdir().unwrap().path().to_path_buf();
        let dir = data_dir(&file_a, Some(override_dir)).await.unwrap();

        assert!(dir.exists());
    }

    #[tokio::test]
    async fn test_data_dir_nests_under_override() {
        let sources = tempdir().unwrap();
        let file_a = sources.path().join("a.log");
        std::fs::write(&file_a, "").unwrap();

        let override_dir = tempdir().unwrap().path().to_path_buf();
        let dir = data_dir(&file_a, Some(override_dir.clone())).await.unwrap();

        assert!(dir.starts_with(&override_dir));
    }

    #[tokio::test]
    async fn test_data_dir_errors_on_nonexistent_source() {
        let override_dir = tempdir().unwrap().path().to_path_buf();
        let missing = PathBuf::from("/nonexistent/path/does-not-exist.log");

        let result = data_dir(&missing, Some(override_dir)).await;
        assert!(result.is_err());
    }
}
