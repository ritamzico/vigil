use crate::event::Event;
use crate::event::PersistedEvent;
use crate::index::Index;
use crate::value::Value;
use crate::wal::WAL;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{self, AsyncBufReadExt, AsyncSeekExt};
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

const SLEEP_TIME: u64 = 100;

/// Read everything already in the file once, returning the resulting byte
/// offset. Callers run this before opening the query socket so clients never
/// observe a half-loaded index.
pub async fn initial_read(
    file_path: &PathBuf,
    index: &Arc<RwLock<Index>>,
    time_field: Option<&str>,
    wal: &Option<Arc<Mutex<WAL>>>,
    starting_byte_offset: u64,
) -> Result<u64, io::Error> {
    // Runs before the query socket opens, so nobody can be following yet.
    read_file(file_path, index, starting_byte_offset, time_field, wal, None).await
}

pub async fn run_tailer(
    file_path: PathBuf,
    index: Arc<RwLock<Index>>,
    time_field: Option<String>,
    wal: Option<Arc<Mutex<WAL>>>,
    starting_byte_offset: u64,
    follow_tx: broadcast::Sender<Arc<Event>>,
) -> Result<(), io::Error> {
    let mut byte_offset = starting_byte_offset;

    let mut last_inode = fs::metadata(&file_path).await.ok().map(|m| m.ino());

    loop {
        (byte_offset, last_inode) = check_rotation(&file_path, byte_offset, last_inode).await?;
        byte_offset = read_file(
            &file_path,
            &index,
            byte_offset,
            time_field.as_deref(),
            &wal,
            Some(&follow_tx),
        )
        .await?;
        sleep(Duration::from_millis(SLEEP_TIME)).await;
    }
}

async fn check_rotation(
    file_path: &PathBuf,
    byte_offset: u64,
    last_inode: Option<u64>,
) -> io::Result<(u64, Option<u64>)> {
    let Ok(metadata) = fs::metadata(file_path).await else {
        return Ok((byte_offset, last_inode));
    };

    let new_inode = Some(metadata.ino());

    if metadata.len() < byte_offset || last_inode != new_inode {
        return Ok((0, new_inode));
    }

    Ok((byte_offset, last_inode))
}

async fn read_file(
    file_path: &PathBuf,
    index: &Arc<RwLock<Index>>,
    mut byte_offset: u64,
    time_field: Option<&str>,
    wal: &Option<Arc<Mutex<WAL>>>,
    follow_tx: Option<&broadcast::Sender<Arc<Event>>>,
) -> Result<u64, io::Error> {
    let Ok(file) = File::open(file_path).await else {
        return Ok(byte_offset);
    };
    let mut reader = io::BufReader::new(file);

    reader.seek(SeekFrom::Start(byte_offset)).await?;
    let mut lines = reader.lines();

    let mut line_num: u64 = 0;

    while let Some(line) = lines.next_line().await? {
        byte_offset += line.len() as u64 + 1; // +1 for the stripped newline
        line_num += 1;

        let json: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
            let preview: String = line.chars().take(80).collect();
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("line {line_num} is not valid JSON: {e}\n  {preview}"),
            )
        })?;

        let mut timestamp: Option<DateTime<Utc>> = None;
        let mut fields: HashMap<String, Value> = HashMap::new();

        if let serde_json::Value::Object(map) = json {
            for (key, value) in map {
                if time_field == Some(key.as_str()) {
                    if let serde_json::Value::String(ref s) = value {
                        timestamp = DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.to_utc());
                    }
                } else if let Some(val) = Value::from_json(value) {
                    fields.insert(key, val);
                }
            }
        }

        let event = Event::new(timestamp, line, fields);

        if let Some(wal) = wal {
            wal.lock()
                .await
                .append(byte_offset, PersistedEvent::from_event(&event))
                .await?;
        }

        if let Some(tx) = follow_tx {
            // Zero cost when nobody follows.
            if tx.receiver_count() > 0 {
                let _ = tx.send(Arc::new(event.clone()));
            }
        }

        index.write().unwrap().push_event(event);
    }

    Ok(byte_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ComparisonOp, Query};
    use tempfile::NamedTempFile;

    fn make_index() -> Arc<RwLock<Index>> {
        Arc::new(RwLock::new(Index::new(None)))
    }

    async fn make_wal() -> Option<Arc<Mutex<WAL>>> {
        let tmp = NamedTempFile::new().unwrap();
        let wal = WAL::open(tmp.path(), None).await.unwrap();
        Some(Arc::new(Mutex::new(wal)))
    }

    async fn read(
        path: &str,
        index: &Arc<RwLock<Index>>,
        offset: u64,
        time_field: Option<&str>,
    ) -> u64 {
        let wal = make_wal().await;
        read_file(&PathBuf::from(path), index, offset, time_field, &wal, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_empty_file() {
        let index = make_index();
        let offset = read("test_files/empty.log", &index, 0, None).await;
        assert_eq!(offset, 0);
        assert_eq!(index.read().unwrap().event_count(), 0);
    }

    #[tokio::test]
    async fn test_no_wal_still_indexes_events() {
        let index = make_index();
        let offset = read_file(
            &PathBuf::from("test_files/test.log"),
            &index,
            0,
            None,
            &None,
            None,
        )
        .await
        .unwrap();
        assert!(offset > 0);
        assert_eq!(index.read().unwrap().event_count(), 25);
    }

    #[tokio::test]
    async fn test_basic_fields_parsed() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        assert_eq!(idx.event_count(), 25);
    }

    #[tokio::test]
    async fn test_string_field_stored() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "level".to_string(),
            op: ComparisonOp::Eq,
            value: Value::String("ERROR".to_string()),
        };
        assert_eq!(idx.apply_query(&q).len(), 8);
    }

    #[tokio::test]
    async fn test_warn_level_count() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "level".to_string(),
            op: ComparisonOp::Eq,
            value: Value::String("WARN".to_string()),
        };
        assert_eq!(idx.apply_query(&q).len(), 4);
    }

    #[tokio::test]
    async fn test_status_200_count() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: Value::Number(200.0),
        };
        assert_eq!(idx.apply_query(&q).len(), 12);
    }

    #[tokio::test]
    async fn test_status_500_count() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: Value::Number(500.0),
        };
        assert_eq!(idx.apply_query(&q).len(), 4);
    }

    #[tokio::test]
    async fn test_method_post_count() {
        let index = make_index();
        read("test_files/test.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "method".to_string(),
            op: ComparisonOp::Eq,
            value: Value::String("POST".to_string()),
        };
        assert_eq!(idx.apply_query(&q).len(), 9);
    }

    #[tokio::test]
    async fn test_timestamp_parsed() {
        let index = make_index();
        read("test_files/timestamps.log", &index, 0, Some("time")).await;
        let idx = index.read().unwrap();
        assert_eq!(idx.event_count(), 15);
        let start = chrono::DateTime::parse_from_rfc3339("2026-01-01T08:00:00Z")
            .unwrap()
            .to_utc();
        let end = chrono::DateTime::parse_from_rfc3339("2026-01-01T20:00:00Z")
            .unwrap()
            .to_utc();
        let q = Query::TimeRange {
            start: Some(start),
            end: Some(end),
        };
        // 08:00, 10:45, 12:00, 14:30, 16:00, 18:20, 20:00 (bounds inclusive)
        assert_eq!(idx.apply_query(&q).len(), 7);
    }

    #[tokio::test]
    async fn test_time_range_morning() {
        let index = make_index();
        read("test_files/timestamps.log", &index, 0, Some("time")).await;
        let idx = index.read().unwrap();
        let start = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        let end = chrono::DateTime::parse_from_rfc3339("2026-01-01T04:00:00Z")
            .unwrap()
            .to_utc();
        let q = Query::TimeRange {
            start: Some(start),
            end: Some(end),
        };
        // 00:00, 01:00, 02:30, 04:00 (bounds inclusive)
        assert_eq!(idx.apply_query(&q).len(), 4);
    }

    #[tokio::test]
    async fn test_no_time_field_means_no_time_indexing() {
        let index = make_index();
        read("test_files/timestamps.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::TimeRange {
            start: None,
            end: None,
        };
        assert_eq!(idx.apply_query(&q).len(), 0);
    }

    #[tokio::test]
    async fn test_wrong_time_field_means_no_time_indexing() {
        let index = make_index();
        read("test_files/timestamps.log", &index, 0, Some("created_at")).await;
        let idx = index.read().unwrap();
        let q = Query::TimeRange {
            start: None,
            end: None,
        };
        assert_eq!(idx.apply_query(&q).len(), 0);
    }

    #[tokio::test]
    async fn test_array_and_object_fields_ignored() {
        let index = make_index();
        read("test_files/types.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        // "tags" (array) and "meta" (object) should not appear in field_index
        let q = Query::FieldComparison {
            field: "tags".to_string(),
            op: ComparisonOp::Eq,
            value: Value::String("rust".to_string()),
        };
        assert_eq!(idx.apply_query(&q).len(), 0);
    }

    #[tokio::test]
    async fn test_bool_true_field_stored() {
        let index = make_index();
        read("test_files/types.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "active".to_string(),
            op: ComparisonOp::Eq,
            value: Value::Bool(true),
        };
        assert_eq!(idx.apply_query(&q).len(), 5);
    }

    #[tokio::test]
    async fn test_bool_false_field_stored() {
        let index = make_index();
        read("test_files/types.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "active".to_string(),
            op: ComparisonOp::Eq,
            value: Value::Bool(false),
        };
        assert_eq!(idx.apply_query(&q).len(), 5);
    }

    #[tokio::test]
    async fn test_number_gt_comparison() {
        let index = make_index();
        read("test_files/types.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "age".to_string(),
            op: ComparisonOp::Gt,
            value: Value::Number(30.0),
        };
        // charlie(35), eve(42), grace(33), henry(55), jack(38)
        assert_eq!(idx.apply_query(&q).len(), 5);
    }

    #[tokio::test]
    async fn test_number_le_comparison() {
        let index = make_index();
        read("test_files/types.log", &index, 0, None).await;
        let idx = index.read().unwrap();
        let q = Query::FieldComparison {
            field: "age".to_string(),
            op: ComparisonOp::Le,
            value: Value::Number(25.0),
        };
        // bob(25), frank(19), iris(23)
        assert_eq!(idx.apply_query(&q).len(), 3);
    }

    #[allow(unused_variables)]
    #[tokio::test]
    async fn test_byte_offset_incremental_read() {
        let index = make_index();
        let index2 = make_index();
        let offset = read("test_files/types.log", &index2, 0, None).await;

        let index3 = make_index();
        let offset2 = read("test_files/types.log", &index3, offset, None).await;
        assert_eq!(offset2, offset);
        assert_eq!(index3.read().unwrap().event_count(), 0);
    }

    #[tokio::test]
    async fn test_nonexistent_file_returns_unchanged_offset() {
        let index = make_index();

        let offset = 1;
        let inode = Some(0);

        let mut new_offset;
        let new_inode;

        new_offset = read("test_files/does_not_exist.log", &index, offset, None).await;
        assert_eq!(offset, new_offset);

        (new_offset, new_inode) = check_rotation(&PathBuf::new(), offset, inode)
            .await
            .unwrap();

        assert_eq!(offset, new_offset);
        assert_eq!(inode, new_inode);
    }

    #[tokio::test]
    async fn test_check_rotation_returns_unchanged_offset_for_unchanged_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        std::fs::write(&path, "line one\nline two\n").unwrap();

        let inode = Some(std::fs::metadata(&path).unwrap().ino());
        let offset = 1;

        std::fs::write(&path, "line three\nline four\n").unwrap();
        let (new_offset, new_inode) = check_rotation(&path, offset, inode).await.unwrap();

        assert_eq!(new_offset, offset);
        assert_eq!(new_inode, inode);
    }

    #[tokio::test]
    async fn test_truncation_resets_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        std::fs::write(&path, "line one\nline two\n").unwrap();

        let inode = Some(std::fs::metadata(&path).unwrap().ino());
        let stale_offset = 100;

        let (new_offset, new_inode) = check_rotation(&path, stale_offset, inode).await.unwrap();

        assert_eq!(new_offset, 0);
        assert_eq!(new_inode, inode);
    }

    #[tokio::test]
    async fn test_changed_inode_updates_old_inode_and_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.log");
        std::fs::write(&path, "new content\n").unwrap();

        let inode = Some(std::fs::metadata(&path).unwrap().ino());
        let offset = 1;

        let other_path = dir.path().join("app.log.new");
        std::fs::write(&other_path, "rotated content\n").unwrap();
        std::fs::rename(&other_path, &path).unwrap();
        let (new_offset, new_inode) = check_rotation(&path, offset, inode).await.unwrap();

        assert_eq!(new_offset, 0);
        assert_ne!(inode, new_inode);
    }

    #[tokio::test]
    async fn test_non_json_line_error_includes_line_number_and_content() {
        let index = make_index();
        let wal = make_wal().await;
        let result = read_file(
            &PathBuf::from("test_files/non-json.log"),
            &index,
            0,
            None,
            &wal,
            None,
        )
        .await;
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("line 1"),
            "expected line number in error: {err}"
        );
        assert!(
            err.contains("Lorem"),
            "expected line content in error: {err}"
        );
    }
}
