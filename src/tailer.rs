use crate::event::Event;
use crate::event::PersistedEvent;
use crate::index::Index;
use crate::value::Value;
use crate::wal::WAL;
use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::fs::File;
use tokio::io::{self, AsyncBufReadExt, AsyncSeekExt};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

const SLEEP_TIME: u64 = 100;

pub async fn run_tailer(
    file_path: PathBuf,
    index: Arc<RwLock<Index>>,
    time_field: Option<String>,
    wal: Option<Arc<Mutex<WAL>>>,
    starting_byte_offset: u64,
) -> Result<(), io::Error> {
    let mut byte_offset = starting_byte_offset;

    loop {
        byte_offset =
            read_file(&file_path, &index, byte_offset, time_field.as_deref(), &wal).await?;
        sleep(Duration::from_millis(SLEEP_TIME)).await;
    }
}

async fn read_file(
    file_path: &PathBuf,
    index: &Arc<RwLock<Index>>,
    mut byte_offset: u64,
    time_field: Option<&str>,
    wal: &Option<Arc<Mutex<WAL>>>,
) -> Result<u64, io::Error> {
    let file = File::open(file_path).await?;
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
        Arc::new(RwLock::new(Index::new()))
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
        read_file(&PathBuf::from(path), index, offset, time_field, &wal)
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
    async fn test_nonexistent_file_returns_error() {
        let index = make_index();
        let wal = make_wal().await;
        let result = read_file(
            &PathBuf::from("test_files/does_not_exist.log"),
            &index,
            0,
            None,
            &wal,
        )
        .await;
        assert!(result.is_err());
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
