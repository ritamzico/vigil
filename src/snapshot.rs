use std::path::Path;

use tokio::{
    fs::{rename, File},
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};
use wincode::{deserialize, serialize, SchemaRead, SchemaWrite};

use crate::event::PersistedEvent;

const TEMP_FILE_NAME: &str = "snapshot.tmp";
const BIN_FILE_NAME: &str = "snapshot.bin";

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct Snapshot {
    schema_version: u32,
    time_field: Option<String>,
    next_seq: u64,
    byte_offset: u64,
    events: Vec<PersistedEvent>,
}

impl Snapshot {
    pub fn new(
        schema_version: u32,
        time_field: Option<String>,
        byte_offset: u64,
        next_seq: u64,
        events: Vec<PersistedEvent>,
    ) -> Self {
        Snapshot {
            schema_version,
            time_field,
            byte_offset,
            next_seq,
            events,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub fn byte_offset(&self) -> u64 {
        self.byte_offset
    }

    pub fn into_events(self) -> Vec<PersistedEvent> {
        self.events
    }

    pub async fn save(&self, dir: &Path) -> io::Result<()> {
        let temp_path = dir.join(TEMP_FILE_NAME);
        let bin_path = dir.join(BIN_FILE_NAME);
        let temp_file = File::create(&temp_path).await?;

        let mut writer = BufWriter::new(temp_file);

        let payload = serialize(self).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer.write_all(&payload).await?;

        writer.flush().await?;
        writer.get_ref().sync_data().await?;

        rename(&temp_path, &bin_path).await?;

        Ok(())
    }

    pub async fn load(dir: &Path) -> io::Result<Option<Snapshot>> {
        let bin_path = dir.join(BIN_FILE_NAME);

        let file = match File::open(&bin_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut reader = BufReader::new(file);
        let mut payload = vec![];
        reader.read_to_end(&mut payload).await?;

        let snapshot =
            deserialize(&payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn make_snapshot(events: Vec<PersistedEvent>) -> Snapshot {
        Snapshot::new(1, Some("timestamp".to_string()), 512, 42, events)
    }

    fn make_event(raw: &str) -> PersistedEvent {
        PersistedEvent::new(Some(1000), raw.to_string(), HashMap::new())
    }

    #[tokio::test]
    async fn test_load_returns_none_when_no_file() {
        let dir = tempdir().unwrap();
        let result = Snapshot::load(dir.path()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_roundtrip() {
        let dir = tempdir().unwrap();
        let snapshot = make_snapshot(vec![make_event("log line 1"), make_event("log line 2")]);

        snapshot.save(dir.path()).await.unwrap();

        let loaded = Snapshot::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn test_save_and_load_empty_events() {
        let dir = tempdir().unwrap();
        let snapshot = make_snapshot(vec![]);

        snapshot.save(dir.path()).await.unwrap();

        let loaded = Snapshot::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn test_save_overwrites_previous_snapshot() {
        let dir = tempdir().unwrap();

        let first = make_snapshot(vec![make_event("first")]);
        first.save(dir.path()).await.unwrap();

        let second = Snapshot::new(1, None, 0, 99, vec![make_event("second")]);
        second.save(dir.path()).await.unwrap();

        let loaded = Snapshot::load(dir.path()).await.unwrap().unwrap();
        assert_eq!(loaded, second);
    }
}
