use std::path::Path;

use tokio::{
    fs::File,
    fs::OpenOptions,
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};
use wincode::{
    config::{deserialize, serialize, Configuration},
    SchemaRead, SchemaWrite,
};

use crate::event::PersistedEvent;

// Payloads are CRC-verified before deserialization, so a corrupt length can't
// reach the decoder — safe to lift wincode's 4 MiB preallocation cap, which a
// single legitimate large record (e.g. a multi-MiB log line) would exceed.
fn wal_config() -> Configuration<true, { usize::MAX }> {
    Configuration::default().disable_preallocation_size_limit()
}

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct WALRecord {
    pub seq: u64,
    pub byte_offset: u64,
    pub event: PersistedEvent,
}

impl WALRecord {
    pub fn new(seq: u64, byte_offset: u64, event: PersistedEvent) -> Self {
        WALRecord {
            seq,
            byte_offset,
            event,
        }
    }
}

pub struct Wal {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    pub next_seq: u64,
    pub last_byte_offset: u64,
}

impl Wal {
    pub async fn open(path: &Path, starting_seq: Option<u64>) -> io::Result<Self> {
        let writer_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let reader_file = File::open(path).await?;
        Ok(Wal {
            reader: BufReader::new(reader_file),
            writer: BufWriter::new(writer_file),
            next_seq: starting_seq.unwrap_or(0),
            last_byte_offset: 0,
        })
    }

    pub async fn append(&mut self, byte_offset: u64, event: PersistedEvent) -> io::Result<()> {
        let record = &WALRecord::new(self.next_seq, byte_offset, event);

        let payload = serialize(record, wal_config())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let crc = crc32fast::hash(&payload);

        self.writer
            .write_all(&(payload.len() as u32).to_le_bytes())
            .await?;
        self.writer.write_all(&payload).await?;
        self.writer.write_all(&crc.to_le_bytes()).await?;

        self.next_seq += 1;
        self.last_byte_offset = byte_offset;

        Ok(())
    }

    pub async fn flush_fsync(&mut self) -> io::Result<()> {
        self.writer.flush().await?;
        self.writer.get_ref().sync_data().await?;

        Ok(())
    }

    /// Read back every intact record from the start of the log.
    ///
    /// A crash can tear the last record (short write, or blocks that never hit
    /// disk before fsync). Anything after the last intact record is dropped
    /// and the file is truncated back to the intact prefix, so future appends
    /// land where the next replay will actually read them.
    pub async fn replay(&mut self) -> io::Result<Vec<WALRecord>> {
        let mut records = vec![];

        self.writer.flush().await?;
        let file_len = self.writer.get_ref().metadata().await?.len();
        self.reader.seek(io::SeekFrom::Start(0)).await?;

        // Byte offset of the end of the last intact record.
        let mut valid_len: u64 = 0;

        loop {
            let mut len_bytes = [0u8; 4];
            match self.reader.read_exact(&mut len_bytes).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
                _ => {}
            }
            let payload_len = u32::from_le_bytes(len_bytes);

            // A record that claims to extend past the end of the file is a
            // torn tail. Checking up front also stops a corrupt length from
            // preallocating gigabytes below.
            if valid_len + 4 + payload_len as u64 + 4 > file_len {
                break;
            }

            let mut payload = vec![0u8; payload_len as usize];
            match self.reader.read_exact(&mut payload).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
                _ => {}
            }

            let mut crc_bytes = [0u8; 4];
            match self.reader.read_exact(&mut crc_bytes).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
                _ => {}
            }
            let expected_crc = u32::from_le_bytes(crc_bytes);
            let actual_crc = crc32fast::hash(&payload);
            if expected_crc != actual_crc {
                break;
            }

            let record = match deserialize(&payload, wal_config()) {
                Ok(record) => record,
                Err(_) => break,
            };
            records.push(record);
            valid_len += 4 + payload.len() as u64 + 4;
        }

        if file_len > valid_len {
            eprintln!(
                "WAL: dropping {} bytes of torn or corrupt data after the last intact record",
                file_len - valid_len
            );
            self.writer.get_ref().set_len(valid_len).await?;
            self.writer.get_ref().sync_data().await?;
            self.reader.seek(io::SeekFrom::Start(valid_len)).await?;
        }

        Ok(records)
    }

    pub async fn truncate(&mut self) -> io::Result<()> {
        self.writer.flush().await?;
        self.writer.get_ref().set_len(0).await?;
        self.writer.seek(io::SeekFrom::Start(0)).await?;
        self.reader.seek(io::SeekFrom::Start(0)).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn make_event(raw: &str) -> PersistedEvent {
        PersistedEvent::new(None, raw.to_string(), HashMap::new())
    }

    async fn open_temp_wal() -> (Wal, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let wal = Wal::open(tmp.path(), None).await.unwrap();
        (wal, tmp)
    }

    #[tokio::test]
    async fn test_append_and_replay_single_record() {
        let (mut wal, _tmp) = open_temp_wal().await;

        wal.append(0, make_event("hello")).await.unwrap();
        wal.flush_fsync().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq, 0);
        assert_eq!(records[0].byte_offset, 0);
        assert_eq!(records[0].event, make_event("hello"));
    }

    #[tokio::test]
    async fn test_append_and_replay_multiple_records() {
        let (mut wal, _tmp) = open_temp_wal().await;

        for i in 0..5u64 {
            wal.append(i * 100, make_event(&format!("event-{i}")))
                .await
                .unwrap();
        }
        wal.flush_fsync().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 5);
        for i in 0..5u64 {
            assert_eq!(records[i as usize].seq, i);
            assert_eq!(records[i as usize].byte_offset, i * 100);
        }
    }

    #[tokio::test]
    async fn test_replay_empty_wal_returns_empty() {
        let (mut wal, _tmp) = open_temp_wal().await;
        let records = wal.replay().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_truncate_clears_all_records() {
        let (mut wal, _tmp) = open_temp_wal().await;

        wal.append(0, make_event("before truncate")).await.unwrap();
        wal.flush_fsync().await.unwrap();
        wal.truncate().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_crc_corruption_drops_corrupt_record_and_truncates() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut wal = Wal::open(tmp.path(), None).await.unwrap();
            wal.append(0, make_event("data")).await.unwrap();
            wal.flush_fsync().await.unwrap();
        }

        // Flip a byte in the middle of the file to corrupt the payload
        let mut bytes = tokio::fs::read(tmp.path()).await.unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        tokio::fs::write(tmp.path(), &bytes).await.unwrap();

        let mut wal = Wal::open(tmp.path(), None).await.unwrap();
        let records = wal.replay().await.unwrap();
        assert!(records.is_empty());

        // The corrupt tail is gone from disk, so future appends stay readable.
        let len = tokio::fs::metadata(tmp.path()).await.unwrap().len();
        assert_eq!(len, 0);
    }

    #[tokio::test]
    async fn test_torn_tail_keeps_intact_prefix() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut wal = Wal::open(tmp.path(), None).await.unwrap();
            wal.append(0, make_event("good-0")).await.unwrap();
            wal.append(10, make_event("good-1")).await.unwrap();
            wal.flush_fsync().await.unwrap();
        }
        let intact_len = tokio::fs::metadata(tmp.path()).await.unwrap().len();

        // Simulate a crash mid-append: a length prefix that claims more bytes
        // than the file holds, followed by a short payload.
        let mut bytes = tokio::fs::read(tmp.path()).await.unwrap();
        bytes.extend_from_slice(&1000u32.to_le_bytes());
        bytes.extend_from_slice(b"partial");
        tokio::fs::write(tmp.path(), &bytes).await.unwrap();

        let mut wal = Wal::open(tmp.path(), None).await.unwrap();
        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].event, make_event("good-0"));
        assert_eq!(records[1].event, make_event("good-1"));

        // Torn bytes were truncated away.
        let len = tokio::fs::metadata(tmp.path()).await.unwrap().len();
        assert_eq!(len, intact_len);
    }

    #[tokio::test]
    async fn test_appends_after_torn_tail_recovery_survive_next_replay() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut wal = Wal::open(tmp.path(), None).await.unwrap();
            wal.append(0, make_event("good")).await.unwrap();
            wal.flush_fsync().await.unwrap();
        }

        // Torn tail: garbage length prefix + short payload.
        let mut bytes = tokio::fs::read(tmp.path()).await.unwrap();
        bytes.extend_from_slice(&(u32::MAX).to_le_bytes());
        bytes.extend_from_slice(b"xx");
        tokio::fs::write(tmp.path(), &bytes).await.unwrap();

        // First recovery drops the torn tail, then the daemon keeps appending.
        let mut wal = Wal::open(tmp.path(), Some(1)).await.unwrap();
        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 1);
        wal.append(20, make_event("after-recovery")).await.unwrap();
        wal.flush_fsync().await.unwrap();

        // A later recovery must see both records — nothing written after the
        // truncation may be lost.
        let mut wal = Wal::open(tmp.path(), None).await.unwrap();
        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].event, make_event("after-recovery"));
    }

    #[tokio::test]
    async fn test_large_record_exceeding_prealloc_cap_replays() {
        let (mut wal, _tmp) = open_temp_wal().await;

        // A single record bigger than wincode's default 4 MiB prealloc cap.
        let big = "x".repeat(5 * 1024 * 1024);
        wal.append(0, make_event(&big)).await.unwrap();
        wal.flush_fsync().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event, make_event(&big));
    }

    #[tokio::test]
    async fn test_open_with_starting_seq_resumes_numbering() {
        let tmp = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(tmp.path(), Some(10)).await.unwrap();

        wal.append(0, make_event("resumed")).await.unwrap();
        wal.flush_fsync().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records[0].seq, 10);
    }
}
