use std::path::Path;

use tokio::{
    fs::File,
    fs::OpenOptions,
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter},
};
use wincode::{serialize, SchemaRead, SchemaWrite};

use crate::event::PersistedEvent;

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

pub struct WAL {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    pub next_seq: u64,
    pub last_byte_offset: u64,
}

impl WAL {
    pub async fn open(path: &Path, starting_seq: Option<u64>) -> io::Result<Self> {
        let writer_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let reader_file = File::open(path).await?;
        Ok(WAL {
            reader: BufReader::new(reader_file),
            writer: BufWriter::new(writer_file),
            next_seq: starting_seq.unwrap_or(0),
            last_byte_offset: 0,
        })
    }

    pub fn set_next_seq(&mut self, next_seq: u64) {
        self.next_seq = next_seq;
    }

    pub fn set_last_byte_offset(&mut self, last_byte_offset: u64) {
        self.last_byte_offset = last_byte_offset;
    }

    pub async fn append(&mut self, byte_offset: u64, event: PersistedEvent) -> io::Result<()> {
        let record = &WALRecord::new(self.next_seq, byte_offset, event);

        let payload =
            serialize(record).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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

    pub async fn replay(&mut self) -> io::Result<Vec<WALRecord>> {
        let mut records = vec![];

        loop {
            let mut len_bytes = [0u8; 4];
            match self.reader.read_exact(&mut len_bytes).await {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
                _ => {}
            }
            let payload_len = u32::from_le_bytes(len_bytes);

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
                return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
            }

            let record = wincode::deserialize(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            records.push(record);
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

    async fn open_temp_wal() -> (WAL, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let wal = WAL::open(tmp.path(), None).await.unwrap();
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
    async fn test_crc_corruption_returns_error() {
        let tmp = NamedTempFile::new().unwrap();
        {
            let mut wal = WAL::open(tmp.path(), None).await.unwrap();
            wal.append(0, make_event("data")).await.unwrap();
            wal.flush_fsync().await.unwrap();
        }

        // Flip a byte in the middle of the file to corrupt the payload
        let mut bytes = tokio::fs::read(tmp.path()).await.unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        tokio::fs::write(tmp.path(), &bytes).await.unwrap();

        let mut wal = WAL::open(tmp.path(), None).await.unwrap();
        let result = wal.replay().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_with_starting_seq_resumes_numbering() {
        let tmp = NamedTempFile::new().unwrap();
        let mut wal = WAL::open(tmp.path(), Some(10)).await.unwrap();

        wal.append(0, make_event("resumed")).await.unwrap();
        wal.flush_fsync().await.unwrap();

        let records = wal.replay().await.unwrap();
        assert_eq!(records[0].seq, 10);
    }
}
