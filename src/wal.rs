use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

use crate::{
    error::{KvError, Result},
    memtable::{Key, SstEntry, Value},
};

#[derive(Default)]
pub struct Wal;

impl Wal {
    const WAL_PATH: &str = "data/wal/wal.db";

    pub fn startup(&self) -> Result<()> {
        let wal_path = Path::new(Self::WAL_PATH);

        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !wal_path.exists() {
            File::create(wal_path)?;
        }

        Ok(())
    }

    pub fn existing_entries(&self) -> Result<HashMap<Key, SstEntry>> {
        let wal_file = File::open(Self::WAL_PATH)?;
        let reader = BufReader::new(wal_file);
        let mut entries: HashMap<Key, SstEntry> = HashMap::new();

        for line in reader.lines() {
            let line = line?;

            if line.trim().is_empty() {
                continue;
            }

            let Ok(entry) = Log::validate(&line) else {
                break;
            };

            match entry {
                Entry::Put { key, value } => {
                    entries.insert(key.clone(), SstEntry::new_put(key, value));
                }
                Entry::Delete { key } => {
                    entries.insert(key.clone(), SstEntry::new_delete(key));
                }
            }
        }

        Ok(entries)
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let mut wal_file = OpenOptions::new().append(true).open(Self::WAL_PATH)?;

        let entry = Entry::put(key, value);
        let log = Log::new(entry)?;
        let serialised = serde_json::to_string(&log)?;

        writeln!(wal_file, "{serialised}")?;
        wal_file.flush()?;
        wal_file.sync_all()?;

        Ok(())
    }

    pub fn delete(&self, key: Key) -> Result<()> {
        let mut wal_file = OpenOptions::new().append(true).open(Self::WAL_PATH)?;

        let entry = Entry::delete(key);
        let log = Log::new(entry)?;
        let serialised = serde_json::to_string(&log)?;

        writeln!(wal_file, "{serialised}")?;
        wal_file.flush()?;
        wal_file.sync_all()?;

        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        let mut wal_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(Self::WAL_PATH)?;
        wal_file.flush()?;
        wal_file.sync_all()?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Log {
    hash: u32,
    entry: Entry,
}

impl Log {
    pub fn new(entry: Entry) -> Result<Self> {
        let serialised = serde_json::to_string(&entry)?;
        let bytes = serialised.as_bytes();
        let hash = crc32fast::hash(bytes);

        Ok(Self { hash, entry })
    }

    pub fn validate(string: &str) -> Result<Entry> {
        let string = string.trim_end();

        let Log {
            hash: expected_hash,
            entry,
        } = serde_json::from_str(string)?;
        let entry_str = serde_json::to_string(&entry)?;
        let entry_bytes = entry_str.as_bytes();

        let hash = crc32fast::hash(entry_bytes);

        if expected_hash != hash {
            return Err(KvError::InvalidChecksum);
        }

        Ok(entry)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Entry {
    Put { key: Key, value: Value },
    Delete { key: Key },
}

impl Entry {
    pub fn put(key: Key, value: Value) -> Self {
        Self::Put { key, value }
    }

    pub fn delete(key: Key) -> Self {
        Self::Delete { key }
    }
}
