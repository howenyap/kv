use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

use crate::{
    error::Result,
    memtable::{Key, PutRequest, Value},
};

#[derive(Default)]
pub struct Wal;

impl Wal {
    const WAL_PATH: &str = "data/wal/wal.db";

    pub fn startup(&self) -> Result<HashMap<Key, PutRequest>> {
        let wal_path = Path::new(Self::WAL_PATH);

        if let Some(parent) = wal_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !wal_path.exists() {
            File::create(wal_path)?;
        }

        let wal_file = File::open(Self::WAL_PATH)?;
        let reader = BufReader::new(wal_file);

        let map: HashMap<_, _> = reader
            .lines()
            .map(|line| {
                let line = line?;
                let Entry { key, value, .. } = serde_json::from_str(&line)?;

                Ok((key.clone(), PutRequest::new(key, value)))
            })
            .collect::<Result<_>>()?;

        Ok(map)
    }

    pub fn put(&self, key: Key, value: Value) -> Result<()> {
        let mut wal_file = OpenOptions::new().append(true).open(Self::WAL_PATH)?;

        let entry = Entry::put(key, value);
        let serialised = serde_json::to_string(&entry)?;

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
enum Operation {
    Put,
}

#[derive(Serialize, Deserialize)]
struct Entry {
    op: Operation,
    key: Key,
    value: Value,
}

impl Entry {
    pub fn put(key: Key, value: Value) -> Self {
        Self {
            op: Operation::Put,
            key,
            value,
        }
    }
}
