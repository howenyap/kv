use std::sync::RwLock;
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

use crate::error::Result;
use crate::wal::Wal;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct MemTable {
    requests: DashMap<Key, PutRequest>,
    // concurrency safety:
    // only put requests mutate wal/manifest_cache,
    // and only one put request (writer) can exist at a time due to the external rw lock on memtable
    wal: Wal,
    manifest_cache: Vec<String>,
    // concurrency safety:
    // only get requests mutate negative_cache,
    // more than one get request (readers) can exist at a time due to the external rw lock on memtable
    // so a separate lock is needed here
    negative_cache: RwLock<HashSet<Key>>,
}

impl MemTable {
    const FLUSH_THRESHOLD: usize = 2000;
    const MANIFEST_PATH: &str = "data/sst/manifest.txt";
    const TEMP_MANIFEST_PATH: &str = "data/sst/manifest.tmp";

    pub fn startup(&mut self) -> Result<()> {
        let manifest_path = Path::new(Self::MANIFEST_PATH);

        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !manifest_path.exists() {
            File::create(manifest_path)?;
        }

        // load manifest cache
        let manifest_lines: Vec<_> = fs::read_to_string(Self::MANIFEST_PATH)?
            .lines()
            .map(|line| line.to_string())
            .collect();
        self.manifest_cache.extend(manifest_lines);

        // replay wal
        self.wal.startup()?;
        let wal_entries = self.wal.existing_entries()?;
        self.requests.extend(wal_entries);

        Ok(())
    }

    pub fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.wal.put(key.clone(), value)?;

        self.requests
            .entry(key.clone())
            .and_modify(|request| request.value = value)
            .or_insert(PutRequest::new(key.clone(), value));

        self.try_flush()?;
        self.negative_cache.write().unwrap().remove(&key);

        Ok(())
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        if let Some(request) = self.requests.get(key) {
            return Ok(Some(request.value));
        }

        if self.search_negative_cache(key) {
            return Ok(None);
        }

        let result = self.search_sst(key);
        match result {
            Ok(None) => {
                self.negative_cache.write().unwrap().insert(key.clone());

                Ok(None)
            }
            other => other,
        }
    }

    fn search_sst(&self, key: &Key) -> Result<Option<Value>> {
        for sst_path in self.manifest_cache.iter().rev() {
            let reader = BufReader::new(fs::File::open(sst_path)?);
            let requests: Vec<PutRequest> = serde_json::from_reader(reader)?;

            if let Some(request) = requests.iter().find(|request| request.key == *key) {
                return Ok(Some(request.value));
            }
        }

        Ok(None)
    }

    fn search_negative_cache(&self, key: &Key) -> bool {
        self.negative_cache.read().unwrap().contains(key)
    }

    fn try_flush(&mut self) -> Result<()> {
        if self.requests.len() < Self::FLUSH_THRESHOLD {
            return Ok(());
        }

        let sst_path = format!("data/sst/sst-{}.json", self.next_sst_id()?);
        let mut sst_file = File::create(&sst_path)?;

        let mut requests: Vec<_> = std::mem::take(&mut self.requests)
            .into_iter()
            .map(|(_, request)| request)
            .collect();
        requests.sort_by_key(|request| request.key.clone());

        serde_json::to_writer(&sst_file, &requests)?;
        sst_file.flush()?;
        sst_file.sync_all()?;

        let mut temp_manifest_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(Self::TEMP_MANIFEST_PATH)?;

        let manifest_lines = fs::read_to_string(Self::MANIFEST_PATH)?;
        if manifest_lines.is_empty() {
            write!(temp_manifest_file, "{sst_path}")?;
        } else {
            write!(temp_manifest_file, "{manifest_lines}\n{sst_path}")?;
        }
        temp_manifest_file.flush()?;
        temp_manifest_file.sync_all()?;

        fs::rename(Self::TEMP_MANIFEST_PATH, Self::MANIFEST_PATH)?;

        let sst_dir = OpenOptions::new().read(true).open("data/sst")?;
        sst_dir.sync_all()?;

        self.wal.reset()?;

        self.manifest_cache.push(sst_path);

        Ok(())
    }

    fn next_sst_id(&self) -> Result<usize> {
        let last_id = self
            .manifest_cache
            .last()
            .and_then(|line| {
                line.trim_start_matches("data/sst/sst-")
                    .trim_end_matches(".json")
                    .parse()
                    .ok()
            })
            .unwrap_or(0);

        Ok(last_id + 1)
    }
}

pub type Key = String;
pub type Value = u32;

#[derive(Serialize, Deserialize)]
pub struct PutRequest {
    key: Key,
    value: Value,
}

impl PutRequest {
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value }
    }
}
