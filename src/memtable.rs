use std::collections::HashMap;
use std::sync::RwLock;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

use crate::error::Result;
use crate::wal::Wal;
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct MemTable {
    requests: HashMap<Key, SstEntry>,
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
    // concurrency safety:
    // only one update can happen at a time due to the external rw lock on memtable
    updates_since_compaction: usize,
}

impl MemTable {
    const FLUSH_THRESHOLD: usize = 2000;
    const COMPACTION_THRESHOLD: usize = 10_000;
    const MANIFEST_DIR: &str = "data/sst";
    const MANIFEST_PATH: &str = "data/sst/manifest.txt";
    const TEMP_MANIFEST_PATH: &str = "data/sst/manifest.tmp";

    pub fn startup(&mut self) -> Result<()> {
        let manifest_path = Path::new(Self::MANIFEST_PATH);
        fs::create_dir_all(Self::MANIFEST_DIR)?;

        if !manifest_path.exists() {
            File::create(manifest_path)?;
        }

        let manifest_lines: HashSet<_> = fs::read_to_string(Self::MANIFEST_PATH)?
            .lines()
            .map(|line| line.to_string())
            .collect();

        // delete files on disk but not in manifest
        let manifest_files: HashSet<_> = fs::read_dir(Self::MANIFEST_DIR)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                let is_sst_file = path.is_file() && path.extension().unwrap_or_default() == "json";

                is_sst_file.then_some(path.to_string_lossy().to_string())
            })
            .collect();

        for file in manifest_files.difference(&manifest_lines) {
            fs::remove_file(file)?;
        }

        // load manifest cache
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
            .insert(key.clone(), SstEntry::new_put(key.clone(), value));

        self.try_flush()?;
        self.try_compact()?;
        self.negative_cache.write().unwrap().remove(&key);

        Ok(())
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        if let Some(request) = self.requests.get(key) {
            return Ok(request.value());
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

    pub fn delete(&mut self, key: &Key) -> Result<()> {
        self.wal.delete(key.clone())?;

        self.requests
            .insert(key.clone(), SstEntry::new_delete(key.clone()));

        self.try_flush()?;
        self.try_compact()?;
        self.negative_cache.write().unwrap().insert(key.clone());

        Ok(())
    }

    fn search_sst(&self, key: &Key) -> Result<Option<Value>> {
        for sst_path in self.manifest_cache.iter().rev() {
            let reader = BufReader::new(fs::File::open(sst_path)?);
            let requests: Vec<SstEntry> = serde_json::from_reader(reader)?;

            if let Some(request) = requests.iter().find(|request| *request.key() == *key) {
                return Ok(request.value());
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

        let mut requests: Vec<_> = self.requests.drain().map(|(_, request)| request).collect();
        requests.sort_by_key(|request| request.key().clone());

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

    fn try_compact(&mut self) -> Result<()> {
        self.updates_since_compaction += 1;

        if self.updates_since_compaction < Self::COMPACTION_THRESHOLD {
            return Ok(());
        }

        self.compact_sst()?;
        self.updates_since_compaction = 0;

        Ok(())
    }

    fn compact_sst(&mut self) -> Result<()> {
        if self.manifest_cache.is_empty() {
            return Ok(());
        }

        let lsm_tree: Vec<Vec<SstEntry>> = self
            .manifest_cache
            .iter()
            .map(|sst_path| -> Result<Vec<SstEntry>> {
                let reader = BufReader::new(fs::File::open(sst_path)?);
                let sst: Vec<SstEntry> = serde_json::from_reader(reader)?;

                Ok(sst)
            })
            .collect::<Result<_>>()?;

        let mut heap: BinaryHeap<_> = lsm_tree
            .iter()
            .enumerate()
            .filter_map(|(sst_file_index, requests)| {
                requests
                    .first()
                    .map(|request| HeapItem::new(request.key().clone(), sst_file_index, 0))
            })
            .collect();

        let mut new_sst_paths = Vec::new();
        let mut current_entries: Vec<SstEntry> = Vec::new();
        let mut next_id = self.next_sst_id()?;

        while let Some(item) = heap.pop() {
            let key = item.key.clone();
            let mut drained = vec![item];

            while let Some(top) = heap.peek()
                && top.key == key
            {
                drained.push(heap.pop().expect("heap should have at least one item"));
            }

            let newest_entry = &lsm_tree[drained[0].sst_file_index][drained[0].sst_position_index];

            // keep newest non-delete entry
            if !newest_entry.is_delete() {
                current_entries.push(newest_entry.clone());

                if current_entries.len() >= Self::FLUSH_THRESHOLD {
                    let sst_path = Self::sst_path(next_id);
                    next_id += 1;

                    self.write_sst(&sst_path, &current_entries)?;

                    new_sst_paths.push(sst_path);
                    current_entries.clear();
                }
            }

            for HeapItem {
                key: _,
                sst_file_index,
                sst_position_index,
            } in drained
            {
                let next_index = sst_position_index + 1;

                if let Some(next) = lsm_tree[sst_file_index].get(next_index) {
                    let item = HeapItem::new(next.key().clone(), sst_file_index, next_index);

                    heap.push(item);
                }
            }
        }

        if !current_entries.is_empty() {
            let sst_path = Self::sst_path(next_id);
            self.write_sst(&sst_path, &current_entries)?;
            new_sst_paths.push(sst_path);
        }

        self.update_manifest(&new_sst_paths)?;

        for old_sst_file in &self.manifest_cache {
            fs::remove_file(old_sst_file)?;
        }

        self.manifest_cache = new_sst_paths;

        Ok(())
    }

    fn sst_path(id: usize) -> String {
        format!("data/sst/sst-{id}.json")
    }

    fn write_sst(&self, sst_path: &str, entries: &[SstEntry]) -> Result<()> {
        let mut sst_file = File::create(sst_path)?;

        // write file
        serde_json::to_writer(&sst_file, entries)?;
        sst_file.flush()?;
        sst_file.sync_all()?;

        // sync directory
        let sst_dir = OpenOptions::new().read(true).open(Self::MANIFEST_DIR)?;
        sst_dir.sync_all()?;

        Ok(())
    }

    fn update_manifest(&self, new_sst_paths: &[String]) -> Result<()> {
        let mut temp_manifest_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(Self::TEMP_MANIFEST_PATH)?;

        // write file
        if !new_sst_paths.is_empty() {
            let manifest_lines = new_sst_paths.join("\n");
            write!(temp_manifest_file, "{manifest_lines}")?;
        }

        temp_manifest_file.flush()?;
        temp_manifest_file.sync_all()?;

        // atomic update
        fs::rename(Self::TEMP_MANIFEST_PATH, Self::MANIFEST_PATH)?;

        // sync directory
        let sst_dir = OpenOptions::new().read(true).open(Self::MANIFEST_DIR)?;
        sst_dir.sync_all()?;

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

#[derive(Clone)]
pub enum SstEntry {
    Put(PutEntry),
    Delete(DeleteEntry),
}

impl Serialize for SstEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Put(entry) => entry.serialize(serializer),
            Self::Delete(entry) => entry.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SstEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawEntry {
            key: Key,
            value: Option<Value>,
            deleted: Option<bool>,
        }

        let entry = RawEntry::deserialize(deserializer)?;

        if let Some(value) = entry.value {
            return Ok(SstEntry::new_put(entry.key, value));
        }

        if let Some(deleted) = entry.deleted
            && deleted
        {
            return Ok(SstEntry::new_delete(entry.key));
        }

        Err(serde::de::Error::custom(
            "entry must have either value or deleted: true",
        ))
    }
}

impl SstEntry {
    pub fn new_put(key: Key, value: Value) -> Self {
        Self::Put(PutEntry { key, value })
    }

    pub fn new_delete(key: Key) -> Self {
        Self::Delete(DeleteEntry { key, deleted: true })
    }

    pub fn key(&self) -> &Key {
        match self {
            Self::Put(entry) => &entry.key,
            Self::Delete(entry) => &entry.key,
        }
    }

    pub fn value(&self) -> Option<Value> {
        match self {
            Self::Put(entry) => Some(entry.value),
            Self::Delete(_) => None,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete(_))
    }
}

#[derive(Clone, Serialize)]
pub struct PutEntry {
    key: Key,
    value: Value,
}

#[derive(Clone, Serialize)]
pub struct DeleteEntry {
    key: Key,
    deleted: bool,
}

#[derive(Eq, PartialEq)]
struct HeapItem {
    key: Key,
    sst_file_index: usize,
    sst_position_index: usize,
}

impl HeapItem {
    fn new(key: Key, sst_file_index: usize, sst_position_index: usize) -> Self {
        Self {
            key,
            sst_file_index,
            sst_position_index,
        }
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => self.sst_file_index.cmp(&other.sst_file_index),
            ordering => ordering.reverse(),
        }
        .then_with(|| self.sst_position_index.cmp(&other.sst_position_index))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
