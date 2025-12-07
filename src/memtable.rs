use std::sync::RwLock;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
};

use crate::error::Result;
use serde::{Deserialize, Serialize};

pub type Key = String;
pub type Value = u32;

#[derive(Serialize, Deserialize)]
struct PutRequest {
    key: Key,
    value: Value,
}

#[derive(Default)]
pub struct MemTable {
    requests: HashMap<Key, PutRequest>,
    negative_cache: RwLock<HashSet<Key>>,
    manifest_cache: RwLock<Vec<String>>,
}

impl MemTable {
    const FLUSH_THRESHOLD: usize = 2000;
    const MANIFEST_PATH: &str = "data/manifest.txt";

    pub fn startup(&self) -> Result<()> {
        let manifest_path = Path::new(Self::MANIFEST_PATH);

        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !manifest_path.exists() {
            File::create(manifest_path)?;
        }

        Ok(())
    }

    pub fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.negative_cache.write().unwrap().remove(&key);

        self.requests
            .entry(key.clone())
            .and_modify(|request| request.value = value)
            .or_insert(PutRequest { key, value });

        self.try_flush()
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
        for sst_path in self.manifest()?.iter().rev() {
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

        let path = format!("data/sst-{}.json", self.next_sst_id()?);
        let sst_file = File::create(&path)?;

        let mut requests: Vec<_> = self.requests.drain().map(|(_, request)| request).collect();
        requests.sort_by_key(|request| request.key.clone());

        serde_json::to_writer(sst_file, &requests)?;

        let mut manifest = OpenOptions::new().append(true).open(Self::MANIFEST_PATH)?;
        writeln!(manifest, "{path}")?;

        self.manifest_cache.write().unwrap().push(path);

        Ok(())
    }

    fn next_sst_id(&self) -> Result<usize> {
        let manifest = self.manifest()?;
        let last_path = manifest.last();
        let last_id = last_path
            .and_then(|line| {
                line.trim_start_matches("data/sst-")
                    .trim_end_matches(".json")
                    .parse()
                    .ok()
            })
            .unwrap_or(0);

        Ok(last_id + 1)
    }

    // todo: fix potential race condition as manifest file and cache access is not synchronised
    fn manifest(&self) -> Result<Vec<String>> {
        {
            let manifest = self.manifest_cache.read().unwrap();

            if !manifest.is_empty() {
                return Ok(manifest.clone());
            }
        }

        {
            let reader = Self::manifest_reader()?;
            let manifest: Vec<_> = reader.lines().map_while(std::result::Result::ok).collect();
            self.manifest_cache
                .write()
                .unwrap()
                .extend(manifest.clone());

            return Ok(manifest.clone());
        }
    }

    fn manifest_reader() -> Result<BufReader<File>> {
        let file = OpenOptions::new().read(true).open(Self::MANIFEST_PATH)?;

        Ok(BufReader::new(file))
    }
}
