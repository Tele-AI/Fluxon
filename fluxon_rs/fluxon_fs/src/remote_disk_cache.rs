use std::env;
use std::fs;
use std::io;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const REMOTE_DISK_CACHE_MIN_FILE_BYTES: u64 = 128 * 1024 * 1024;
pub const REMOTE_DISK_CACHE_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const REMOTE_DISK_CACHE_MAX_BYTES_DEFAULT: u64 = 4 * 1024 * 1024 * 1024;
pub const REMOTE_DISK_CACHE_METRICS_SOURCE: &str = "fluxon_fs_disk_cache";
pub const REMOTE_DISK_CACHE_DIRNAME: &str = "fluxon_fs_disk_cache";

const REMOTE_DISK_CACHE_ROOT_ENV: &str = "FLUXON_FS_DISK_CACHE_ROOT";
const REMOTE_DISK_CACHE_MAX_BYTES_ENV: &str = "FLUXON_FS_DISK_CACHE_MAX_BYTES";

#[derive(Debug, Default)]
struct RemoteDiskCacheCounters {
    lookup_hit_count: u64,
    lookup_miss_count: u64,
    fill_count: u64,
    fill_bytes: u64,
    evict_count: u64,
    evict_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteDiskCacheIndexMeta {
    export_name: String,
    relpath: String,
    size: u64,
    mtime_ns: u64,
    created_ns: u64,
    last_access_ns: u64,
}

#[derive(Debug)]
struct RemoteDiskCacheScannedEntry {
    key: String,
    index_path: PathBuf,
    final_path: PathBuf,
    meta: RemoteDiskCacheIndexMeta,
    size_bytes: u64,
}

#[derive(Debug)]
pub struct RemoteDiskCacheManager {
    cache_root: PathBuf,
    files_dir: PathBuf,
    index_dir: PathBuf,
    max_bytes: u64,
    counters: Mutex<RemoteDiskCacheCounters>,
}

impl RemoteDiskCacheManager {
    pub fn new(cache_root: PathBuf, max_bytes: u64) -> io::Result<Self> {
        let files_dir = cache_root.join("files");
        let index_dir = cache_root.join("index");
        fs::create_dir_all(&files_dir)?;
        fs::create_dir_all(&index_dir)?;
        let out = Self {
            cache_root,
            files_dir,
            index_dir,
            max_bytes,
            counters: Mutex::new(RemoteDiskCacheCounters::default()),
        };
        out.cleanup_stale_temp_files();
        Ok(out)
    }

    pub fn should_cache(&self, size_bytes: u64) -> bool {
        size_bytes >= REMOTE_DISK_CACHE_MIN_FILE_BYTES
    }

    pub fn lookup(
        &self,
        export_name: &str,
        relpath: &str,
        size: u64,
        mtime_ns: u64,
    ) -> io::Result<Option<PathBuf>> {
        let key = entry_key(export_name, relpath, size, mtime_ns);
        let index_path = self.index_path(&key);
        let final_path = self.final_path(&key);
        let mut counters = self.counters.lock();
        match self.load_entry(
            &key,
            &index_path,
            &final_path,
            Some((export_name, relpath, size, mtime_ns)),
        )? {
            Some(mut entry) => {
                self.touch_entry(&mut entry)?;
                counters.lookup_hit_count = counters.lookup_hit_count.saturating_add(1);
                Ok(Some(entry.final_path))
            }
            None => {
                self.invalidate_same_path_entries(export_name, relpath, Some(key.as_str()))?;
                counters.lookup_miss_count = counters.lookup_miss_count.saturating_add(1);
                Ok(None)
            }
        }
    }

    pub fn materialize<F>(
        &self,
        export_name: &str,
        relpath: &str,
        size: u64,
        mtime_ns: u64,
        fill_fn: F,
    ) -> io::Result<PathBuf>
    where
        F: FnOnce(&Path) -> io::Result<()>,
    {
        let key = entry_key(export_name, relpath, size, mtime_ns);
        let final_path = self.final_path(&key);
        let index_path = self.index_path(&key);
        if let Some(existing) = self.load_entry(
            &key,
            &index_path,
            &final_path,
            Some((export_name, relpath, size, mtime_ns)),
        )? {
            return Ok(existing.final_path);
        }

        let temp_path = self.temp_path(&key);
        let temp_index_path = temp_path.with_extension("json.tmp");
        let fill_res = fill_fn(&temp_path);
        if let Err(err) = fill_res {
            let _ = fs::remove_file(&temp_path);
            return Err(err);
        }

        let actual_size = fs::metadata(&temp_path)?.len();
        if actual_size != size {
            let _ = fs::remove_file(&temp_path);
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "incomplete large-file disk cache materialization: export={} relpath={} expected={} got={}",
                    export_name, relpath, size, actual_size
                ),
            ));
        }

        let now_ns = now_unix_ns();
        let meta = RemoteDiskCacheIndexMeta {
            export_name: export_name.to_string(),
            relpath: relpath.to_string(),
            size,
            mtime_ns,
            created_ns: now_ns,
            last_access_ns: now_ns,
        };
        write_meta(&temp_index_path, &meta)?;

        if let Some(existing) = self.load_entry(
            &key,
            &index_path,
            &final_path,
            Some((export_name, relpath, size, mtime_ns)),
        )? {
            let _ = fs::remove_file(&temp_path);
            let _ = fs::remove_file(&temp_index_path);
            return Ok(existing.final_path);
        }

        fs::rename(&temp_path, &final_path)?;
        fs::rename(&temp_index_path, &index_path)?;

        let mut counters = self.counters.lock();
        counters.fill_count = counters.fill_count.saturating_add(1);
        counters.fill_bytes = counters.fill_bytes.saturating_add(actual_size);
        self.invalidate_same_path_entries(export_name, relpath, Some(key.as_str()))?;
        self.prune_if_needed(Some(key.as_str()), &mut counters)?;
        Ok(final_path)
    }

    pub fn open_read_fd(&self, path: &Path) -> io::Result<OwnedFd> {
        let file = fs::File::open(path)?;
        Ok(unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) })
    }

    pub fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    fn cleanup_stale_temp_files(&self) {
        let Ok(entries) = fs::read_dir(&self.files_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|v| v.to_str()) != Some("part") {
                continue;
            }
            let _ = fs::remove_file(path);
        }
    }

    fn scan_entries(&self) -> io::Result<Vec<RemoteDiskCacheScannedEntry>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.index_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|v| v.to_str()) else {
                let _ = fs::remove_file(path);
                continue;
            };
            let key = stem.to_string();
            let final_path = self.final_path(&key);
            match self.load_entry(&key, &path, &final_path, None) {
                Ok(Some(item)) => out.push(item),
                Ok(None) => {}
                Err(_) => {
                    let _ = self.delete_entry_paths(&key);
                }
            }
        }
        Ok(out)
    }

    fn prune_if_needed(
        &self,
        exempt_key: Option<&str>,
        counters: &mut RemoteDiskCacheCounters,
    ) -> io::Result<()> {
        if self.max_bytes == 0 {
            return Ok(());
        }
        let mut entries = self.scan_entries()?;
        let mut total = entries
            .iter()
            .fold(0u64, |acc, item| acc.saturating_add(item.size_bytes));
        if total <= self.max_bytes {
            return Ok(());
        }
        entries.sort_by_key(|item| {
            (
                item.meta.last_access_ns,
                item.meta.created_ns,
                item.key.clone(),
            )
        });
        for entry in entries {
            if total <= self.max_bytes {
                break;
            }
            if exempt_key.is_some_and(|keep| keep == entry.key.as_str()) {
                continue;
            }
            total = total.saturating_sub(entry.size_bytes);
            let _ = self.delete_entry_paths(&entry.key);
            counters.evict_count = counters.evict_count.saturating_add(1);
            counters.evict_bytes = counters.evict_bytes.saturating_add(entry.size_bytes);
        }
        Ok(())
    }

    fn invalidate_same_path_entries(
        &self,
        export_name: &str,
        relpath: &str,
        keep_key: Option<&str>,
    ) -> io::Result<()> {
        for entry in self.scan_entries()? {
            if keep_key.is_some_and(|keep| keep == entry.key.as_str()) {
                continue;
            }
            if entry.meta.export_name == export_name && entry.meta.relpath == relpath {
                let _ = self.delete_entry_paths(&entry.key);
            }
        }
        Ok(())
    }

    fn delete_entry_paths(&self, key: &str) -> io::Result<()> {
        let final_path = self.final_path(key);
        match fs::remove_file(&final_path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let index_path = self.index_path(key);
        match fs::remove_file(&index_path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }

    fn load_entry(
        &self,
        key: &str,
        index_path: &Path,
        final_path: &Path,
        expected: Option<(&str, &str, u64, u64)>,
    ) -> io::Result<Option<RemoteDiskCacheScannedEntry>> {
        if !index_path.is_file() || !final_path.is_file() {
            let _ = self.delete_entry_paths(key);
            return Ok(None);
        }
        let meta_bytes = match fs::read(index_path) {
            Ok(bytes) => bytes,
            Err(err) => {
                let _ = self.delete_entry_paths(key);
                return Err(err);
            }
        };
        let meta: RemoteDiskCacheIndexMeta = match serde_json::from_slice(&meta_bytes) {
            Ok(v) => v,
            Err(err) => {
                let _ = self.delete_entry_paths(key);
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };
        let size_bytes = match fs::metadata(final_path) {
            Ok(v) => v.len(),
            Err(err) => {
                let _ = self.delete_entry_paths(key);
                return Err(err);
            }
        };
        if size_bytes != meta.size {
            let _ = self.delete_entry_paths(key);
            return Ok(None);
        }
        if let Some((export_name, relpath, size, mtime_ns)) = expected {
            if meta.export_name != export_name
                || meta.relpath != relpath
                || meta.size != size
                || meta.mtime_ns != mtime_ns
            {
                let _ = self.delete_entry_paths(key);
                return Ok(None);
            }
        }
        Ok(Some(RemoteDiskCacheScannedEntry {
            key: key.to_string(),
            index_path: index_path.to_path_buf(),
            final_path: final_path.to_path_buf(),
            meta,
            size_bytes,
        }))
    }

    fn touch_entry(&self, entry: &mut RemoteDiskCacheScannedEntry) -> io::Result<()> {
        let now = now_unix_ns();
        entry.meta.last_access_ns = now;
        let tmp_index_path = entry.index_path.with_extension(format!("json.{}.tmp", now));
        write_meta(&tmp_index_path, &entry.meta)?;
        fs::rename(&tmp_index_path, &entry.index_path)?;
        Ok(())
    }

    fn final_path(&self, key: &str) -> PathBuf {
        self.files_dir.join(format!("{}.bin", key))
    }

    fn index_path(&self, key: &str) -> PathBuf {
        self.index_dir.join(format!("{}.json", key))
    }

    fn temp_path(&self, key: &str) -> PathBuf {
        self.files_dir.join(format!(
            "{}.{}.{}.part",
            key,
            std::process::id(),
            now_unix_ns()
        ))
    }
}

pub fn disk_cache_max_bytes_from_env() -> u64 {
    env::var(REMOTE_DISK_CACHE_MAX_BYTES_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(REMOTE_DISK_CACHE_MAX_BYTES_DEFAULT)
}

pub fn resolve_disk_cache_root(cache_root_base: &Path, instance_key: &str) -> PathBuf {
    if let Some(raw) = env::var_os(REMOTE_DISK_CACHE_ROOT_ENV) {
        let trimmed = raw.to_string_lossy().trim().to_string();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    cache_root_base.join(safe_cache_component(instance_key))
}

fn write_meta(path: &Path, meta: &RemoteDiskCacheIndexMeta) -> io::Result<()> {
    let bytes = serde_json::to_vec(meta)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    fs::write(path, bytes)
}

fn now_unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_nanos() as u64
}

fn entry_key(export_name: &str, relpath: &str, size: u64, mtime_ns: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(export_name.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(relpath.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(size.to_string().as_bytes());
    hasher.update(b"\x1f");
    hasher.update(mtime_ns.to_string().as_bytes());
    hex::encode(hasher.finalize())[..24].to_string()
}

fn safe_cache_component(raw: &str) -> String {
    let mut slug = String::new();
    for ch in raw.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else {
            slug.push('_');
        }
    }
    while slug.starts_with('_') {
        slug.remove(0);
    }
    while slug.ends_with('_') {
        slug.pop();
    }
    let digest = {
        let mut hasher = Sha256::new();
        hasher.update(raw.as_bytes());
        hex::encode(hasher.finalize())[..12].to_string()
    };
    if slug.is_empty() {
        return digest;
    }
    let head: String = slug.chars().take(48).collect();
    format!("{}_{}", head, digest)
}
