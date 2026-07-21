use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Platform cache dir — deliberately not /tmp, which is often RAM-backed tmpfs.
pub fn default_spool_root() -> PathBuf {
    directories::ProjectDirs::from("dev", "sy", "clarify-bq")
        .map(|d| d.cache_dir().join("spool"))
        .unwrap_or_else(|| PathBuf::from(".clarify-bq-spool"))
}

pub struct RunSpool {
    dir: PathBuf,
}

impl RunSpool {
    pub fn create(root: &Path, run_id: &str) -> std::io::Result<RunSpool> {
        let dir = root.join(run_id);
        std::fs::create_dir_all(&dir)?;
        Ok(RunSpool { dir })
    }

    pub fn writer(&self, resource: &str) -> std::io::Result<SpoolWriter> {
        let path = self.dir.join(format!("{resource}.ndjson"));
        Ok(SpoolWriter {
            out: BufWriter::new(std::fs::File::create(&path)?),
            path,
            rows: 0,
        })
    }

    pub fn remove(self) -> std::io::Result<()> {
        std::fs::remove_dir_all(&self.dir)
    }
}

pub struct SpoolWriter {
    out: BufWriter<std::fs::File>,
    path: PathBuf,
    rows: u64,
}

impl SpoolWriter {
    pub fn write_row(&mut self, row: &serde_json::Value) -> std::io::Result<()> {
        serde_json::to_writer(&mut self.out, row)?;
        self.out.write_all(b"\n")?;
        self.rows += 1;
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<(PathBuf, u64)> {
        self.out.flush()?;
        Ok((self.path, self.rows))
    }
}

/// Remove leftover run directories from prior crashed runs.
pub fn sweep_orphans(root: &Path, keep_run_id: &str) -> std::io::Result<Vec<String>> {
    let mut removed = Vec::new();
    if !root.exists() {
        return Ok(removed);
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if entry.file_type()?.is_dir() && name != keep_run_id {
            std::fs::remove_dir_all(entry.path())?;
            removed.push(name);
        }
    }
    removed.sort();
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_ndjson_and_sweeps_orphans() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("old_run")).unwrap();
        let spool = RunSpool::create(root.path(), "run_new").unwrap();
        let removed = sweep_orphans(root.path(), "run_new").unwrap();
        assert_eq!(removed, vec!["old_run".to_string()]);

        let mut w = spool.writer("records_person").unwrap();
        w.write_row(&serde_json::json!({"record_id": "r1", "data": {}}))
            .unwrap();
        w.write_row(&serde_json::json!({"record_id": "r2", "data": {}}))
            .unwrap();
        let (path, rows) = w.finish().unwrap();
        assert_eq!(rows, 2);
        let text = std::fs::read_to_string(path).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(
            text.lines()
                .all(|l| serde_json::from_str::<serde_json::Value>(l).is_ok())
        );
    }
}
