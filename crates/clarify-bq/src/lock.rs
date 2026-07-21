use fs2::FileExt;
use std::fs::File;
use std::path::Path;

/// Exclusive per-host run lock. `None` means another run holds it (exit 5).
/// Released on drop.
pub struct RunLock(#[allow(dead_code)] File);

impl RunLock {
    pub fn acquire(dir: &Path) -> std::io::Result<Option<RunLock>> {
        std::fs::create_dir_all(dir)?;
        let file = File::create(dir.join("clarify-bq.lock"))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(RunLock(file))),
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_fails_while_held_and_succeeds_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let l1 = RunLock::acquire(dir.path()).unwrap();
        assert!(l1.is_some());
        assert!(RunLock::acquire(dir.path()).unwrap().is_none());
        drop(l1);
        assert!(RunLock::acquire(dir.path()).unwrap().is_some());
    }
}
