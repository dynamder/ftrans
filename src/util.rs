use std::path::{Path, PathBuf};

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.1} {}", size, UNITS[unit])
    }
}

/// Pick a directory on the same drive as the given paths for the temp store.
///
/// The system temp dir (usually the C: drive) is often too small for large
/// transfers, so we prefer the drive the data lives on. Falls back to the
/// system temp dir if no path can be resolved.
pub fn default_store_parent(paths: &[PathBuf]) -> PathBuf {
    for p in paths {
        if let Ok(abs) = std::path::absolute(p) {
            // The last ancestor of an absolute path is the drive root (e.g.
            // `C:\` or `\\server\share\`). components().next() would only give
            // the bare prefix like `C:` which is not an absolute root.
            if let Some(root) = abs.ancestors().last() {
                if root.exists() {
                    return root.to_path_buf();
                }
            }
        }
    }
    std::env::temp_dir()
}

/// Create a fresh, empty store directory `name` under `parent`.
///
/// Any previous directory with the same name is removed first.
pub async fn prepare_store_dir(parent: &Path, name: &str) -> anyhow::Result<PathBuf> {
    let dir = parent.join(name);
    if dir.exists() {
        tokio::fs::remove_dir_all(&dir).await?;
    }
    tokio::fs::create_dir_all(&dir).await?;
    Ok(dir)
}

/// Verify `dir` is writable; if not (e.g. a system drive root), fall back to
/// the system temp dir and print a warning.
pub async fn ensure_writable_store_parent(dir: &Path) -> PathBuf {
    let probe = dir.join(".ftrans-write-test");
    match tokio::fs::create_dir(&probe).await {
        Ok(_) => {
            let _ = tokio::fs::remove_dir(&probe).await;
            dir.to_path_buf()
        }
        Err(_) => {
            eprintln!(
                "warning: {} is not writable, using {} for the temp store instead",
                dir.display(),
                std::env::temp_dir().display(),
            );
            std::env::temp_dir()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_store_parent_uses_data_drive() {
        // A path on the same drive as the system temp dir resolves to that
        // drive's absolute root (e.g. C:\ or D:\), not the temp dir itself.
        let tmp = std::env::temp_dir();
        let probe = tmp.join("ftrans-nonexistent-probe.bin");
        let parent = default_store_parent(&[probe]);
        assert_eq!(parent, tmp.ancestors().last().unwrap());
    }

    #[test]
    fn default_store_parent_falls_back_to_temp_dir() {
        assert_eq!(default_store_parent(&[]), std::env::temp_dir());
    }
}

