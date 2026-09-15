use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint,
};
use iroh_blobs::{
    api::blobs::AddPathOptions,
    api::proto::ImportMode,
    provider::events::{ConnectMode, EventMask, EventSender, ProviderMessage},
    store::fs::FsStore,
    BlobFormat, BlobsProtocol, Hash,
};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::util;

#[derive(Serialize, Deserialize)]
struct Manifest {
    root: String,
    files: Vec<FileEntry>,
}

#[derive(Serialize, Deserialize)]
struct FileEntry {
    path: String,
    hash: String,
}

pub async fn create_store(parent: &Path, name: &str) -> Result<(FsStore, PathBuf)> {
    let dir = util::prepare_store_dir(parent, name).await?;
    let store = FsStore::load(&dir)
        .await
        .context("failed to create blob store")?;
    Ok((store, dir))
}

/// Whether a walked entry is a file we can index.
///
/// `DirEntry::file_type` does not follow symlinks, so a symlink to a file
/// would be skipped; use `std`'s following check to include those too.
fn is_indexable_file(entry: &walkdir::DirEntry) -> bool {
    entry.file_type().is_file() || (entry.file_type().is_symlink() && entry.path().is_file())
}

/// Error context for an `add_path` failure, including the offending file.
fn index_context(path: &Path) -> String {
    if !path.exists() {
        format!(
            "failed to index {} (file disappeared during indexing)",
            path.display()
        )
    } else {
        format!("failed to index {}", path.display())
    }
}

pub fn scan(paths: &[PathBuf]) -> (Vec<PathBuf>, u64) {
    let mut files = Vec::new();
    let mut total: u64 = 0;
    for path in paths {
        if path.is_file() {
            total += path.metadata().map(|m| m.len()).unwrap_or(0);
            files.push(path.clone());
        } else if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if is_indexable_file(&entry) {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0);
                    files.push(entry.path().to_path_buf());
                }
            }
        }
    }
    (files, total)
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if root.is_file() {
        files.push(root.to_path_buf());
    } else {
        for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
            if is_indexable_file(&entry) {
                files.push(entry.path().to_path_buf());
            }
        }
    }
    files
}

pub async fn import(
    paths: &[PathBuf],
    store: &FsStore,
) -> Result<(Hash, BlobFormat)> {
    let blobs = store.blobs();

    let mut manifest_files = Vec::new();
    let mut hashseq = Vec::new();

    for path in paths {
        let abs = std::path::absolute(path)?;
        let root_name = path
            .file_name()
            .context("invalid path")?
            .to_string_lossy()
            .to_string();

        if abs.is_file() {
            import_single_file(&abs, &root_name, &mut manifest_files, &mut hashseq, store).await?;
        } else if abs.is_dir() {
            import_dir(&abs, &root_name, &mut manifest_files, &mut hashseq, store).await?;
        } else {
            eprintln!(
                "warning: skipping {} (not a file or directory)",
                abs.display()
            );
        }
    }

    let manifest = Manifest {
        root: String::new(),
        files: manifest_files,
    };
    let manifest_json = serde_json::to_vec(&manifest)?;
    let manifest_tag = blobs.add_slice(manifest_json).await?;

    let mut all = Vec::with_capacity(32 + hashseq.len() * 32);
    all.extend_from_slice(manifest_tag.hash.as_bytes());
    for h in &hashseq {
        all.extend_from_slice(h.as_bytes());
    }
    let root_tag = blobs.add_slice(all).await?;

    Ok((root_tag.hash, BlobFormat::HashSeq))
}

async fn import_single_file(
    abs: &Path,
    root_name: &str,
    manifest_files: &mut Vec<FileEntry>,
    hashseq: &mut Vec<Hash>,
    store: &FsStore,
) -> Result<()> {
    let blobs = store.blobs();
    let opts = AddPathOptions {
        path: abs.to_path_buf(),
        format: BlobFormat::Raw,
        mode: ImportMode::TryReference,
    };
    let tag = blobs
        .add_path_with_opts(opts)
        .await
        .with_context(|| index_context(abs))?;

    manifest_files.push(FileEntry {
        path: root_name.to_string(),
        hash: tag.hash.to_hex(),
    });
    hashseq.push(tag.hash);
    Ok(())
}

async fn import_dir(
    abs: &Path,
    root_name: &str,
    manifest_files: &mut Vec<FileEntry>,
    hashseq: &mut Vec<Hash>,
    store: &FsStore,
) -> Result<()> {
    let blobs = store.blobs();
    let files = collect_files(abs);
    for file_path in &files {
        let rel = file_path
            .strip_prefix(abs)
            .context("strip prefix")?;
        let rel_str = format!("{}/{}", root_name, rel.to_string_lossy().replace('\\', "/"));

        let opts = AddPathOptions {
            path: file_path.clone(),
            format: BlobFormat::Raw,
            mode: ImportMode::TryReference,
        };
        let tag = blobs
            .add_path_with_opts(opts)
            .await
            .with_context(|| index_context(file_path))?;

        manifest_files.push(FileEntry {
            path: rel_str,
            hash: tag.hash.to_hex(),
        });
        hashseq.push(tag.hash);
    }
    Ok(())
}

/// Outcome of a send session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeOutcome {
    /// The receiver signalled a successful transfer.
    Completed,
    /// The receiver signalled that it ended with an error.
    ReceiverError,
    /// The user aborted with Ctrl+C.
    Interrupted,
}

/// ALPN for the receiver's explicit "transfer finished" signal.
///
/// The sender exits only after receiving this signal; connection close or
/// idle time alone never ends the session (the receiver may pause for local
/// verification, e.g. `--retry-failed`, without downloading anything).
pub const ALPN_DONE: &[u8] = b"ftrans-done";

/// Handles the receiver's completion signal: one byte, 0 = success, anything
/// else = error.
#[derive(Debug)]
struct DoneHandler {
    tx: tokio::sync::mpsc::Sender<u8>,
}

impl ProtocolHandler for DoneHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        if let Ok((_send, mut recv)) = conn.accept_bi().await {
            let mut buf = [0u8; 1];
            let _ = recv.read_exact(&mut buf).await;
            let _ = self.tx.send(buf[0]).await;
        }
        Ok(())
    }

    async fn shutdown(&self) {}
}

/// Build the blobs router (plus the completion-signal handler) and start
/// listening for connections.
///
/// Must be called before the ticket is handed out, otherwise the receiver can
/// connect before the ALPN handlers are registered and the download will fail.
pub async fn spawn_router(
    endpoint: Endpoint,
    store: FsStore,
) -> Result<(Router, tokio::sync::mpsc::Receiver<u8>)> {
    let mask = EventMask {
        connected: ConnectMode::Notify,
        ..Default::default()
    };
    let (events, event_rx) = EventSender::channel(64, mask);
    let blobs = BlobsProtocol::new(&store, Some(events));
    let (done_tx, done_rx) = tokio::sync::mpsc::channel(1);
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .accept(ALPN_DONE, DoneHandler { tx: done_tx })
        .spawn();

    // Yield to let router background tasks start listening
    tokio::task::yield_now().await;
    tokio::spawn(print_transfer_events(event_rx));
    Ok((router, done_rx))
}

/// Print connection events from the blobs provider.
async fn print_transfer_events(mut event_rx: tokio::sync::mpsc::Receiver<ProviderMessage>) {
    while let Some(msg) = event_rx.recv().await {
        if let ProviderMessage::ClientConnectedNotify(_) = msg {
            println!("Receiver connected. Transferring...");
        }
    }
}

/// Wait until the receiver explicitly signals completion, or Ctrl+C.
pub async fn wait_for_transfer(done_rx: &mut tokio::sync::mpsc::Receiver<u8>) -> ServeOutcome {
    let status = tokio::select! {
        s = done_rx.recv() => s,
        _ = tokio::signal::ctrl_c() => {
            println!("\nInterrupted (Ctrl+C)");
            None
        }
    };
    match status {
        Some(0) => ServeOutcome::Completed,
        Some(_) => ServeOutcome::ReceiverError,
        None => ServeOutcome::Interrupted,
    }
}

/// Serve the blobs until the receiver signals completion or the user presses
/// Ctrl+C. Prints progress on stdout via the returned [`ServeOutcome`].
pub async fn serve(endpoint: Endpoint, store: FsStore) -> Result<ServeOutcome> {
    let (router, mut done_rx) = spawn_router(endpoint, store).await?;
    let outcome = wait_for_transfer(&mut done_rx).await;
    router.shutdown().await?;
    Ok(outcome)
}
