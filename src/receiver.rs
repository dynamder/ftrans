use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_blobs::{
    api::blobs::{AddPathOptions, ExportOptions},
    api::downloader::Downloader,
    api::proto::{ExportMode, ImportMode},
    store::fs::FsStore,
    ticket::BlobTicket,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::util;

#[derive(Serialize, Deserialize)]
struct Manifest {
    #[allow(dead_code)]
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

/// Download the transfer.
///
/// A HashSeq transfer is fetched as: index (root hashseq) → manifest → every
/// file, with up to `parallel` files downloaded concurrently over separate
/// streams. This matters for transfers with many files: the downloader is
/// sequential per request, so multiple streams overlap per-file latency.
pub async fn download(
    ticket: &BlobTicket,
    endpoint: &Endpoint,
    store: &FsStore,
    parallel: usize,
    only: Option<&HashSet<String>>,
    strip_root: bool,
) -> Result<()> {
    register_sender_addr(endpoint, ticket.addr().clone());
    let downloader = store.downloader(endpoint);
    let peer_id = ticket.addr().id;
    let parallel = parallel.max(1);

    let pb = spinner();

    // Single blob: nothing to parallelize.
    if ticket.format() != iroh_blobs::BlobFormat::HashSeq {
        pb.set_message("transferring...".to_string());
        downloader
            .download(ticket.hash_and_format(), Some(peer_id))
            .await
            .context("download failed")?;
        pb.finish_with_message("transfer complete".to_string());
        return Ok(());
    }

    let (manifest, file_hashes) =
        fetch_index_and_manifest(ticket, &downloader, store, peer_id, &pb, strip_root).await?;
    download_files(&downloader, peer_id, &manifest, &file_hashes, only, parallel, &pb).await?;
    pb.finish_with_message("transfer complete".to_string());
    Ok(())
}

/// Retry a transfer for files that failed a previous verify.
///
/// If `<output>/ftrans-failed.txt` exists (written by a failed verify), only
/// those files are re-transferred. Otherwise the local output files are
/// checked against the sender's manifest (no full download) and every file
/// that is missing or mismatched is re-transferred. In both cases only the
/// failing files are downloaded, exported and verified.
pub async fn retry_failed(
    ticket: &BlobTicket,
    endpoint: &Endpoint,
    store: &FsStore,
    output: &Path,
    parallel: usize,
    strip_root: bool,
    print_retried: bool,
) -> Result<()> {
    register_sender_addr(endpoint, ticket.addr().clone());
    let downloader = store.downloader(endpoint);
    let peer_id = ticket.addr().id;
    let abs_output = std::path::absolute(output)?;
    let pb = spinner();

    // Single blob: just fetch, export and verify it.
    if ticket.format() != iroh_blobs::BlobFormat::HashSeq {
        pb.set_message("transferring...".to_string());
        downloader
            .download(ticket.hash_and_format(), Some(peer_id))
            .await
            .context("download failed")?;
        pb.finish_with_message("transfer complete".to_string());
        export_single(ticket.hash(), &abs_output, store).await?;
        verify_single(ticket, &abs_output, store).await?;
        if print_retried {
            println!("Retrying file: {}", abs_output.display());
        }
        return Ok(());
    }

    let (manifest, file_hashes) =
        fetch_index_and_manifest(ticket, &downloader, store, peer_id, &pb, strip_root).await?;

    // Determine which files need re-transferring.
    let list = abs_output.join("ftrans-failed.txt");
    let failed: HashSet<String> = if let Ok(content) = tokio::fs::read_to_string(&list).await {
        let set: HashSet<String> = content
            .lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if set.is_empty() {
            anyhow::bail!("{} is empty", list.display());
        }
        println!(
            "Retrying {} previously failed file(s) from {}",
            set.len(),
            list.display()
        );
        set
    } else {
        // No list: check every local file against the sender's manifest.
        println!(
            "No failed-file list at {}; checking local files against the sender...",
            list.display()
        );
        let failures = check_exported_files(&manifest.files, &abs_output, store).await?;
        if failures.is_empty() {
            pb.finish_with_message("nothing to retry");
            println!(
                "All {} local files already match the sender; nothing to retry.",
                manifest.files.len()
            );
            return Ok(());
        }
        println!(
            "{} of {} local files differ; retrying only those.",
            failures.len(),
            manifest.files.len()
        );
        failures.into_iter().map(|(p, _)| p).collect()
    };
    if print_retried {
        print_retried_files(&failed);
    }

    download_files(
        &downloader,
        peer_id,
        &manifest,
        &file_hashes,
        Some(&failed),
        parallel,
        &pb,
    )
    .await?;
    pb.finish_with_message("download complete".to_string());

    export(ticket, output, store, Some(&failed), strip_root).await?;
    verify(ticket, output, store, Some(&failed), strip_root).await?;

    // Everything retried verified: clean up the stale list.
    let _ = tokio::fs::remove_file(&list).await;
    println!("All {} retried files verified.", failed.len());
    Ok(())
}

/// Fetch the index (root hashseq content) and the manifest, without any file
/// data. Applies `--strip-root` to the manifest paths if requested.
async fn fetch_index_and_manifest(
    ticket: &BlobTicket,
    downloader: &Downloader,
    store: &FsStore,
    peer_id: EndpointId,
    pb: &ProgressBar,
    strip_root: bool,
) -> Result<(Manifest, Vec<iroh_blobs::Hash>)> {
    pb.set_message("fetching index...".to_string());
    downloader
        .download(
            iroh_blobs::HashAndFormat::new(ticket.hash(), iroh_blobs::BlobFormat::Raw),
            Some(peer_id),
        )
        .await
        .context("failed to download index")?;

    let mut reader = store.blobs().reader(ticket.hash());
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;
    anyhow::ensure!(data.len() >= 32, "invalid hashseq: too short");
    anyhow::ensure!(data.len() % 32 == 0, "invalid hashseq: not aligned");

    let manifest_hash = iroh_blobs::Hash::from_bytes(data[..32].try_into().unwrap());
    let file_hashes: Vec<iroh_blobs::Hash> = data[32..]
        .chunks_exact(32)
        .map(|c| iroh_blobs::Hash::from_bytes(c.try_into().unwrap()))
        .collect();

    pb.set_message("fetching manifest...".to_string());
    downloader
        .download(
            iroh_blobs::HashAndFormat::new(manifest_hash, iroh_blobs::BlobFormat::Raw),
            Some(peer_id),
        )
        .await
        .context("failed to download manifest")?;

    let mut manifest = read_manifest(manifest_hash, store).await?;
    if strip_root {
        for entry in &mut manifest.files {
            entry.path = strip_root_path(&entry.path);
        }
    }
    Ok((manifest, file_hashes))
}

/// Remove the leading "{source-root}/" segment from a manifest path.
fn strip_root_path(p: &str) -> String {
    match p.split_once('/') {
        Some((_, rest)) if !rest.is_empty() => rest.to_string(),
        _ => p.to_string(),
    }
}

/// Download the given files (all of them unless `only` is set) with bounded
/// parallelism over separate streams.
async fn download_files(
    downloader: &Downloader,
    peer_id: EndpointId,
    manifest: &Manifest,
    file_hashes: &[iroh_blobs::Hash],
    only: Option<&HashSet<String>>,
    parallel: usize,
    pb: &ProgressBar,
) -> Result<()> {
    let targets: Vec<(String, iroh_blobs::Hash)> = manifest
        .files
        .iter()
        .zip(file_hashes.iter())
        .filter(|(e, _)| only.map_or(true, |set| set.contains(&e.path)))
        .map(|(e, &h)| (e.path.clone(), h))
        .collect();

    pb.set_message(format!(
        "downloading {} files ({} parallel streams)...",
        targets.len(),
        parallel,
    ));

    let sem = Arc::new(tokio::sync::Semaphore::new(parallel));
    let mut set = tokio::task::JoinSet::new();
    for (_, hash) in &targets {
        let hash = *hash;
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .context("failed to acquire download slot")?;
        let dl = downloader.clone();
        set.spawn(async move {
            let _permit = permit;
            dl.download(
                iroh_blobs::HashAndFormat::new(hash, iroh_blobs::BlobFormat::Raw),
                Some(peer_id),
            )
            .await
        });
    }
    while let Some(res) = set.join_next().await {
        res.context("download task panicked")?
            .with_context(|| "parallel file download failed")?;
    }
    Ok(())
}

/// Re-hash every given file on disk (reference mode: no copying) and return
/// the ones that are missing or do not match, as (manifest path, detail).
async fn check_exported_files(
    files: &[FileEntry],
    output: &Path,
    store: &FsStore,
) -> Result<Vec<(String, String)>> {
    let blobs = store.blobs();
    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template("{bar:40.cyan/blue} {pos}/{len} files")
            .unwrap()
            .progress_chars("##-"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(200));

    let mut failures: Vec<(String, String)> = Vec::new();
    for entry in files {
        let dest = output.join(&entry.path);
        if !dest.is_file() {
            failures.push((
                entry.path.clone(),
                format!("missing: {}", dest.display()),
            ));
        } else {
            match blobs
                .add_path_with_opts(AddPathOptions {
                    path: dest.clone(),
                    format: iroh_blobs::BlobFormat::Raw,
                    mode: ImportMode::TryReference,
                })
                .await
            {
                Ok(tag) => {
                    if tag.hash.to_hex() != entry.hash {
                        failures.push((
                            entry.path.clone(),
                            format!(
                                "checksum mismatch: {} (expected {}, got {})",
                                dest.display(),
                                entry.hash,
                                tag.hash.to_hex()
                            ),
                        ));
                    }
                }
                Err(cause) => {
                    failures.push((
                        entry.path.clone(),
                        format!("read error: {} ({cause:#})", dest.display()),
                    ));
                }
            }
        }
        pb.inc(1);
    }
    pb.finish_and_clear();
    Ok(failures)
}

pub async fn export(
    ticket: &BlobTicket,
    output: &Path,
    store: &FsStore,
    only: Option<&HashSet<String>>,
    strip_root: bool,
) -> Result<()> {
    let abs_output = std::path::absolute(output)?;
    tokio::fs::create_dir_all(&abs_output).await?;

    if ticket.format() == iroh_blobs::BlobFormat::HashSeq {
        export_hashseq(ticket.hash(), &abs_output, store, only, strip_root).await?;
    } else {
        export_single(ticket.hash(), &abs_output, store).await?;
    }

    Ok(())
}

async fn export_single(hash: iroh_blobs::Hash, dest: &Path, store: &FsStore) -> Result<()> {
    store
        .blobs()
        .export_with_opts(ExportOptions {
            hash,
            mode: ExportMode::TryReference,
            target: dest.to_owned(),
        })
        .await
        .context("export failed")?;
    Ok(())
}

async fn export_hashseq(
    root_hash: iroh_blobs::Hash,
    output: &Path,
    store: &FsStore,
    only: Option<&HashSet<String>>,
    strip_root: bool,
) -> Result<()> {
    let info = load_transfer(root_hash, store, strip_root).await?;
    let Manifest { files, .. } = info.manifest;

    let selected: Vec<(usize, &FileEntry)> = files
        .iter()
        .enumerate()
        .filter(|(_, e)| only.map_or(true, |set| set.contains(&e.path)))
        .collect();

    if only.is_some() && selected.is_empty() {
        anyhow::bail!(
            "none of the {} requested file(s) matched the manifest (manifest paths e.g.: {:?}); the retry list may be stale or malformed",
            only.map(|s| s.len()).unwrap_or(0),
            files.iter().take(3).map(|e| e.path.clone()).collect::<Vec<_>>(),
        );
    }

    println!(
        "Exporting {} files to {}...",
        selected.len(),
        output.display()
    );

    for (i, entry) in selected {
        let file_hash = info.file_hashes[i];
        let dest = output.join(&entry.path);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if !store.blobs().has(file_hash).await? {
            anyhow::bail!("file blob not downloaded: {}", entry.path);
        }
        store
            .blobs()
            .export_with_opts(ExportOptions {
                hash: file_hash,
                mode: ExportMode::TryReference,
                target: dest,
            })
            .await
            .with_context(|| format!("export failed: {}", entry.path))?;
    }

    Ok(())
}

/// Re-check every exported file against the sender's checksums by re-hashing
/// it from disk (reference mode: no copying). All selected files are verified
/// even if some fail; failures are collected and reported together, and the
/// call returns an error listing every failing file. If any fail, their
/// manifest paths are written to `<output>/ftrans-failed.txt` so a later
/// `receive --retry-failed` can re-transfer just those files.
pub async fn verify(
    ticket: &BlobTicket,
    output: &Path,
    store: &FsStore,
    only: Option<&HashSet<String>>,
    strip_root: bool,
) -> Result<()> {
    let abs_output = std::path::absolute(output)?;

    if ticket.format() == iroh_blobs::BlobFormat::HashSeq {
        let info = load_transfer(ticket.hash(), store, strip_root).await?;
        let sample: Vec<String> = info
            .manifest
            .files
            .iter()
            .take(3)
            .map(|e| e.path.clone())
            .collect();
        let selected: Vec<FileEntry> = info
            .manifest
            .files
            .into_iter()
            .filter(|e| only.map_or(true, |set| set.contains(&e.path)))
            .collect();

        if only.is_some() && selected.is_empty() {
            anyhow::bail!(
                "none of the {} requested file(s) matched the manifest (manifest paths e.g.: {:?}); the retry list may be stale or malformed",
                only.map(|s| s.len()).unwrap_or(0),
                sample,
            );
        }
        println!("Verifying {} exported files...", selected.len());

        let failures = check_exported_files(&selected, &abs_output, store).await?;
        if failures.is_empty() {
            println!("All {} files verified.", selected.len());
            Ok(())
        } else {
            // Persist the failed files so `--retry-failed` can re-transfer them.
            let list = abs_output.join("ftrans-failed.txt");
            let paths: Vec<String> = failures.iter().map(|(p, _)| p.clone()).collect();
            let _ = tokio::fs::write(&list, paths.join("\n")).await;
            eprintln!("failed file list written to {}", list.display());

            let mut msg = format!(
                "{}/{} files failed verification:",
                failures.len(),
                selected.len()
            );
            for (_, detail) in &failures {
                msg.push_str("\n  - ");
                msg.push_str(detail);
            }
            anyhow::bail!("{msg}");
        }
    } else {
        verify_single(ticket, &abs_output, store).await
    }
}

async fn verify_single(ticket: &BlobTicket, abs_output: &Path, store: &FsStore) -> Result<()> {
    let tag = store
        .blobs()
        .add_path_with_opts(AddPathOptions {
            path: abs_output.to_path_buf(),
            format: iroh_blobs::BlobFormat::Raw,
            mode: ImportMode::TryReference,
        })
        .await
        .context("failed to read back exported file")?;
    anyhow::ensure!(
        tag.hash == ticket.hash(),
        "checksum mismatch: expected {}, got {}",
        ticket.hash(),
        tag.hash,
    );
    println!("File verified.");
    Ok(())
}

/// Parse the root hashseq and the manifest it points to.
struct TransferInfo {
    manifest: Manifest,
    file_hashes: Vec<iroh_blobs::Hash>,
}

async fn load_transfer(
    root_hash: iroh_blobs::Hash,
    store: &FsStore,
    strip_root: bool,
) -> Result<TransferInfo> {
    let mut reader = store.blobs().reader(root_hash);
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;

    anyhow::ensure!(data.len() >= 32, "invalid hashseq: too short");
    anyhow::ensure!(data.len() % 32 == 0, "invalid hashseq: not aligned");

    let manifest_hash = iroh_blobs::Hash::from_bytes(data[..32].try_into().unwrap());
    let file_hashes: Vec<iroh_blobs::Hash> = data[32..]
        .chunks_exact(32)
        .map(|c| iroh_blobs::Hash::from_bytes(c.try_into().unwrap()))
        .collect();

    if !store.blobs().has(manifest_hash).await? {
        anyhow::bail!("manifest blob not downloaded");
    }

    let mut manifest = read_manifest(manifest_hash, store).await?;
    if strip_root {
        for entry in &mut manifest.files {
            entry.path = strip_root_path(&entry.path);
        }
    }
    anyhow::ensure!(
        manifest.files.len() == file_hashes.len(),
        "manifest/file count mismatch: {} vs {}",
        manifest.files.len(),
        file_hashes.len(),
    );

    Ok(TransferInfo {
        manifest,
        file_hashes,
    })
}

async fn read_manifest(hash: iroh_blobs::Hash, store: &FsStore) -> Result<Manifest> {
    let mut reader = store.blobs().reader(hash);
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;
    let manifest: Manifest = serde_json::from_slice(&data).context("invalid manifest")?;
    Ok(manifest)
}

fn spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
            .unwrap()
            .progress_chars("##-"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

/// Print the list of files being retried (sorted for readability).
fn print_retried_files(files: &HashSet<String>) {
    println!("Retrying these file(s):");
    let mut sorted: Vec<&String> = files.iter().collect();
    sorted.sort();
    for f in sorted {
        println!("  - {f}");
    }
}

/// Register the sender's addresses from the ticket so discovery does not
/// depend on (flaky) mDNS.
pub(crate) fn register_sender_addr(endpoint: &Endpoint, addr: EndpointAddr) {
    if let Ok(lookup) = endpoint.address_lookup() {
        lookup.add(MemoryLookup::from_endpoint_info([addr]));
    }
}

/// How long to allow for the short-code ticket exchange.
const META_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Fetch the transfer ticket from a sender that was found by its session code.
///
/// The code travels inside the encrypted QUIC connection, and the sender only
/// answers with the ticket when it matches the code it announced on the LAN.
pub async fn fetch_ticket(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    code: &str,
) -> Result<BlobTicket> {
    register_sender_addr(endpoint, addr.clone());

    let conn = tokio::time::timeout(
        META_TIMEOUT,
        endpoint.connect(addr, crate::sender::ALPN_META),
    )
    .await
    .context("timed out connecting to the sender")?
    .context("failed to connect to the sender")?;

    let outcome = exchange_ticket(&conn, code).await;

    // Closing tells the sender the ticket arrived; it waits for exactly this
    // before dropping the connection.
    conn.close(0u32.into(), b"ftrans-meta-done");
    outcome
}

async fn exchange_ticket(conn: &iroh::endpoint::Connection, code: &str) -> Result<BlobTicket> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .context("failed to open the ticket stream")?;

    let code_bytes = code.as_bytes();
    anyhow::ensure!(code_bytes.len() <= u8::MAX as usize, "session code too long");
    send.write_all(&[code_bytes.len() as u8]).await?;
    send.write_all(code_bytes).await?;
    let _ = send.finish();

    // iroh's recv stream has its own read_to_end with a size limit.
    let buf = tokio::time::timeout(META_TIMEOUT, recv.read_to_end(64 * 1024))
        .await
        .context("timed out waiting for the ticket")??;
    anyhow::ensure!(
        !buf.is_empty(),
        "the sender rejected the session code (typo, or that session already ended)"
    );

    let ticket = String::from_utf8(buf).context("the sender sent an invalid ticket")?;
    ticket
        .trim()
        .parse()
        .context("the sender sent an unparseable ticket")
}

/// Full receive flow with automatic retry of files that fail verification.
///
/// Downloads everything, exports it, then verifies; on verification failure
/// the failing files are re-downloaded and re-exported (only those), up to
/// `max_retries` additional attempts.
pub async fn receive(
    ticket: &BlobTicket,
    endpoint: &Endpoint,
    store: &FsStore,
    output: &Path,
    parallel: usize,
    strip_root: bool,
    max_retries: usize,
    print_retried: bool,
) -> Result<()> {
    // Connection establishment can fail transiently (mDNS/QUIC hiccups), so
    // retry the initial download a few times before giving up.
    let mut last_err = None;
    for attempt in 0..3 {
        match download(ticket, endpoint, store, parallel, None, strip_root).await {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(e) => {
                if attempt < 2 {
                    eprintln!(
                        "download attempt {}/3 failed: {e:#}; retrying...",
                        attempt + 1
                    );
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                } else {
                    last_err = Some(e);
                }
            }
        }
    }
    if let Some(e) = last_err {
        return Err(e);
    }

    export(ticket, output, store, None, strip_root).await?;

    let mut only: Option<HashSet<String>> = None;
    for attempt in 0..=max_retries {
        match verify(ticket, output, store, only.as_ref(), strip_root).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt == max_retries => return Err(e),
            Err(_) => {
                // verify already wrote the failed-file list.
                let abs_output = std::path::absolute(output)?;
                let list = abs_output.join("ftrans-failed.txt");
                let content = tokio::fs::read_to_string(&list)
                    .await
                    .with_context(|| {
                        format!(
                            "verification failed but {} could not be read",
                            list.display()
                        )
                    })?;
                let set: HashSet<String> = content
                    .lines()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
                eprintln!(
                    "verification failed for {} file(s); retrying (attempt {}/{})",
                    set.len(),
                    attempt + 1,
                    max_retries
                );
                if print_retried {
                    print_retried_files(&set);
                }
                download(ticket, endpoint, store, parallel, Some(&set), strip_root).await?;
                export(ticket, output, store, Some(&set), strip_root).await?;
                only = Some(set);
            }
        }
    }
    unreachable!("loop always returns")
}

/// Tell the sender that this receiver is done. `ok` is true on success.
///
/// The sender only terminates after receiving this signal, so it must be sent
/// even on failure (the sender then knows to shut down as well). Retries a few
/// times because a transient connect failure must not leave the sender hanging.
pub async fn send_done(endpoint: &Endpoint, ticket: &BlobTicket, ok: bool) {
    const ATTEMPTS: usize = 3;
    const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
    for attempt in 0..ATTEMPTS {
        let conn = match tokio::time::timeout(
            CONNECT_TIMEOUT,
            endpoint.connect(ticket.addr().clone(), crate::sender::ALPN_DONE),
        )
        .await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                eprintln!(
                    "warning: failed to notify sender of completion (connect, attempt {}/{}): {e:?}",
                    attempt + 1,
                    ATTEMPTS
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            Err(_) => {
                eprintln!(
                    "warning: timed out notifying sender of completion (attempt {}/{})",
                    attempt + 1,
                    ATTEMPTS
                );
                continue;
            }
        };
        match conn.open_bi().await {
            Ok((mut send, _recv)) => {
                let _ = send.write_all(&[if ok { 0 } else { 1 }]).await;
                let _ = send.finish();
                // Give the sender a moment to process the signal before our
                // endpoint is closed: closing immediately can drop in-flight
                // QUIC data and leave the sender waiting forever.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                return;
            }
            Err(e) => {
                eprintln!(
                    "warning: failed to notify sender of completion (open_bi, attempt {}/{}): {e:?}",
                    attempt + 1,
                    ATTEMPTS
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
}
