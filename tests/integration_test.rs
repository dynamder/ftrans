use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tempfile::TempDir;

fn create_test_data(root: &Path) -> HashMap<String, Vec<u8>> {
    let mut files: HashMap<String, Vec<u8>> = HashMap::new();

    let write_file = |dest: &Path, content: &[u8]| {
        let mut f = std::fs::File::create(dest).unwrap();
        f.write_all(content).unwrap();
    };

    let entries: Vec<(&str, Vec<u8>)> = vec![
        ("readme.txt", b"Hello, ftrans migration test!\nLine 2.\n".to_vec()),
        ("empty.dat", vec![]),
        (
            "notes.md",
            b"# Notes\n\nThis is in a subdirectory.\nIt has some text.\n".to_vec(),
        ),
        ("config.json", br#"{"key": "value", "num": 42}"#.to_vec()),
        (
            "data.bin",
            (0u8..=255).cycle().take(1024 * 1024).collect(),
        ),
        ("deep/nested/path/file.txt", b"Deeply nested.\n".to_vec()),
        ("deep/nested/path/second.txt", b"Also deeply nested.\n".to_vec()),
        (
            "special_chars/name with spaces.txt",
            b"File with spaces in name.\n".to_vec(),
        ),
        (
            "large_random.bin",
            {
                let mut rng = XorShift(0xdead_beef_cafe_babe);
                (0..100_000).map(|_| rng.next()).collect::<Vec<u8>>()
            },
        ),
    ];

    for (rel_path, content) in &entries {
        let dest = root.join(rel_path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        write_file(&dest, content);
        files.insert(rel_path.to_string(), content.clone());
    }

    files
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u8 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 & 0xFF) as u8
    }
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        files.push(entry.path().strip_prefix(root).unwrap().to_path_buf());
    }
    files.sort();
    files
}

fn verify_equal(source: &Path, dest: &Path) -> Result<(), String> {
    let src_files = collect_files(source);
    let dst_files = collect_files(dest);

    if src_files.len() != dst_files.len() {
        return Err(format!(
            "file count mismatch: source has {}, dest has {}",
            src_files.len(),
            dst_files.len(),
        ));
    }

    for (i, (sf, df)) in src_files.iter().zip(dst_files.iter()).enumerate() {
        if sf != df {
            return Err(format!(
                "path mismatch at index {i}: source={}, dest={}",
                sf.display(),
                df.display(),
            ));
        }

        let src_content = std::fs::read(source.join(sf)).unwrap();
        let dst_content = std::fs::read(dest.join(df)).unwrap();

        if src_content != dst_content {
            return Err(format!(
                "content mismatch: {}\nsrc size: {}, dst size: {}",
                sf.display(),
                src_content.len(),
                dst_content.len(),
            ));
        }
    }

    Ok(())
}

// ── In-process test (reliable, no subprocess mDNS dependency) ─────────────

#[tokio::test]
async fn end_to_end_in_process() {
    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();

    let _expected = create_test_data(src_dir.path());
    let (files, total) = ftrans::sender::scan(&[src_dir.path().to_path_buf()]);
    assert!(files.len() >= 9);
    assert!(total > 0);

    let sender_ep = ftrans::net::create_endpoint(&ftrans::net::RelayChoice::Lan)
        .await
        .unwrap();

    let recv_ep = ftrans::net::create_endpoint(&ftrans::net::RelayChoice::Lan)
        .await
        .unwrap();

    let (sender_store, sender_store_dir) = ftrans::sender::create_store(
        std::env::temp_dir().as_path(),
        "ftrans-test-send",
    )
    .await
    .unwrap();

    let paths = vec![src_dir.path().to_path_buf()];
    let (root_hash, root_format) =
        ftrans::sender::import(&paths, &sender_store).await.unwrap();

    let ticket = iroh_blobs::ticket::BlobTicket::new(
        sender_ep.addr(),
        root_hash,
        root_format,
    );
    assert!(!ticket.to_string().is_empty());

    let serve_handle = tokio::spawn(async move {
        let _ = ftrans::sender::serve(sender_ep, sender_store).await;
    });

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let (recv_store, recv_store_dir) = ftrans::receiver::create_store(
        std::env::temp_dir().as_path(),
        "ftrans-test-recv",
    )
    .await
    .unwrap();

    ftrans::receiver::download(&ticket, &recv_ep, &recv_store, 4, None, false)
        .await
        .unwrap();

    let dst_output = dst_dir.path().join("output");
    ftrans::receiver::export(&ticket, &dst_output, &recv_store, None, false)
        .await
        .unwrap();

    // New: re-hash every exported file and compare against the sender's checksums
    ftrans::receiver::verify(&ticket, &dst_output, &recv_store, None, false)
        .await
        .expect("exported files must verify against sender checksums");

    recv_ep.close().await;
    serve_handle.abort();

    let source_name = src_dir.path().file_name().unwrap();
    let actual_dest = dst_output.join(source_name);

    if actual_dest.exists() {
        verify_equal(src_dir.path(), &actual_dest).unwrap();
    } else {
        verify_equal(src_dir.path(), &dst_output).unwrap();
    }

    let _ = tokio::fs::remove_dir_all(&sender_store_dir).await;
    let _ = tokio::fs::remove_dir_all(&recv_store_dir).await;
}

// ── CLI subprocess test (requires network+mDNS, run manually) ──────────────

fn binary_path() -> PathBuf {
    let mut path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    if path.ends_with("deps") {
        path = path.parent().unwrap().to_path_buf();
    }
    path.join("ftrans").with_extension(std::env::consts::EXE_EXTENSION)
}

#[test]
#[ignore = "requires network and reliable mDNS; run manually: cargo test --test integration_test end_to_end_cli -- --ignored --nocapture"]
fn end_to_end_cli() {
    let ftrans_bin = binary_path();
    assert!(ftrans_bin.exists());

    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();

    println!("Test data at: {}", src_dir.path().display());
    let _expected = create_test_data(src_dir.path());

    println!("Starting sender...");
    let mut sender = Command::new(&ftrans_bin)
        .arg("send")
        .arg("--path")
        .arg(src_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn sender");

    let stdout = sender.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut ticket = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                print!("[SEND] {}", line);
                let trimmed = line.trim();
                if trimmed.starts_with("blob") {
                    ticket = trimmed.to_string();
                    break;
                }
            }
            Err(e) => {
                sender.kill().unwrap();
                panic!("failed to read sender stdout: {e}");
            }
        }
        if std::time::Instant::now() > deadline {
            sender.kill().unwrap();
            panic!("timed out waiting for ticket");
        }
    }

    assert!(!ticket.is_empty());
    println!("Got ticket: {ticket}");

    std::thread::sleep(std::time::Duration::from_secs(10));

    println!("Starting receiver...");
    let dst_output = dst_dir.path().join("output");

    let receiver = Command::new(&ftrans_bin)
        .arg("receive")
        .arg(&ticket)
        .arg("--output")
        .arg(&dst_output)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn receiver");

    let output = receiver.wait_with_output().unwrap();
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let stderr_str = String::from_utf8_lossy(&output.stderr);
    println!("[RECV-OUT]\n{stdout_str}");
    if !stderr_str.is_empty() {
        eprintln!("[RECV-ERR]\n{stderr_str}");
    }

    assert!(output.status.success(), "receiver failed: {stderr_str}");

    // The sender should notice the transfer finished, print a completion
    // summary, and exit on its own.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(status) = sender.try_wait().unwrap() {
            assert!(status.success(), "sender exited with error: {status:?}");
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = sender.kill();
            panic!("sender did not exit after the transfer completed");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut remaining = String::new();
    reader.read_to_string(&mut remaining).unwrap();
    if !remaining.is_empty() {
        print!("[SEND-REST]\n{remaining}");
    }
    assert!(
        remaining.contains("Transfer complete!"),
        "sender did not print a completion summary; got: {remaining}"
    );

    let source_name = src_dir.path().file_name().unwrap();
    let actual_dest = dst_output.join(source_name);

    if actual_dest.exists() {
        verify_equal(src_dir.path(), &actual_dest).unwrap();
    } else {
        verify_equal(src_dir.path(), &dst_output).unwrap();
    }

    println!("=== CLI test passed ===");
}

// ── verify: full-check behaviour reports every failing file ────────────────

#[tokio::test]
async fn verify_reports_all_failures() {
    let src_dir = TempDir::new().unwrap();
    std::fs::write(src_dir.path().join("a.txt"), b"aaa").unwrap();
    std::fs::write(src_dir.path().join("b.txt"), b"bbb").unwrap();
    std::fs::write(src_dir.path().join("c.txt"), b"ccc").unwrap();

    let endpoint = ftrans::net::create_endpoint(&ftrans::net::RelayChoice::Lan)
        .await
        .unwrap();
    let (store, store_dir) = ftrans::sender::create_store(
        std::env::temp_dir().as_path(),
        "ftrans-test-verify",
    )
    .await
    .unwrap();

    let paths = vec![src_dir.path().to_path_buf()];
    let (hash, format) = ftrans::sender::import(&paths, &store).await.unwrap();
    let ticket = iroh_blobs::ticket::BlobTicket::new(endpoint.addr(), hash, format);

    let dst = TempDir::new().unwrap();
    let out = dst.path().join("output");
    ftrans::receiver::export(&ticket, &out, &store, None, false)
        .await
        .unwrap();

    let root_name = src_dir.path().file_name().unwrap();
    // Corrupt two of the three exported files.
    std::fs::write(out.join(root_name).join("a.txt"), b"corrupted-a").unwrap();
    std::fs::write(out.join(root_name).join("b.txt"), b"corrupted-b").unwrap();

    let err = ftrans::receiver::verify(&ticket, &out, &store, None, false)
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("failed verification"), "got: {msg}");
    assert!(msg.contains("a.txt"), "a.txt should be listed: {msg}");
    assert!(msg.contains("b.txt"), "b.txt should be listed: {msg}");
    assert!(
        !msg.contains("c.txt"),
        "c.txt must not be listed as failed: {msg}"
    );

    // The failed-file list must be persisted for --retry-failed.
    let list = out.join("ftrans-failed.txt");
    let list_content = std::fs::read_to_string(&list).expect("failed list must be written");
    assert!(list_content.contains("a.txt"), "list: {list_content}");
    assert!(list_content.contains("b.txt"), "list: {list_content}");
    assert!(
        !list_content.contains("c.txt"),
        "list must not contain c.txt: {list_content}"
    );

    // Restore the corrupted files, then a filtered verify over just the
    // failed set must pass (and c.txt must not be touched).
    std::fs::write(out.join(root_name).join("a.txt"), b"aaa").unwrap();
    std::fs::write(out.join(root_name).join("b.txt"), b"bbb").unwrap();
    let only: std::collections::HashSet<String> =
        list_content.lines().map(str::to_string).collect();
    ftrans::receiver::verify(&ticket, &out, &store, Some(&only), false)
        .await
        .expect("filtered re-verify must pass");

    endpoint.close().await;
    let _ = tokio::fs::remove_dir_all(&store_dir).await;
}

// ── verify: --strip-root exports directly under --output ───────────────────

#[tokio::test]
async fn strip_root_exports_without_source_folder() {
    let src_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(src_dir.path().join("sub")).unwrap();
    std::fs::write(src_dir.path().join("top.txt"), b"top").unwrap();
    std::fs::write(src_dir.path().join("sub").join("nested.txt"), b"nested").unwrap();

    let endpoint = ftrans::net::create_endpoint(&ftrans::net::RelayChoice::Lan)
        .await
        .unwrap();
    let (store, store_dir) = ftrans::sender::create_store(
        std::env::temp_dir().as_path(),
        "ftrans-test-strip",
    )
    .await
    .unwrap();

    let paths = vec![src_dir.path().to_path_buf()];
    let (hash, format) = ftrans::sender::import(&paths, &store).await.unwrap();
    let ticket = iroh_blobs::ticket::BlobTicket::new(endpoint.addr(), hash, format);

    let dst = TempDir::new().unwrap();
    let out = dst.path().join("output");

    // With strip_root, files land directly under `out` (no source folder layer).
    ftrans::receiver::export(&ticket, &out, &store, None, true)
        .await
        .unwrap();
    assert!(out.join("top.txt").is_file(), "top.txt at root");
    assert!(out.join("sub").join("nested.txt").is_file(), "nested in sub");
    let root_name = src_dir.path().file_name().unwrap();
    assert!(
        !out.join(root_name).exists(),
        "source folder layer must be stripped"
    );

    ftrans::receiver::verify(&ticket, &out, &store, None, true)
        .await
        .expect("strip-root export must verify");

    endpoint.close().await;
    let _ = tokio::fs::remove_dir_all(&store_dir).await;
}
