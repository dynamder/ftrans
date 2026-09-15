use anyhow::{Context, Result};
use clap::Parser;

use ftrans::{cli, net, receiver, sender, util};

/// How long to wait for a home relay when one is configured. Never wait
/// indefinitely: `Endpoint::online` stalls forever when relays are unreachable,
/// which is exactly what restricted networks (e.g. campus Wi-Fi) do.
const RELAY_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();
    let relay = cli.relay;

    match cli.command {
        cli::Command::Send { path, store_dir } => {
            anyhow::ensure!(!path.is_empty(), "at least one --path is required");

            let (files, total) = sender::scan(&path);
            println!(
                "Found {} files, {} total",
                files.len(),
                util::human_bytes(total),
            );

            let store_parent = match store_dir {
                Some(d) => d,
                None => {
                    util::ensure_writable_store_parent(&util::default_store_parent(&path)).await
                }
            };
            println!(
                "Indexing files... (temp store: {})",
                store_parent.join("ftrans-send").display(),
            );
            let endpoint = net::create_endpoint(&relay).await?;
            if relay.uses_relay() && !net::wait_online(&endpoint, RELAY_WAIT).await {
                eprintln!(
                    "warning: no relay connection after {}s; only peers reachable \
                     directly on this network can connect",
                    RELAY_WAIT.as_secs(),
                );
            }
            println!("Transport: {}", relay.label());
            let local_addrs = net::ip_addrs(&endpoint.addr());
            println!("Listening on: {}", net::format_addrs(&local_addrs));
            if local_addrs.is_empty() {
                eprintln!(
                    "warning: no local IP addresses to advertise; the receiver will \
                     have to find this machine via mDNS"
                );
            }

            let (store, store_dir) = sender::create_store(&store_parent, "ftrans-send").await?;
            let (root_hash, root_format) = sender::import(&path, &store)
                .await
                .context("indexing failed")?;

            // Start listening BEFORE handing out the ticket, otherwise the
            // receiver can connect before the ALPN handler is registered.
            let addr = endpoint.addr();
            let (router, mut done_rx) = sender::spawn_router(endpoint, store).await?;
            let ticket = iroh_blobs::ticket::BlobTicket::new(addr, root_hash, root_format);

            println!("\n=== Transfer ticket ===");
            println!("{}", ticket);
            println!("\nOn the new machine, run:");
            println!("  ftrans receive \"{}\" --output <dir>", ticket);
            println!("Waiting for connection... (Ctrl+C to stop)");

            // Ensure ticket is flushed to stdout (piped stdout is block-buffered)
            use std::io::Write;
            let _ = std::io::stdout().flush();

            let outcome = sender::wait_for_transfer(&mut done_rx).await;
            router.shutdown().await?;

            if store_dir.exists() {
                let _ = tokio::fs::remove_dir_all(&store_dir).await;
            }

            match outcome {
                sender::ServeOutcome::Completed => {
                    println!(
                        "\nTransfer complete! All {} files ({} total) were sent.",
                        files.len(),
                        util::human_bytes(total),
                    );
                }
                sender::ServeOutcome::ReceiverError => {
                    println!("\nReceiver ended the session with an error.");
                }
                sender::ServeOutcome::Interrupted => {
                    println!("Shutting down...\nDone.");
                }
            }
            let _ = std::io::stdout().flush();
        }
        cli::Command::Receive {
            ticket,
            output,
            store_dir,
            no_verify,
            parallel,
            retry_failed,
            strip_root,
            max_retries,
            print_retried,
        } => {
            let ticket: iroh_blobs::ticket::BlobTicket = ticket
                .trim()
                .parse()
                .context("invalid ticket format")?;

            println!(
                "Connecting to sender ({} + mDNS + QUIC)...",
                relay.label()
            );
            let endpoint = net::create_endpoint(&relay).await?;
            let store_parent = match store_dir {
                Some(d) => d,
                None => {
                    util::ensure_writable_store_parent(&util::default_store_parent(
                        std::slice::from_ref(&output),
                    ))
                    .await
                }
            };
            let (store, store_dir) = receiver::create_store(&store_parent, "ftrans-recv").await?;

            // Run the receive flow, then explicitly tell the sender we are
            // done (the sender only exits after this signal).
            let result: anyhow::Result<()> = async {
                if retry_failed {
                    receiver::retry_failed(
                        &ticket,
                        &endpoint,
                        &store,
                        &output,
                        parallel,
                        strip_root,
                        print_retried,
                    )
                    .await
                } else if no_verify {
                    receiver::download(&ticket, &endpoint, &store, parallel, None, strip_root).await?;
                    receiver::export(&ticket, &output, &store, None, strip_root).await?;
                    Ok(())
                } else {
                    receiver::receive(
                        &ticket,
                        &endpoint,
                        &store,
                        &output,
                        parallel,
                        strip_root,
                        max_retries,
                        print_retried,
                    )
                    .await
                }
            }
            .await;
            receiver::send_done(&endpoint, &ticket, result.is_ok()).await;
            if let Err(e) = &result {
                eprintln!("\nTransfer failed: {e:#}");
                eprint!("{}", failure_hints(&relay));
            }
            result?;

            endpoint.close().await;

            if store_dir.exists() {
                let _ = tokio::fs::remove_dir_all(&store_dir).await;
            }

            println!("All done!");
        }
    }

    Ok(())
}

/// Actionable hints printed when a receive fails.
///
/// On a campus network the usual causes are, in order of likelihood: Windows
/// Firewall dropping inbound UDP, the two machines sitting on different subnets,
/// or Wi-Fi client isolation (AP isolation) blocking peer-to-peer traffic
/// entirely — the last one cannot be fixed from userspace, only by pairing the
/// machines over a hotspot or cable.
fn failure_hints(relay: &net::RelayChoice) -> String {
    let mut hints = String::from(
        "\nhints:\n\
         \x20 - make sure the sender is still running and both machines are on the \
         same subnet\n\
         \x20 - on Windows, allow inbound UDP for ftrans.exe (Firewall -> the network \
         profile must not be \"Public\" if the rule is restricted there)\n\
         \x20 - campus Wi-Fi often enables client isolation, which blocks all \
         machine-to-machine traffic: check with a quick ping between the two machines\n\
         \x20 - if isolation is on, join the machines directly instead: start a \
         hotspot on one of them (Windows: Mobile hotspot; Linux: nmcli device wifi \
         hotspot) and connect the other to it\n",
    );
    if relay.uses_relay() {
        hints.push_str(
            "\x20 - a relay was configured but may be unreachable here; retry with \
             --relay none for a pure LAN transfer\n",
        );
    }
    hints
}
