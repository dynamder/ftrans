use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use iroh_blobs::ticket::BlobTicket;

use ftrans::{cli, net, receiver, sender, session, util};

/// How long to wait for a home relay when one is configured. Never wait
/// indefinitely: `Endpoint::online` stalls forever when relays are unreachable,
/// which is exactly what restricted networks (e.g. campus Wi-Fi) do.
const RELAY_WAIT: Duration = Duration::from_secs(10);

/// How long to look for the sender on the local network.
const DISCOVER_TIMEOUT: Duration = Duration::from_secs(3);

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
            let local_ips = unique_ips(&local_addrs);
            println!("Listening on: {}", net::format_addrs(&local_addrs));
            if local_addrs.is_empty() {
                eprintln!(
                    "warning: no local IP addresses to advertise; the receiver will \
                     have to be given an address with --addr"
                );
            }

            let (store, store_dir) = sender::create_store(&store_parent, "ftrans-send").await?;
            let (root_hash, root_format) = sender::import(&path, &store)
                .await
                .context("indexing failed")?;

            // The ticket is built before the router so that the short-code
            // handler can hand it out to a receiver that only knows the code.
            let addr = endpoint.addr();
            let endpoint_id = endpoint.id();
            let ticket = BlobTicket::new(addr, root_hash, root_format);
            let code = session::generate_code();
            let meta = sender::MetaHandler {
                code: code.clone(),
                ticket: ticket.to_string(),
            };

            // Start listening BEFORE announcing the code, otherwise the
            // receiver can connect before the ALPN handlers are registered.
            let (router, mut done_rx) = sender::spawn_router(endpoint, store, Some(meta)).await?;

            // Announce the session on the LAN: the receiver only types the code.
            let beacon =
                session::Beacon::new(&code, endpoint_id, &local_addrs, session::hostname());
            session::announce(beacon, local_ips.clone()).await?;

            println!("\n=== Session code ===");
            println!("    {code}");
            println!("\nOn the new machine, run:");
            println!("    ftrans receive {code} --output <dir>");
            println!("\nIf that machine cannot reach this one by broadcast discovery, add");
            println!("--addr <ip-of-this-machine> to the command above; the full ticket");
            println!("works as a fallback too:");
            println!("    ftrans receive \"{ticket}\" --output <dir>");
            println!("\nWaiting for connection... (Ctrl+C to stop)");

            // Ensure the code is flushed to stdout (piped stdout is block-buffered)
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
            addr,
            output,
            store_dir,
            no_verify,
            parallel,
            retry_failed,
            strip_root,
            max_retries,
            print_retried,
        } => {
            println!("Transport: {}", relay.label());
            let endpoint = net::create_endpoint(&relay).await?;

            // Either a full ticket, or a short code we resolve over the LAN.
            let ticket = resolve_ticket(&endpoint, ticket.as_deref(), &addr).await?;

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

/// Resolve the receiver's argument into a ticket.
///
/// A full ticket is used as-is (that path also covers transfers over a relay or
/// across networks). A short session code instead means: find the sender on the
/// local network by its announcement, then ask it for the ticket over the
/// authenticated `ftrans-meta` protocol. With no argument at all we can only
/// list what was found, since the code is what authenticates the request.
async fn resolve_ticket(
    endpoint: &Endpoint,
    arg: Option<&str>,
    hints: &[IpAddr],
) -> Result<BlobTicket> {
    if let Some(raw) = arg {
        let raw = raw.trim();
        if let Ok(ticket) = raw.parse::<BlobTicket>() {
            println!("Using the ticket given on the command line.");
            return Ok(ticket);
        }
    }

    let code = match arg {
        Some(raw) => Some(session::normalize_code(raw).ok_or_else(|| {
            anyhow::anyhow!(
                "{:?} is neither a ticket nor a {}-character session code",
                raw.trim(),
                session::CODE_LEN,
            )
        })?),
        None => None,
    };

    println!(
        "Looking for the sender on the local network (up to {}s)...",
        DISCOVER_TIMEOUT.as_secs()
    );
    let found = session::discover(&local_ips(endpoint), hints, DISCOVER_TIMEOUT).await;
    if found.is_empty() {
        anyhow::bail!(
            "no sender found. Make sure it is running and that both machines are on the same \
             network; if broadcast is filtered (many campus/guest Wi-Fi networks), pass \
             --addr <sender-ip>"
        );
    }

    let Some(code) = code else {
        anyhow::bail!(
            "found {} sender(s), but no session code was given:\n{}",
            found.len(),
            describe_beacons(&found),
        );
    };

    let tag = session::code_tag(&code);
    let Some(beacon) = found.iter().find(|b| b.tag == tag) else {
        anyhow::bail!(
            "no sender announced the code {code}; found:\n{}",
            describe_beacons(&found),
        );
    };

    println!("Found sender \"{}\", fetching the ticket...", beacon.host);
    receiver::fetch_ticket(endpoint, beacon_endpoint_addr(beacon)?, &code).await
}

fn local_ips(endpoint: &Endpoint) -> Vec<IpAddr> {
    unique_ips(&net::ip_addrs(&endpoint.addr()))
}

fn unique_ips(addrs: &[std::net::SocketAddr]) -> Vec<IpAddr> {
    let mut ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
    ips.sort();
    ips.dedup();
    ips
}

fn beacon_endpoint_addr(beacon: &session::Beacon) -> Result<EndpointAddr> {
    let id: EndpointId = beacon
        .id
        .parse()
        .context("the sender announced an invalid endpoint id")?;
    Ok(EndpointAddr::from_parts(
        id,
        beacon.socket_addrs().into_iter().map(TransportAddr::Ip),
    ))
}

fn describe_beacons(found: &[session::Beacon]) -> String {
    found
        .iter()
        .map(|b| format!("  - {} at {}", b.host, b.addrs.join(", ")))
        .collect::<Vec<_>>()
        .join("\n")
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
         \x20 - if the code was not found, retry with --addr <sender-ip>, which skips \
         broadcast discovery\n\
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
