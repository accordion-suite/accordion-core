//! `accordion-sync`: keeps a folder in sync with Accordion on Linux
//! (decision 23). Each account is one server and one folder, and they all
//! sync at once.
//!
//! ```text
//! accordion-sync add [<server>] <folder> [--name <name>] [--device <device>]
//! accordion-sync run [--once] [--allow-deletes] [--bring-back]
//! accordion-sync list
//! accordion-sync remove <name>
//! ```
//!
//! Accounts are kept in `~/.config/accordion-sync/accounts.json`, readable
//! only by you, since each one holds a session token. What each folder
//! last agreed with its server is in `~/.local/share/accordion-sync`.
//! `ACCORDION_SYNC_HOME` puts both in one folder instead, such as for tests.
//!
//! Each account reaches its server directly, or through its own tunnel when
//! the server has remote access on, whichever answers (decision 23).
//! `ACCORDION_SYNC_VIA=tunnel` uses only the tunnel, to try it at home, and
//! `ACCORDION_SYNC_VIA=direct` never uses it.

use std::collections::HashSet;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use accordion_sync::add::add as add_account;
use accordion_sync::tunnel::{self, TunnelSetup};
use accordion_sync::{Client, Engine, Error, Reach, State, Via};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

const USAGE: &str = "\
Usage:
  accordion-sync add [<server>] <folder> [--name <name>] [--device <device>]
      Adds an account. Make a code in Accordion's Settings, under Apps and
      devices, and enter it when asked. Away from home, copy the whole
      setup under the QR code instead. <server> is its address, such as
      https://ourhome.dedyn.io, and isn't needed with the whole setup.
  accordion-sync run [--once] [--allow-deletes] [--bring-back]
      Keeps every account in sync until you stop it with Ctrl+C. --once
      syncs once and stops. If you delete many files at once, Accordion
      waits: --allow-deletes moves them to Accordion's trash too, and
      --bring-back downloads them again instead.
  accordion-sync list
      Shows each account.
  accordion-sync remove <name>
      Stops syncing an account. Its folder and files stay.";

/// How often to look for changes on this computer, and on the server if
/// live updates aren't coming through.
const EVERY: Duration = Duration::from_secs(5);
/// How long to wait before trying an unreachable server again.
const RETRY: Duration = Duration::from_secs(30);

#[derive(Clone, Serialize, Deserialize)]
struct Account {
    name: String,
    server: String,
    folder: PathBuf,
    device: String,
    username: String,
    token: String,
    /// The account's own tunnel, once the server has remote access on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tunnel: Option<TunnelSetup>,
}

#[derive(Default, Serialize, Deserialize)]
struct Accounts {
    accounts: Vec<Account>,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let runtime = tokio::runtime::Runtime::new().expect("the async runtime starts");
    let result = runtime.block_on(async {
        match args.first().map(String::as_str) {
            Some("add") => add(&args[1..]).await,
            Some("run") => run(&args[1..]).await,
            Some("list") => list(),
            Some("remove") => remove(&args[1..]),
            _ => Err(USAGE.to_owned()),
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

async fn add(args: &[String]) -> Result<(), String> {
    let mut positional = Vec::new();
    let mut name = None;
    let mut device = None;
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--name" => name = rest.next().cloned(),
            "--device" => device = rest.next().cloned(),
            _ => positional.push(arg.clone()),
        }
    }
    let (server, folder) = match positional.as_slice() {
        [server, folder] => (Some(server.as_str()), folder),
        [folder] => (None, folder),
        _ => return Err(USAGE.to_owned()),
    };
    let mut accounts = load()?;
    let folder = absolute(Path::new(folder))?;
    if accounts
        .accounts
        .iter()
        .any(|account| account.folder == folder)
    {
        return Err(format!(
            "{} already syncs another account. Choose a different folder.",
            folder.display()
        ));
    }
    let device = device.unwrap_or_else(device_name);
    let code = match std::env::var("ACCORDION_SYNC_CODE") {
        Ok(code) => code,
        Err(_) => {
            print!("Code from Settings, under Apps and devices: ");
            io::stdout().flush().ok();
            let mut line = String::new();
            io::stdin()
                .lock()
                .read_line(&mut line)
                .map_err(|err| format!("Couldn't read the code: {err}"))?;
            line
        }
    };
    let added = add_account(
        code.trim(),
        server,
        &device,
        Reach {
            only: only_via()?,
            ..Reach::default()
        },
    )
    .await
    .map_err(|err| err.to_string())?;
    let name = name.unwrap_or_else(|| host_of(&added.server));
    if accounts.accounts.iter().any(|account| account.name == name) {
        return Err(format!(
            "There's already an account named {name}. Choose another with --name, then add it \
             again with a new code."
        ));
    }
    std::fs::create_dir_all(&folder)
        .map_err(|err| format!("Couldn't make {}: {err}", folder.display()))?;
    let away = if added.tunnel.is_some() {
        " It reaches the server away from home too."
    } else {
        ""
    };
    if let Some(problem) = &added.signed_in.tunnel_problem {
        println!("This computer can sync at home, but not away from home yet: {problem}");
    }
    accounts.accounts.push(Account {
        name: name.clone(),
        server: added.server.clone(),
        folder: folder.clone(),
        device,
        username: added.signed_in.username,
        token: added.signed_in.token,
        tunnel: added.tunnel,
    });
    save(&accounts)?;
    println!(
        "Added {name}: {} as {}, in {}.{away} Run `accordion-sync run` to start syncing.",
        added.server,
        added.signed_in.display_name,
        folder.display()
    );
    Ok(())
}

/// The way `ACCORDION_SYNC_VIA` asks for, if it's set.
fn only_via() -> Result<Option<Via>, String> {
    match std::env::var("ACCORDION_SYNC_VIA").as_deref() {
        Err(_) | Ok("") => Ok(None),
        Ok("tunnel") => Ok(Some(Via::Tunnel)),
        Ok("direct") => Ok(Some(Via::Direct)),
        Ok(other) => Err(format!(
            "ACCORDION_SYNC_VIA is \"{other}\". Set it to tunnel or direct, or leave it unset."
        )),
    }
}

/// Registers the account's tunnel key again, so its setup follows the
/// server's address and port, or registers a new key if remote access was
/// turned on since. Returns the account's tunnel, which is unchanged if the
/// server can't be reached or remote access is off.
async fn refresh_tunnel(account: &Account, only: Option<Via>) -> Option<TunnelSetup> {
    let client = Client::with_reach(
        &account.server,
        Some(account.token.clone()),
        Reach {
            tunnel: account.tunnel.clone(),
            only,
            ..Reach::default()
        },
    )
    .ok()?;
    let private_key = account
        .tunnel
        .as_ref()
        .map_or_else(|| tunnel::new_key().0, |tunnel| tunnel.private_key.clone());
    let public_key = tunnel::public_key(&private_key)?;
    match client.register_tunnel(&public_key).await {
        Ok(reply) => Some(reply.with_key(private_key)),
        Err(_) => account.tunnel.clone(),
    }
}

fn list() -> Result<(), String> {
    let accounts = load()?;
    if accounts.accounts.is_empty() {
        println!("No accounts yet. Add one with `accordion-sync add`.");
    }
    for account in accounts.accounts {
        println!(
            "{}: {} as {}, in {}",
            account.name,
            account.server,
            account.username,
            account.folder.display()
        );
    }
    Ok(())
}

fn remove(args: &[String]) -> Result<(), String> {
    let [name] = args else {
        return Err(USAGE.to_owned());
    };
    let mut accounts = load()?;
    let before = accounts.accounts.len();
    accounts.accounts.retain(|account| account.name != *name);
    if accounts.accounts.len() == before {
        return Err(format!("There's no account named {name}."));
    }
    save(&accounts)?;
    let _ = std::fs::remove_file(state_path(name)?);
    println!(
        "Stopped syncing {name}. Its folder and files are still here. To sign this device out, \
         sign it out in Accordion's Settings."
    );
    Ok(())
}

async fn run(args: &[String]) -> Result<(), String> {
    let once = args.iter().any(|arg| arg == "--once");
    let allow_deletes = args.iter().any(|arg| arg == "--allow-deletes");
    let bring_back = args.iter().any(|arg| arg == "--bring-back");
    let accounts = load()?.accounts;
    if accounts.is_empty() {
        return Err("No accounts yet. Add one with `accordion-sync add`.".into());
    }
    let only = only_via()?;
    // Each account's tunnel follows the server's setup, as of now.
    let mut refreshed = Vec::new();
    let mut changed = false;
    for mut account in accounts {
        let tunnel = refresh_tunnel(&account, only).await;
        changed |= tunnel != account.tunnel;
        account.tunnel = tunnel;
        refreshed.push(account);
    }
    if changed {
        save(&Accounts {
            accounts: refreshed.clone(),
        })?;
    }
    let mut tasks = tokio::task::JoinSet::new();
    for account in refreshed {
        let client = Client::with_reach(
            &account.server,
            Some(account.token.clone()),
            Reach {
                tunnel: account.tunnel.clone(),
                only,
                ..Reach::default()
            },
        )
        .map_err(|err| err.to_string())?;
        let state = State::open(&state_path(&account.name)?)
            .map_err(|err| format!("Couldn't open the sync state for {}: {err}", account.name))?;
        let mut engine = Engine::new(
            client.clone(),
            state,
            account.folder.clone(),
            account.device.clone(),
        );
        engine.allow_deletes = allow_deletes;
        if bring_back {
            let count = engine.bring_back().map_err(|err| err.to_string())?;
            println!("{}: bringing back {count} items.", account.name);
        }
        tasks.spawn(keep_in_sync(account.name, engine, client, once));
    }
    if once {
        while tasks.join_next().await.is_some() {}
        return Ok(());
    }
    tokio::select! {
        _ = tokio::signal::ctrl_c() => println!("Stopped."),
        _ = async { while tasks.join_next().await.is_some() {} } => {}
    }
    Ok(())
}

/// Syncs one account until it's stopped, as soon as live updates say
/// something changed, and every few seconds for this computer's changes.
async fn keep_in_sync(name: String, mut engine: Engine, client: Client, once: bool) {
    let changed = Arc::new(Notify::new());
    if !once {
        tokio::spawn(listen(client.clone(), Arc::clone(&changed)));
    }
    let mut shown: HashSet<String> = HashSet::new();
    let mut unreachable = false;
    let mut via = None;
    loop {
        match engine.sync().await {
            Ok(report) => {
                if unreachable {
                    println!("{name}: connected again.");
                    unreachable = false;
                }
                if client.via() != via {
                    via = client.via();
                    match via {
                        Some(Via::Tunnel) => {
                            println!("{name}: reaching the server through its tunnel.")
                        }
                        Some(Via::Direct) if client.has_tunnel() => {
                            println!("{name}: reaching the server directly.");
                        }
                        _ => {}
                    }
                }
                for line in &report.done {
                    println!("{name}: {line}");
                }
                // A problem that stays is shown once.
                let notices: HashSet<String> = report.notices.iter().cloned().collect();
                for notice in report
                    .notices
                    .iter()
                    .filter(|notice| !shown.contains(*notice))
                {
                    println!("{name}: {notice}");
                }
                shown = notices;
                if report.paused_deletes > 0 && !shown.contains("deletes") {
                    println!(
                        "{name}: You deleted {} items here, so Accordion is waiting before it \
                         deletes them there too. Nothing in Accordion was deleted. To move them \
                         to Accordion's trash, run `accordion-sync run --allow-deletes`. To get \
                         them back here, run `accordion-sync run --bring-back`.",
                        report.paused_deletes
                    );
                    shown.insert("deletes".into());
                }
            }
            Err(Error::SignedOut) => {
                println!("{name}: {}", Error::SignedOut);
                return;
            }
            Err(err) if err.is_temporary() => {
                if !unreachable {
                    let away = if client.has_tunnel() {
                        " If you're away from home, the network you're on may be blocking \
                         Accordion's tunnel."
                    } else {
                        ""
                    };
                    println!("{name}: {err}{away} Accordion tries again every 30 seconds.");
                    unreachable = true;
                }
                if once {
                    return;
                }
                tokio::time::sleep(RETRY).await;
                continue;
            }
            Err(err) => println!("{name}: {err}"),
        }
        if once {
            return;
        }
        let _ = tokio::time::timeout(EVERY, changed.notified()).await;
    }
}

/// Wakes the account's sync when live updates say Drive changed. If the
/// stream drops, it opens it again.
async fn listen(client: Client, changed: Arc<Notify>) {
    loop {
        if let Ok(mut body) = client.live().await {
            while let Some(Ok(frame)) = body.frame().await {
                if let Ok(data) = frame.into_data()
                    && data.windows(6).any(|window| window == b"drive.")
                {
                    changed.notify_one();
                }
            }
        }
        tokio::time::sleep(EVERY).await;
    }
}

fn home() -> Result<(PathBuf, PathBuf), String> {
    if let Some(home) = std::env::var_os("ACCORDION_SYNC_HOME") {
        let home = PathBuf::from(home);
        return Ok((home.clone(), home));
    }
    let user = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or("HOME isn't set.")?;
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map_or_else(|| user.join(".config"), PathBuf::from)
        .join("accordion-sync");
    let data = std::env::var_os("XDG_DATA_HOME")
        .map_or_else(|| user.join(".local/share"), PathBuf::from)
        .join("accordion-sync");
    Ok((config, data))
}

fn accounts_path() -> Result<PathBuf, String> {
    Ok(home()?.0.join("accounts.json"))
}

fn state_path(name: &str) -> Result<PathBuf, String> {
    let dir = home()?.1;
    std::fs::create_dir_all(&dir)
        .map_err(|err| format!("Couldn't make {}: {err}", dir.display()))?;
    Ok(dir.join(format!("{name}.db")))
}

fn load() -> Result<Accounts, String> {
    let path = accounts_path()?;
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|err| format!("{} isn't readable: {err}", path.display())),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Accounts::default()),
        Err(err) => Err(format!("Couldn't read {}: {err}", path.display())),
    }
}

/// Writes the accounts readable only by you, since they hold session
/// tokens.
fn save(accounts: &Accounts) -> Result<(), String> {
    let path = accounts_path()?;
    let dir = path.parent().expect("the file is in a folder");
    std::fs::create_dir_all(dir)
        .map_err(|err| format!("Couldn't make {}: {err}", dir.display()))?;
    let temp = dir.join("accounts.json.new");
    let json = serde_json::to_vec_pretty(accounts).expect("accounts serialize");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .map_err(|err| format!("Couldn't save the accounts: {err}"))?;
    file.write_all(&json)
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::rename(&temp, &path))
        .map_err(|err| format!("Couldn't save the accounts: {err}"))
}

fn absolute(path: &Path) -> Result<PathBuf, String> {
    std::path::absolute(path).map_err(|err| format!("{} isn't a folder: {err}", path.display()))
}

/// The server's host name, such as `ourhome.dedyn.io`, for the account's
/// name.
fn host_of(origin: &str) -> String {
    origin
        .split("://")
        .nth(1)
        .unwrap_or(origin)
        .split(['/', ':'])
        .next()
        .unwrap_or("accordion")
        .to_owned()
}

fn device_name() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "Linux computer".to_owned())
}
