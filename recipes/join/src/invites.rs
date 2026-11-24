//! Invite store: DATA_DIR/invites.json, guarded by an exclusive lock on DATA_DIR/.lock for every read-modify-write
//! (the CLI and the server are separate processes). Only SHA-256(code) is stored; the code itself is shown once.

use std::{
    fs::{self, File, OpenOptions},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize, Clone)]
pub struct Invite {
    /// Short public id (first 8 hex chars of the hash) for list/revoke
    pub id: String,
    pub hash: String,
    pub note: String,
    pub created: u64,
    pub expires: u64,
    pub max_uses: u32,
    pub uses: u32,
    #[serde(default)]
    pub revoked: bool,
}

impl Invite {
    pub fn usable(&self, now: u64) -> bool {
        !self.revoked && now < self.expires && self.uses < self.max_uses
    }
}

pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn hash(code: &str) -> String {
    Sha256::digest(code.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// Runs `f` on the invite list under the lock and saves the result atomically.
pub fn with_store<T>(dir: &str, f: impl FnOnce(&mut Vec<Invite>) -> anyhow::Result<T>) -> anyhow::Result<T> {
    let lock = OpenOptions::new().create(true).truncate(false).write(true).open(Path::new(dir).join(".lock")).context("opening lock file")?;
    lock.lock().context("locking invite store")?;
    let path = Path::new(dir).join("invites.json");
    let mut list: Vec<Invite> = match fs::read(&path) {
        Ok(b) => serde_json::from_slice(&b).context("invites.json is corrupt")?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e).context("reading invites.json"),
    };
    let out = f(&mut list)?;
    let tmp = Path::new(dir).join("invites.json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(&list)?).context("writing invites.json")?;
    File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, &path).context("replacing invites.json")?;
    Ok(out)
}

fn arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).map(String::as_str)
}

pub fn cli_new(dir: &str, public_url: &str, args: &[String]) -> anyhow::Result<()> {
    let days: u64 = arg(args, "--days").unwrap_or("7").parse().context("--days must be a number")?;
    let uses: u32 = arg(args, "--uses").unwrap_or("10").parse().context("--uses must be a number")?;
    let note = arg(args, "--note").unwrap_or("").chars().take(100).collect::<String>();
    if !(1..=30).contains(&days) || !(1..=100).contains(&uses) {
        bail!("--days must be 1-30 and --uses 1-100");
    }
    let mut buf = [0u8; 24];
    getrandom::fill(&mut buf).map_err(|e| anyhow::anyhow!("no randomness: {e}"))?;
    let code: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    let h = hash(&code);
    let inv = Invite { id: h[..8].to_owned(), hash: h, note, created: now(), expires: now() + days * 86400, max_uses: uses, uses: 0, revoked: false };
    let id = inv.id.clone();
    with_store(dir, |list| {
        list.push(inv);
        Ok(())
    })?;
    println!("invite {id}: {uses} sign-ups, expires in {days} days");
    println!("{public_url}/join/{code}");
    Ok(())
}

pub fn cli_list(dir: &str) -> anyhow::Result<()> {
    with_store(dir, |list| {
        let n = now();
        for i in list.iter() {
            let state = if i.revoked { "revoked" } else if n >= i.expires { "expired" } else if i.uses >= i.max_uses { "used up" } else { "active" };
            let left = i.expires.saturating_sub(n);
            println!("{}  {:8}  {}/{} used  {}d{}h left  {}", i.id, state, i.uses, i.max_uses, left / 86400, left % 86400 / 3600, i.note);
        }
        if list.is_empty() {
            println!("no invites");
        }
        Ok(())
    })
}

pub fn cli_revoke(dir: &str, id: &str) -> anyhow::Result<()> {
    if id.len() < 4 {
        bail!("usage: revoke <id> (from `list`)");
    }
    with_store(dir, |list| {
        let hits: Vec<&mut Invite> = list.iter_mut().filter(|i| i.id.starts_with(id)).collect();
        if hits.len() != 1 {
            bail!("{} invites match {id:?}", hits.len());
        }
        for i in hits {
            i.revoked = true;
            println!("revoked {}", i.id);
        }
        Ok(())
    })
}

pub enum Reserve {
    Ok { id: String, uses: u32, max_uses: u32 },
    Invalid,
}

/// Look up a code without changing anything.
pub fn peek(dir: &str, code: &str) -> anyhow::Result<Option<Invite>> {
    let h = hash(code);
    with_store(dir, |list| Ok(list.iter().find(|i| i.hash == h && i.usable(now())).cloned()))
}

/// Reserve one use of a code (before creating the account).
pub fn reserve(dir: &str, code: &str) -> anyhow::Result<Reserve> {
    let h = hash(code);
    with_store(dir, |list| match list.iter_mut().find(|i| i.hash == h && i.usable(now())) {
        Some(i) => {
            i.uses += 1;
            Ok(Reserve::Ok { id: i.id.clone(), uses: i.uses, max_uses: i.max_uses })
        }
        None => Ok(Reserve::Invalid),
    })
}

/// Give a reserved use back (account creation failed).
pub fn release(dir: &str, id: &str) -> anyhow::Result<()> {
    with_store(dir, |list| {
        if let Some(i) = list.iter_mut().find(|i| i.id == id) {
            i.uses = i.uses.saturating_sub(1);
        }
        Ok(())
    })
}
