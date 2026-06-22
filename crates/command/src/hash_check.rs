// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use clap::Parser;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use serde::Deserialize;
use solana_accounts_db::accounts_file::AccountsFile;
use solana_pubkey::Pubkey;
use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
};
use cloudbreak_core::{AccountSelectorConfig, DatabaseConfig, TryLoadConfig};
use cloudbreak_snapshot::sidecar::{self, AccountFileData};

#[derive(Deserialize, Debug)]
pub struct HashCheckConfig {
    #[serde(default)]
    pub database: Option<DatabaseConfig>,
    #[serde(default)]
    pub programs: AccountSelectorConfig,
}

impl TryLoadConfig for HashCheckConfig {}

#[derive(Parser, Debug)]
#[command(name = "snapshot-diff")]
pub struct DiffArgs {
    #[arg(short, long, default_value = "cloudbreak.index.toml")]
    config: String,
    #[arg(long)]
    target_slot: u64,
    #[arg(long)]
    full_snapshot: String,
    #[arg(long)]
    incremental_snapshot: Option<String>,
    /// Optional single prefix (hex, e.g. "0a1b") to only scan/compare that prefix.
    #[arg(long)]
    prefix: Option<String>,
    /// Optional file path; every MISMATCH / ONLY_IN_SNAPSHOT / ONLY_IN_DB line is
    /// appended here, uncapped. Stdout still shows only the first 50 of each kind.
    #[arg(long)]
    dump_mismatches: Option<PathBuf>,
    /// Keep the ./snapshot_diff_tmp/ directory at the end of the run (debugging).
    #[arg(long, default_value_t = false)]
    keep_tmp: bool,
}

pub async fn run_diff(args: &DiffArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    let config: HashCheckConfig = HashCheckConfig::try_load(&args.config)?;
    let db_cfg = config
        .database
        .context("snapshot-diff requires a [database] section in the config")?;
    let db = Database::connect(ConnectOptions::from(db_cfg)).await?;

    let only_prefix: Option<u16> = match &args.prefix {
        Some(s) => Some(
            u16::from_str_radix(s.trim_start_matches("0x"), 16)
                .with_context(|| format!("invalid prefix hex: {}", s))?,
        ),
        None => None,
    };
    if let Some(p) = only_prefix {
        println!("single-prefix mode: 0x{:04x}", p);
    }

    let mut dump_file: Option<std::fs::File> = args
        .dump_mismatches
        .as_ref()
        .map(|p| {
            std::fs::File::create(p).with_context(|| format!("Failed to open dump file {:?}", p))
        })
        .transpose()?;

    println!("unpacking snapshots...");
    let snapshot_files =
        unpack_snapshots(&args.full_snapshot, args.incremental_snapshot.as_deref())?;
    println!("unpacked {} files", snapshot_files.len());

    let tmp_dir = PathBuf::from("./snapshot_diff_tmp");
    std::fs::create_dir_all(&tmp_dir)?;

    println!("scanning snapshot, writing per-prefix files...");
    scan_to_prefix_files(&snapshot_files, &tmp_dir, only_prefix, &config.programs)?;

    println!("comparing against db...");
    let mut total_only_snap = 0u64;
    let mut total_only_db = 0u64;
    let mut total_mismatch = 0u64;
    let mut total_match = 0u64;

    let prefix_range: Box<dyn Iterator<Item = u32>> = match only_prefix {
        Some(p) => Box::new(std::iter::once(p as u32)),
        None => Box::new(0u32..=0xFFFF),
    };

    for prefix in prefix_range {
        let prefix = prefix as u16;
        let snap = load_prefix_deduped(&tmp_dir, prefix)?;
        let db_map = query_db_prefix(&db, args.target_slot, prefix, &config.programs).await?;

        for (
            pubkey,
            PrefixAccount {
                hash: snap_hash,
                slot: snap_slot,
                wv: snap_wv,
            },
        ) in &snap
        {
            match db_map.get(pubkey) {
                Some(db_hash) if db_hash == snap_hash => total_match += 1,
                Some(_) => {
                    total_mismatch += 1;
                    let line = format!(
                        "MISMATCH pubkey={} snap_slot={} snap_wv={}",
                        Pubkey::new_from_array(*pubkey),
                        snap_slot,
                        snap_wv
                    );
                    if only_prefix.is_some() || total_mismatch <= 50 {
                        println!("{}", line);
                    }
                    if let Some(f) = dump_file.as_mut() {
                        let _ = writeln!(f, "{}", line);
                    }
                }
                None => {
                    total_only_snap += 1;
                    let line = format!(
                        "ONLY_IN_SNAPSHOT pubkey={} snap_slot={} snap_wv={}",
                        Pubkey::new_from_array(*pubkey),
                        snap_slot,
                        snap_wv
                    );
                    if only_prefix.is_some() || total_only_snap <= 50 {
                        println!("{}", line);
                    }
                    if let Some(f) = dump_file.as_mut() {
                        let _ = writeln!(f, "{}", line);
                    }
                }
            }
        }

        for pubkey in db_map.keys() {
            if !snap.contains_key(pubkey) {
                total_only_db += 1;
                let line = format!("ONLY_IN_DB pubkey={}", Pubkey::new_from_array(*pubkey));
                if only_prefix.is_some() || total_only_db <= 50 {
                    println!("{}", line);
                }
                if let Some(f) = dump_file.as_mut() {
                    let _ = writeln!(f, "{}", line);
                }
            }
        }

        if only_prefix.is_none() && ((prefix as u32 + 1).is_multiple_of(4096) || prefix == 0xFFFF) {
            println!(
                "progress: {}/65536 | match={} mismatch={} only_snap={} only_db={}",
                prefix as u32 + 1,
                total_match,
                total_mismatch,
                total_only_snap,
                total_only_db
            );
        }
    }

    println!("match: {}", total_match);
    println!("mismatch: {}", total_mismatch);
    println!("only_in_snapshot: {}", total_only_snap);
    println!("only_in_db: {}", total_only_db);

    if args.keep_tmp {
        println!("keeping tmp dir at {:?}", tmp_dir);
    } else if let Err(e) = std::fs::remove_dir_all(&tmp_dir) {
        tracing::warn!("Failed to remove {:?}: {}", tmp_dir, e);
    }
    Ok(())
}

fn slot_from_snapshot(path: &str) -> Result<u64> {
    let file =
        std::fs::File::open(path).with_context(|| format!("Failed to open snapshot: {}", path))?;
    let decoder = zstd::Decoder::new(file).context("Failed to create zstd decoder")?;
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries().context("Failed to read tar entries")? {
        let entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        if let Some(path_str) = entry_path.to_str()
            && let Some(rest) = path_str.strip_prefix("snapshots/")
        {
            let slot_str = rest.trim_end_matches('/').split('/').next().unwrap_or("");
            if let Ok(slot) = slot_str.parse::<u64>() {
                return Ok(slot);
            }
        }
    }

    anyhow::bail!("Could not find slot in snapshot archive: {}", path)
}

fn unpack_snapshots(
    full_snapshot: &str,
    incremental_snapshot: Option<&str>,
) -> Result<Vec<AccountFileData>> {
    let full_path = PathBuf::from(full_snapshot);
    anyhow::ensure!(
        full_path.exists(),
        "Full snapshot file not found: {}",
        full_snapshot
    );

    let full_slot = slot_from_snapshot(full_snapshot)?;
    let full_base_dir = sidecar::snapshot_base_dir(full_slot);
    let mut all_files =
        sidecar::unpack_compressed_snapshot(full_path, &full_base_dir, full_slot)?.account_files;

    if let Some(inc) = incremental_snapshot {
        let inc_path = PathBuf::from(inc);
        anyhow::ensure!(
            inc_path.exists(),
            "Incremental snapshot file not found: {}",
            inc
        );
        let inc_slot = slot_from_snapshot(inc)?;
        let inc_base_dir = sidecar::snapshot_base_dir(inc_slot);
        all_files.extend(
            sidecar::unpack_compressed_snapshot(inc_path, &inc_base_dir, inc_slot)?.account_files,
        );
    }

    Ok(all_files)
}

fn build_owner_filter(programs: &AccountSelectorConfig) -> String {
    if !programs.include.is_empty() {
        let owner_literals: Vec<String> = programs
            .include
            .iter()
            .map(|p| format!("'\\x{}'::bytea", hex::encode(p.0.to_bytes())))
            .collect();
        format!("AND owner IN ({})", owner_literals.join(", "))
    } else if !programs.exclude.is_empty() {
        let exclude_owners: Vec<String> = programs
            .exclude
            .iter()
            .map(|p| format!("'\\x{}'::bytea", hex::encode(p.0.to_bytes())))
            .collect();
        format!("AND owner NOT IN ({})", exclude_owners.join(", "))
    } else {
        String::new()
    }
}

fn blake3_account(
    lamports: u64,
    data: &[u8],
    executable: bool,
    owner: &[u8],
    pubkey: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&lamports.to_le_bytes());
    hasher.update(data);
    hasher.update(&[executable as u8]);
    hasher.update(owner);
    hasher.update(pubkey);
    *hasher.finalize().as_bytes()
}

fn scan_to_prefix_files(
    snapshot_files: &[AccountFileData],
    tmp_dir: &Path,
    only_prefix: Option<u16>,
    programs: &AccountSelectorConfig,
) -> Result<()> {
    let mut writers: HashMap<u16, std::io::BufWriter<std::fs::File>> = HashMap::new();
    let total = snapshot_files.len();
    let log_every = (total / 10).max(1);

    for (i, file_data) in snapshot_files.iter().enumerate() {
        if i % log_every == 0 && i > 0 {
            println!(
                "  {}/{} files ({:.0}%)",
                i,
                total,
                (i as f64 / total as f64) * 100.0
            );
        }

        let af = AccountsFile::new_for_startup(
            &file_data.path,
            file_data.size,
            solana_accounts_db::accounts_file::StorageAccess::default(),
        )
        .map_err(|e| anyhow::anyhow!("{:?}: {:?}", file_data.path, e))?;

        let mut offsets = Vec::new();
        af.scan_accounts_without_data(|offset, _| offsets.push(offset))
            .map_err(|e| anyhow::anyhow!("scan: {:?}", e))?;

        for offset in offsets {
            af.get_stored_account_callback(offset, |account| {
                let pubkey = account.pubkey().to_bytes();
                let pfx = u16::from_be_bytes([pubkey[0], pubkey[1]]);
                if let Some(target) = only_prefix
                    && pfx != target
                {
                    return;
                }
                if !programs.is_program_selected(account.owner) {
                    return;
                }
                let h = if account.lamports == 0 {
                    [0u8; 32]
                } else {
                    blake3_account(
                        account.lamports,
                        account.data,
                        account.executable,
                        &account.owner.to_bytes(),
                        &pubkey,
                    )
                };
                let w = writers.entry(pfx).or_insert_with(|| {
                    let p = tmp_dir.join(format!("{:04x}.bin", pfx));
                    std::io::BufWriter::new(std::fs::File::create(p).unwrap())
                });
                let _ = w.write_all(&pubkey);
                let _ = w.write_all(&file_data.slot.to_le_bytes());
                let _ = w.write_all(&file_data.write_version.to_le_bytes());
                let _ = w.write_all(&account.lamports.to_le_bytes());
                let _ = w.write_all(&h);
            });
        }
    }

    for (_, mut w) in writers {
        w.flush()?;
    }
    Ok(())
}

pub struct PrefixAccount {
    pub hash: [u8; 32],
    pub slot: u64,
    pub wv: u64,
}

fn load_prefix_deduped(tmp_dir: &Path, prefix: u16) -> Result<HashMap<[u8; 32], PrefixAccount>> {
    let path = tmp_dir.join(format!("{:04x}.bin", prefix));
    if !path.exists() {
        return Ok(HashMap::new());
    }

    // entry layout: pubkey(32) + slot(8) + wv(8) + lamports(8) + hash(32) = 88
    let mut deduped: HashMap<[u8; 32], ([u8; 32], u64, u64, u64)> = HashMap::new();
    let data = std::fs::read(&path)?;

    for chunk in data.chunks_exact(88) {
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&chunk[0..32]);
        let slot = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
        let wv = u64::from_le_bytes(chunk[40..48].try_into().unwrap());
        let lamports = u64::from_le_bytes(chunk[48..56].try_into().unwrap());
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&chunk[56..88]);

        match deduped.get(&pubkey) {
            Some(&(_, s, w, _)) if (s, w) >= (slot, wv) => {}
            _ => {
                deduped.insert(pubkey, (hash, slot, wv, lamports));
            }
        }
    }

    // Filter out closed accounts (lamports == 0 in the latest version)
    Ok(deduped
        .into_iter()
        .filter(|(_, (_, _, _, lamports))| *lamports > 0)
        .map(|(k, (h, s, w, _))| {
            (
                k,
                PrefixAccount {
                    hash: h,
                    slot: s,
                    wv: w,
                },
            )
        })
        .collect())
}

async fn query_db_prefix(
    db: &sea_orm::DatabaseConnection,
    target_slot: u64,
    prefix: u16,
    programs: &AccountSelectorConfig,
) -> Result<HashMap<[u8; 32], [u8; 32]>> {
    let lower = format!("'\\x{:04x}'::bytea", prefix);
    let upper = if prefix == 0xFFFF {
        String::new()
    } else {
        format!("AND pubkey < '\\x{:04x}'::bytea", prefix + 1)
    };
    let owner_filter = build_owner_filter(programs);

    let sql = format!(
        "SELECT DISTINCT ON (pubkey) pubkey, lamports, owner, executable, data
         FROM (
             SELECT pubkey, lamports, owner, executable, data, slot, write_version
             FROM accounts WHERE slot <= {slot} AND pubkey >= {lower} {upper} {owner_filter}
             UNION ALL
             SELECT pubkey, lamports, owner, executable, data, slot, write_version
             FROM snapshot_accounts WHERE slot <= {slot} AND pubkey >= {lower} {upper} {owner_filter}
         ) combined
         ORDER BY pubkey, slot DESC, write_version DESC",
        slot = target_slot,
        owner_filter = owner_filter,
    );

    let txn = sea_orm::TransactionTrait::begin(db).await?;
    txn.execute(Statement::from_string(
        DbBackend::Postgres,
        "SET LOCAL statement_timeout = '0'".to_string(),
    ))
    .await?;
    let rows = txn
        .query_all(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .with_context(|| format!("query prefix 0x{:04x}", prefix))?;
    txn.commit().await?;

    let mut map = HashMap::new();
    for row in &rows {
        let pubkey: Vec<u8> = row.try_get_by_index(0)?;
        let lamports: i64 = row.try_get_by_index(1)?;
        let owner: Vec<u8> = row.try_get_by_index(2)?;
        let executable: bool = row.try_get_by_index(3)?;
        let data: Vec<u8> = row.try_get_by_index(4)?;

        if lamports <= 0 {
            continue;
        }

        let h = blake3_account(lamports as u64, &data, executable, &owner, &pubkey);
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&pubkey);
        map.insert(pk, h);
    }
    Ok(map)
}
