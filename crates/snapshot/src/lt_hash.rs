// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use anyhow::{Context, Result};
use agave_fs::FileInfo;
use solana_accounts_db::accounts_file::AccountsFile;
use solana_lattice_hash::lt_hash::LtHash;
use solana_pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};
use cloudbreak_core::AccountSelectorConfig;

use crate::sidecar::AccountFileData;

pub fn lt_hash_account(
    lamports: u64,
    data: &[u8],
    executable: bool,
    owner: &[u8],
    pubkey: &[u8],
) -> LtHash {
    if lamports == 0 {
        return LtHash::identity();
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(&lamports.to_le_bytes());
    hasher.update(data);
    hasher.update(&[executable as u8]);
    hasher.update(owner);
    hasher.update(pubkey);
    LtHash::with(&hasher)
}

fn for_each_deduplicated_snapshot_account(
    snapshot_files: &[AccountFileData],
    programs: &AccountSelectorConfig,
    mut on_account: impl FnMut([u8; 32], LtHash),
) -> Result<()> {
    let mut version_map: HashMap<[u8; 32], (u64, u64)> = HashMap::new();

    // When `include` is non-empty the first pass narrows to that set, keeping
    // version_map bounded. Otherwise we can't safely narrow here.
    let first_pass_filter = !programs.include.is_empty();

    let total_files = snapshot_files.len();
    let log_every = (total_files / 10).max(1);

    for (i, file_data) in snapshot_files.iter().enumerate() {
        if i > 0 && i % log_every == 0 {
            println!(
                "  first pass: {}/{} files ({:.0}%), {} unique pubkeys tracked",
                i,
                total_files,
                (i as f64 / total_files as f64) * 100.0,
                version_map.len()
            );
        }

        let accounts_file = AccountsFile::new_for_startup(
            FileInfo::new_from_path(&file_data.path).map_err(|e| {
                anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
            })?,
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
        })?;

        let mut offsets = Vec::new();
        accounts_file
            .scan_accounts_without_data(|offset, _| offsets.push(offset))
            .map_err(|e| anyhow::anyhow!("Failed to scan account file: {:?}", e))?;

        for offset in offsets {
            accounts_file.get_stored_account_callback(offset, |account| {
                if first_pass_filter
                    && !programs.is_program_selected(&Pubkey::from(account.owner.to_bytes()))
                {
                    return;
                }
                let pubkey_bytes = account.pubkey().to_bytes();
                let entry = version_map.entry(pubkey_bytes).or_insert((0, 0));
                if (file_data.slot, file_data.write_version) > *entry {
                    *entry = (file_data.slot, file_data.write_version);
                }
            });
        }
    }
    println!(
        "  first pass done: {} unique pubkeys tracked",
        version_map.len()
    );

    for (i, file_data) in snapshot_files.iter().enumerate() {
        if i > 0 && i % log_every == 0 {
            println!(
                "  second pass: {}/{} files ({:.0}%)",
                i,
                total_files,
                (i as f64 / total_files as f64) * 100.0
            );
        }

        let accounts_file = AccountsFile::new_for_startup(
            FileInfo::new_from_path(&file_data.path).map_err(|e| {
                anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
            })?,
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
        })?;

        let mut offsets = Vec::new();
        accounts_file
            .scan_accounts_without_data(|offset, _| offsets.push(offset))
            .map_err(|e| anyhow::anyhow!("Failed to scan account file: {:?}", e))?;

        for offset in offsets {
            accounts_file.get_stored_account_callback(offset, |account| {
                let pubkey_bytes = account.pubkey().to_bytes();

                if let Some(&(winning_slot, winning_wv)) = version_map.get(&pubkey_bytes)
                    && (file_data.slot != winning_slot || file_data.write_version != winning_wv)
                {
                    return;
                }

                let owner_pubkey = Pubkey::from(account.owner.to_bytes());
                if !programs.is_program_selected(&owner_pubkey) {
                    return;
                }

                if account.lamports == 0 {
                    return;
                }

                let hash = lt_hash_account(
                    account.lamports,
                    account.data,
                    account.executable,
                    &account.owner.to_bytes(),
                    &pubkey_bytes,
                );
                on_account(pubkey_bytes, hash);
            });
        }
    }

    Ok(())
}

pub fn compute_snapshot_lt_hash(
    snapshot_files: &[AccountFileData],
    programs: &AccountSelectorConfig,
) -> Result<(LtHash, usize)> {
    let mut aggregate = LtHash::identity();
    let mut count = 0usize;

    for_each_deduplicated_snapshot_account(snapshot_files, programs, |_, hash| {
        aggregate.mix_in(&hash);
        count += 1;
    })?;

    Ok((aggregate, count))
}

// Single-pass variant. In exclude mode the hash covers the excluded set (to be
// subtracted from the metadata total); the returned count is always the kept set.
pub fn compute_snapshot_lt_hash_filtered_single_pass(
    snapshot_files: &[AccountFileData],
    programs: &AccountSelectorConfig,
) -> Result<(LtHash, usize)> {
    let exclude_mode = programs.include.is_empty();
    anyhow::ensure!(
        !exclude_mode || !programs.exclude.is_empty(),
        "compute_snapshot_lt_hash_filtered_single_pass requires a non-empty selector"
    );

    // Process files newest-first so the first occurrence of each pubkey wins.
    let mut order: Vec<usize> = (0..snapshot_files.len()).collect();
    order.sort_by(|&a, &b| {
        (snapshot_files[b].slot, snapshot_files[b].write_version)
            .cmp(&(snapshot_files[a].slot, snapshot_files[a].write_version))
    });

    let total_files = snapshot_files.len();
    let log_every = (total_files / 10).max(1);
    let mut seen: HashSet<[u8; 32]> = HashSet::new();
    let mut aggregate = LtHash::identity();
    let mut count = 0usize;
    let mut live_total = 0usize;

    for (i, &idx) in order.iter().enumerate() {
        let file_data = &snapshot_files[idx];
        if i > 0 && i % log_every == 0 {
            println!(
                "  single pass: {}/{} files ({:.0}%), {} unique pubkeys seen",
                i,
                total_files,
                (i as f64 / total_files as f64) * 100.0,
                seen.len()
            );
        }

        let accounts_file = AccountsFile::new_for_startup(
            FileInfo::new_from_path(&file_data.path).map_err(|e| {
                anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
            })?,
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to open account file {:?}: {:?}", file_data.path, e)
        })?;

        let mut offsets = Vec::new();
        accounts_file
            .scan_accounts_without_data(|offset, _| offsets.push(offset))
            .map_err(|e| anyhow::anyhow!("Failed to scan account file: {:?}", e))?;

        for offset in offsets {
            accounts_file.get_stored_account_callback(offset, |account| {
                let pubkey = account.pubkey().to_bytes();
                if !seen.insert(pubkey) {
                    return;
                }
                if account.lamports == 0 {
                    return;
                }
                live_total += 1;
                if programs.is_program_selected(&Pubkey::from(account.owner.to_bytes()))
                    == exclude_mode
                {
                    return;
                }
                let hash = lt_hash_account(
                    account.lamports,
                    account.data,
                    account.executable,
                    &account.owner.to_bytes(),
                    &pubkey,
                );
                aggregate.mix_in(&hash);
                count += 1;
            });
        }
    }
    if exclude_mode {
        count = live_total.saturating_sub(count);
    }
    println!("  single pass done: {} unique pubkeys seen", seen.len());

    Ok((aggregate, count))
}

pub fn extract_lt_hash_from_snapshot_metadata(metadata_path: &Path) -> Result<LtHash> {
    let file_bytes = std::fs::read(metadata_path)
        .with_context(|| format!("Failed to read metadata file: {:?}", metadata_path))?;

    anyhow::ensure!(
        file_bytes.len() >= 2048,
        "metadata file too small ({} bytes): {:?}",
        file_bytes.len(),
        metadata_path
    );

    let lt_hash_bytes = &file_bytes[file_bytes.len() - 2048..];
    let mut lt_hash_array = [0u16; 1024];
    for (i, chunk) in lt_hash_bytes.chunks_exact(2).enumerate() {
        lt_hash_array[i] = u16::from_le_bytes([chunk[0], chunk[1]]);
    }
    Ok(LtHash(lt_hash_array))
}

pub fn compute_filtered_snapshot_lt_hash(
    snapshot_files: &[AccountFileData],
    snapshot_metadata_path: &Path,
    programs: &AccountSelectorConfig,
) -> Result<(LtHash, usize)> {
    if !programs.include.is_empty() {
        return compute_snapshot_lt_hash_filtered_single_pass(snapshot_files, programs);
    }

    let system_program_id = Pubkey::default();
    anyhow::ensure!(
        !programs.exclude.iter().any(|p| p.0 == system_program_id),
        "exclude path cannot contain the System program ({}) — its accounts can be reassigned",
        system_program_id
    );

    let mut total = extract_lt_hash_from_snapshot_metadata(snapshot_metadata_path)?;

    if programs.exclude.is_empty() {
        return Ok((total, 0));
    }

    let (excluded, count) =
        compute_snapshot_lt_hash_filtered_single_pass(snapshot_files, programs)?;
    total.mix_out(&excluded);
    Ok((total, count))
}
