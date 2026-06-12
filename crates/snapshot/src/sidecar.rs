// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bincode::Options;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tar::Archive;
use tokio::time::{Instant, sleep};
use tokio::{fs::File, io::AsyncWriteExt};
use cloudbreak_core::Result;
use zstd::Decoder;

use crate::accountsdb_helpers::{
    AccountsDbFields, DeserializableVersionedBank, MAX_STREAM_SIZE, SerializableAccountStorageEntry,
};

#[derive(Debug, Clone)]
pub struct SnapshotData {
    pub file_name: String,
    pub base_slot: Option<u64>,
    pub slot: u64,
    pub snapshot_type: SnapshotType,
    /// If there is a download url for the file, it would be preferred over the sidecar pair endpoint.
    pub download_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SnapshotPair {
    pub full_snapshot: SnapshotData,
    /// It will be None if the snapshot pair doesn't contain an incremental snapshot
    pub incremental_snapshot: Option<SnapshotData>,
    /// The Sidecar endpoint from which to download the snapshot
    pub downloading_endpoint: String,
}

impl SnapshotPair {
    pub fn parse(json_value: &serde_json::Value) -> Result<Self> {
        let slot = json_value
            .get("slot")
            .ok_or(anyhow::anyhow!("slot not found"))?
            .as_u64()
            .ok_or(anyhow::anyhow!("slot not found"))?;

        let base_slot = json_value
            .get("base_slot")
            .ok_or(anyhow::anyhow!("base_slot not found"))?
            .as_u64()
            .ok_or(anyhow::anyhow!("base_slot not found"))?;

        let sidecar_endpoint = json_value
            .get("target")
            .and_then(|value| value.as_str())
            .ok_or(anyhow::anyhow!("sidecar endpoint not found"))?;

        // 0. Incremental snapshot (check filename contains incremental)
        // 1. Full snapshot
        let mut files = json_value.get("files").unwrap().as_array().unwrap().iter();

        let first_file = Self::parse_file(files.next().unwrap())?;

        // If 1st file is incremental, we need to get the full snapshot file
        let (full_snapshot_file, incremental_snapshot_file) =
            if first_file.snapshot_type == SnapshotType::Incremental {
                let full_snapshot_file = Self::parse_file(files.next().unwrap())?;

                if full_snapshot_file.snapshot_type != SnapshotType::Full {
                    return Err(anyhow::anyhow!("full snapshot file is not a full snapshot"));
                }

                (full_snapshot_file, Some(first_file))
            } else {
                (first_file, None)
            };

        // Check that files slots are correct
        let snapshot_pair = SnapshotPair {
            full_snapshot: full_snapshot_file,
            incremental_snapshot: incremental_snapshot_file,
            downloading_endpoint: sidecar_endpoint.to_string(),
        };
        snapshot_pair.check_files_slots(slot, base_slot)?;

        Ok(snapshot_pair)
    }

    fn parse_file(file: &serde_json::Value) -> Result<SnapshotData> {
        let file_name = file
            .get("file_name")
            .ok_or(anyhow::anyhow!("file_name not found"))?
            .as_str()
            .ok_or(anyhow::anyhow!("file_name not found"))?
            .to_string();
        let slot = file
            .get("slot")
            .ok_or(anyhow::anyhow!("slot not found"))?
            .as_u64()
            .ok_or(anyhow::anyhow!("slot not found"))?;
        let base_slot = file.get("base_slot").and_then(|v| v.as_u64());
        let download_url = file
            .get("download_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let snapshot_type = if file_name.contains("incremental") {
            if base_slot.is_none() {
                return Err(anyhow::anyhow!(
                    "base slot not found for incremental snapshot"
                ));
            }

            SnapshotType::Incremental
        } else {
            SnapshotType::Full
        };

        Ok(SnapshotData {
            file_name,
            base_slot,
            slot,
            snapshot_type,
            download_url,
        })
    }

    /// It will check that each file slot data matches the root json item slot data
    fn check_files_slots(&self, slot: u64, base_slot: u64) -> Result<()> {
        let is_full_correct = self.full_snapshot.slot == base_slot;

        let is_incremental_correct = if let Some(incremental_snapshot) = &self.incremental_snapshot
        {
            incremental_snapshot.slot == slot
                && incremental_snapshot.base_slot.ok_or(anyhow::anyhow!(
                    "base slot not found for incremental snapshot"
                ))? == base_slot
        } else {
            true
        };

        if !is_full_correct || !is_incremental_correct {
            return Err(anyhow::anyhow!("files slots do not match"));
        }

        Ok(())
    }

    /// Checks if the target slot is covered by the snapshot pair.
    /// If there is an incremental snapshot, it will also check that full and incremental base slots match.
    pub fn check_target_slot(&self, target_slot: u64) -> Result<bool> {
        let mut snapshot_covered_slot = self.full_snapshot.slot;

        if let Some(incremental_snapshot) = &self.incremental_snapshot {
            let incremental_base_slot = incremental_snapshot.base_slot.ok_or(anyhow::anyhow!(
                "base slot not found for incremental snapshot"
            ))?;

            if incremental_base_slot != self.full_snapshot.slot {
                return Err(anyhow::anyhow!(
                    "incremental snapshot base slot does not match full snapshot slot"
                ));
            }
            snapshot_covered_slot = incremental_snapshot.slot;
        }

        let is_covered = snapshot_covered_slot >= target_slot;

        Ok(is_covered)
    }
}

const RETRY_WAIT_SECS: Duration = Duration::from_secs(10);

/// Minimum interval between "no covering snapshot" warnings while polling the tracker.
const NO_COVERAGE_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Base directory where a snapshot for `slot` is downloaded and unpacked (e.g. `./snapshot_123`).
pub fn snapshot_base_dir(slot: u64) -> PathBuf {
    PathBuf::from(format!("./snapshot_{}", slot))
}

/// Like [`snapshot_base_dir`] but suffixed with a millisecond timestamp (e.g.
/// `./snapshot_123_1700000000000`). Used by self-healing gap fills so concurrent/sequential
/// downloads for the same slot never collide on disk.
pub fn snapshot_base_dir_timestamped(slot: u64) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    PathBuf::from(format!("./snapshot_{}_{}", slot, timestamp))
}

/// Returns what are the correct snapshots to be downloaded based on the received slot and sidecar available snapshots
/// If target_slot is not provided, it will return the latest available full and incremental snapshot pair
///
/// It will block until the snapshots required are available
///
/// `force_returned_incremental` will only return a pair that contains also an incremental snapshot
pub async fn get_snapshot_data(
    tracker_endpoint: &str,
    target_slot: Option<u64>,
    save_to_file: bool,
    force_returned_incremental: bool,
) -> Result<SnapshotPair> {
    let client = reqwest::Client::new();
    let mut last_no_coverage_log: Option<Instant> = None;

    loop {
        let response = client
            .get(format!("{}/v1/snapshots", tracker_endpoint))
            .send()
            .await?;

        let json_value: serde_json::Value = response.json().await?;

        if save_to_file {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();

            let file_path = format!("./tracker_responses/{}.json", timestamp);

            if let Some(parent) = Path::new(&file_path).parent() {
                tokio::fs::create_dir_all(parent).await?;
            }

            let mut file = File::create(&file_path).await?;
            let pretty_json = serde_json::to_string_pretty(&json_value)?;

            file.write_all(pretty_json.as_bytes()).await?;
        }

        // Highest slot covered by any pair offered by the tracker (incremental slot if present,
        // full slot otherwise), used to log how far behind the tracker is when nothing covers
        // the target slot.
        let mut highest_available_slot: Option<u64> = None;

        for snapshot in json_value.as_array().unwrap() {
            let snapshot_pair = SnapshotPair::parse(snapshot)?;

            let pair_covered_slot = snapshot_pair
                .incremental_snapshot
                .as_ref()
                .map(|incremental| incremental.slot)
                .unwrap_or(snapshot_pair.full_snapshot.slot);
            highest_available_slot = Some(
                highest_available_slot.map_or(pair_covered_slot, |slot| slot.max(pair_covered_slot)),
            );

            let is_covered = if let Some(target_slot) = target_slot {
                snapshot_pair.check_target_slot(target_slot)?
            } else {
                true
            };

            // If incremental is required, we need to check that the snapshot pair contains an incremental snapshot
            let is_incremental_flag_satisfied = if force_returned_incremental {
                snapshot_pair.incremental_snapshot.is_some()
            } else {
                true
            };

            if is_covered && is_incremental_flag_satisfied {
                return Ok(snapshot_pair);
            }
        }

        let should_log = last_no_coverage_log
            .is_none_or(|last| last.elapsed() >= NO_COVERAGE_LOG_INTERVAL);
        if should_log {
            tracing::warn!(
                target: "get_snapshot_data",
                "No covering snapshot available from tracker yet - highest available slot: {:?} - target slot: {:?} - retrying every {}s",
                highest_available_slot,
                target_slot,
                RETRY_WAIT_SECS.as_secs()
            );
            last_no_coverage_log = Some(Instant::now());
        }

        sleep(RETRY_WAIT_SECS).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotType {
    Full,
    Incremental,
}

/// Downloads the snapshot file from the sidecar
///
/// It will prefer the download url if it is available, otherwise it will use the sidecar endpoint.
pub async fn download_snapshot_file(
    sidecar_endpoint: &str,
    snapshot_data: SnapshotData,
    snapshot_type: SnapshotType,
    base_dir: &Path,
) -> Result<()> {
    let url = if let Some(download_url) = snapshot_data.download_url {
        download_url
    } else {
        format!(
            "{}/v1/snapshot/{}",
            sidecar_endpoint, snapshot_data.file_name
        )
    };

    let client = reqwest::Client::new();
    let start_time = tokio::time::Instant::now();
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to download file: HTTP {}",
            response.status()
        ));
    }

    let total_size = response.content_length().unwrap_or(0);
    tracing::info!(
        target: "download_snapshot_file",
        "Starting to download file {} of size: {} MB ({:?}) from endpoint: {}",
        snapshot_data.file_name,
        total_size / 1024 / 1024,
        snapshot_type,
        sidecar_endpoint,
    );

    let file_path = base_dir.join(&snapshot_data.file_name);

    // Create the directory if it doesn't exist
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(&file_path).await?;
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    let mut last_log_time = tokio::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;

        if total_size > 0 && last_log_time.elapsed().as_secs() > 10 {
            let progress = (downloaded as f64 / total_size as f64) * 100.0;
            tracing::info!(
                target: "snapshot_download_progress",
                "Progress: {:.1}% ({}/{}) - {} seconds - {:.1} MB/s",
                progress,
                downloaded / 1024 / 1024,
                total_size / 1024 / 1024,
                start_time.elapsed().as_secs_f64(),
                downloaded as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
            );
            last_log_time = tokio::time::Instant::now();
        }
    }

    file.flush().await?;
    tracing::info!(
        target: "download_snapshot_file",
        "File {} downloaded successfully in {} secs ({:?})",
        snapshot_data.file_name,
        start_time.elapsed().as_secs_f64(),
        snapshot_type
    );

    Ok(())
}

pub fn unpack_compressed_snapshot<P: Into<PathBuf>>(
    path: P,
    base_dir: &Path,
    slot: u64,
) -> Result<Vec<AccountFileData>> {
    let start_time = Instant::now();
    let path_buf: PathBuf = path.into();

    let temp_dir = base_dir.join("uncompressed_snapshot");

    let file = std::fs::File::open(path_buf)?;

    let decoder = Decoder::new(file)?;

    let mut archive = Archive::new(decoder);
    archive.unpack(temp_dir.clone())?;

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "unpack_compressed_snapshot", "Unpacked compressed snapshot in {} seconds", elapsed);

    let version_path = temp_dir.join("version");
    let _version = std::fs::read_to_string(version_path)?.trim().to_string();

    // Deserializing the snapshot metadata file
    let snapshots_dir = temp_dir.join("snapshots");
    let snapshot_file_name = format!("{}/{}", slot, slot);
    let snapshot_file = std::fs::File::open(snapshots_dir.join(snapshot_file_name))?;

    let mut snapshot_stream = std::io::BufReader::new(snapshot_file);

    let bank_fields: DeserializableVersionedBank = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)?;

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized DeserializableVersionedBank in {} seconds", elapsed);

    let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)
        .unwrap();

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized AccountsDbFields Vec in {} seconds", elapsed);

    let AccountsDbFields(accounts_metadata, _, accountsdb_fields_slot, ..) = accounts_db_fields;

    assert_eq!(slot, accountsdb_fields_slot);
    assert_eq!(slot, bank_fields.slot);

    // Deserializing the accounts directory files
    let accounts_dir = temp_dir.join("accounts");

    let mut account_file_data = Vec::new();

    for entry in std::fs::read_dir(accounts_dir)?.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        let file_size = std::fs::metadata(&path)?.len() as usize;
        let file_name = entry.file_name().to_string_lossy().to_string();

        let (slot_str, id_str) = file_name
            .split_once('.')
            .ok_or(anyhow::anyhow!("Invalid file name: {}", file_name))?;
        let slot = slot_str.parse::<u64>()?;
        let id = id_str.parse::<u64>()?;

        let accounts_metadata = match accounts_metadata.get(&slot) {
            Some(accounts_metadata) => accounts_metadata,
            None => {
                tracing::error!(
                    "accounts_metadata not found for slot: {} - file_size: {} - write_version: {}",
                    slot,
                    file_size,
                    id
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        let mut size = None;
        for account in accounts_metadata {
            if account.id as u64 == id {
                size = Some(account.accounts_current_len);
                break;
            }
        }
        let size = match size {
            Some(size) => size,
            None => {
                tracing::error!(
                    "size not found for write version: {} and slot: {} - file_size: {} - accounts_metadata: {:?}",
                    id,
                    slot,
                    file_size,
                    accounts_metadata
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        if size != file_size {
            tracing::warn!("size mismatch for id: {} and slot: {}", id, slot);
        }

        account_file_data.push(AccountFileData {
            path,
            size,
            slot,
            write_version: id,
        });
    }

    let elapsed = start_time.elapsed().as_secs_f64() - elapsed;
    tracing::info!(target: "unpack_compressed_snapshot", "Deserialized accounts directory metadata in {} seconds", elapsed);

    let elapsed = start_time.elapsed().as_secs_f64();
    tracing::info!(target: "unpack_compressed_snapshot", "Total unpacking time: {} seconds", elapsed);

    Ok(account_file_data)
}

pub struct AccountFileData {
    pub path: PathBuf,
    pub size: usize,
    pub slot: u64,
    pub write_version: u64,
}

/// If for some reason we don't file the account file we are looking for (or the write version doesn't match)
///  we use the file name for getting the write version and the file size for the default account file data
fn return_default_account_file_data(
    path: PathBuf,
    slot: u64,
    file_size: usize,
    write_version: u64,
) -> AccountFileData {
    AccountFileData {
        path,
        size: file_size,
        slot,
        write_version,
    }
}
