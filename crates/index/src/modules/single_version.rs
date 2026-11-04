// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! Writes a block into the one-row-per-account `accounts` table in one transaction, so a reader
//! sees all of the block or none of it. A process-wide lock keeps live blocks and repairs apart.
//! Two upsert modes alternate by slot parity: `unnest` (array binds) and `copy` (binary COPY).

use std::time::Duration;

use futures::future::BoxFuture;
use sea_orm::sqlx::{
    self, Postgres, Transaction,
    postgres::{PgConnection, PgPool},
};
use tokio::sync::{Mutex, MutexGuard, OnceCell};
use tokio::time::{Instant, timeout};

use crate::metrics;

static BLOCK_TX_LOCK: Mutex<()> = Mutex::const_new(());

/// The `ON CONFLICT` target, read once from the `accounts` primary key.
static CONFLICT_TARGET: OnceCell<String> = OnceCell::const_new();

const STAGE_TABLE_SQL: &str = "CREATE TEMP TABLE IF NOT EXISTS block_accounts_stage (
    pubkey BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    lamports BIGINT NOT NULL,
    executable BOOLEAN NOT NULL,
    rent_epoch TEXT NOT NULL,
    data BYTEA NOT NULL,
    write_version BIGINT NOT NULL,
    txn_signature BYTEA
) ON COMMIT DELETE ROWS";

/// Columns of a COPY row, in `block_accounts_stage` order.
const STAGE_FIELDS: i16 = 8;

#[derive(Clone, Copy, Debug)]
pub enum WriteMode {
    Unnest,
    Copy,
}

impl WriteMode {
    pub fn for_slot(slot: u64) -> Self {
        if slot.is_multiple_of(2) {
            WriteMode::Unnest
        } else {
            WriteMode::Copy
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            WriteMode::Unnest => "unnest",
            WriteMode::Copy => "copy",
        }
    }
}

/// The block's written accounts, one entry per account in every column.
#[derive(Default)]
pub struct BlockRows {
    pub pubkeys: Vec<Vec<u8>>,
    pub owners: Vec<Vec<u8>>,
    pub lamports: Vec<i64>,
    pub executable: Vec<bool>,
    pub rent_epoch: Vec<String>,
    pub data: Vec<Vec<u8>>,
    pub write_version: Vec<i64>,
    pub txn_signature: Vec<Option<Vec<u8>>>,
}

impl BlockRows {
    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        pubkey: Vec<u8>,
        owner: Vec<u8>,
        lamports: u64,
        executable: bool,
        rent_epoch: u64,
        data: Vec<u8>,
        write_version: u64,
        txn_signature: Option<Vec<u8>>,
    ) {
        self.pubkeys.push(pubkey);
        self.owners.push(owner);
        self.lamports.push(lamports as i64);
        self.executable.push(executable);
        self.rent_epoch.push(rent_epoch.to_string());
        self.data.push(data);
        self.write_version.push(write_version as i64);
        self.txn_signature.push(txn_signature);
    }

    pub fn len(&self) -> usize {
        self.pubkeys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pubkeys.is_empty()
    }
}

/// Rows the block removes: closes and owner moves routed by the old owner, and closes the owner
/// map did not know (deleted by pubkey in every partition).
#[derive(Default)]
pub struct BlockDeletes {
    pub routed_pubkeys: Vec<Vec<u8>>,
    pub routed_owners: Vec<Vec<u8>>,
    pub unrouted_pubkeys: Vec<Vec<u8>>,
}

/// A block written but not yet visible. [`OpenBlockTx::publish`] writes the slot row and commits.
pub struct OpenBlockTx {
    tx: Transaction<'static, Postgres>,
    _guard: MutexGuard<'static, ()>,
    mode: WriteMode,
    started: Instant,
    query_timeout: Duration,
}

pub type TxResult<T> = Result<T, String>;

fn observe(mode: WriteMode, phase: &str, since: Instant) {
    metrics::BLOCK_TX_SECONDS
        .with_label_values(&[mode.as_str(), phase])
        .observe(since.elapsed().as_secs_f64());
}

/// Awaits a query under the timeout and turns either failure into a message naming the step.
macro_rules! bounded {
    ($timeout:expr, $what:expr, $fut:expr) => {
        match timeout($timeout, $fut).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => Err(format!("{}: {}", $what, e)),
            Err(_) => Err(format!("{}: timeout after {:?}", $what, $timeout)),
        }
    };
}

/// Reads the primary key columns of `accounts`. Refuses a key that still holds `slot`.
async fn conflict_target(pool: &PgPool) -> TxResult<&'static str> {
    CONFLICT_TARGET
        .get_or_try_init(|| async {
            let columns: Option<String> = sqlx::query_scalar(
                "SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                 FROM pg_index i
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                 WHERE i.indrelid = 'accounts'::regclass AND i.indisprimary",
            )
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read accounts primary key: {e}"))?;
            let columns = columns.ok_or("accounts has no primary key")?;
            if columns.split(", ").any(|column| column == "slot") {
                return Err(format!(
                    "accounts primary key ({columns}) still holds slot; run the single-version migration"
                ));
            }
            tracing::info!(target: "single_version", "accounts conflict target: ({columns})");
            Ok(columns)
        })
        .await
        .map(String::as_str)
}

fn upsert_sql(source: &str, conflict_target: &str) -> String {
    // The owner is the partition key on a partitioned table, so it changes only without partitions.
    let owner_update = if conflict_target == "pubkey" {
        "owner = EXCLUDED.owner,\n            "
    } else {
        ""
    };
    format!(
        "INSERT INTO accounts
            (pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version, txn_signature)
        {source}
        ON CONFLICT ({conflict_target}) DO UPDATE SET
            {owner_update}lamports = EXCLUDED.lamports,
            slot = EXCLUDED.slot,
            executable = EXCLUDED.executable,
            rent_epoch = EXCLUDED.rent_epoch,
            data = EXCLUDED.data,
            write_version = EXCLUDED.write_version,
            txn_signature = EXCLUDED.txn_signature,
            updated_on = CURRENT_TIMESTAMP
        WHERE accounts.slot <= EXCLUDED.slot"
    )
}

fn push_field(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

/// Encodes the rows in the Postgres binary COPY format, in `block_accounts_stage` column order.
fn copy_binary(rows: &BlockRows) -> Vec<u8> {
    let data_bytes: usize = rows.data.iter().map(Vec::len).sum();
    let mut buf = Vec::with_capacity(32 + data_bytes + rows.len() * 200);
    buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    buf.extend_from_slice(&0i32.to_be_bytes());
    buf.extend_from_slice(&0i32.to_be_bytes());
    for i in 0..rows.len() {
        buf.extend_from_slice(&STAGE_FIELDS.to_be_bytes());
        push_field(&mut buf, &rows.pubkeys[i]);
        push_field(&mut buf, &rows.owners[i]);
        push_field(&mut buf, &rows.lamports[i].to_be_bytes());
        push_field(&mut buf, &[rows.executable[i] as u8]);
        push_field(&mut buf, rows.rent_epoch[i].as_bytes());
        push_field(&mut buf, &rows.data[i]);
        push_field(&mut buf, &rows.write_version[i].to_be_bytes());
        match &rows.txn_signature[i] {
            Some(signature) => push_field(&mut buf, signature),
            None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    buf.extend_from_slice(&(-1i16).to_be_bytes());
    buf
}

/// Creates `block_accounts_stage` if needed and streams the encoded rows into it.
/// Boxed for the same `Send` reason as [`write_block`].
fn copy_stage(conn: &mut PgConnection, buf: Vec<u8>) -> BoxFuture<'_, Result<u64, sqlx::Error>> {
    Box::pin(async move {
        sqlx::query(STAGE_TABLE_SQL).execute(&mut *conn).await?;
        let mut copy = conn
            .copy_in_raw("COPY block_accounts_stage FROM STDIN (FORMAT binary)")
            .await?;
        copy.send(buf).await?;
        copy.finish().await
    })
}

/// Writes the block's deletes and upserts in a new transaction and returns it open.
/// `snapshot_ready` is false while `snapshot_accounts` is still loading.
/// Boxed so the future is checked `Send` here; callers run inside `tokio::spawn`.
pub fn write_block<'a>(
    pool: &'a PgPool,
    slot: u64,
    rows: &'a BlockRows,
    deletes: &'a BlockDeletes,
    snapshot_ready: bool,
    query_timeout: Duration,
) -> BoxFuture<'a, TxResult<OpenBlockTx>> {
    Box::pin(write_block_inner(
        pool,
        slot,
        rows,
        deletes,
        snapshot_ready,
        query_timeout,
    ))
}

async fn write_block_inner(
    pool: &PgPool,
    slot: u64,
    rows: &BlockRows,
    deletes: &BlockDeletes,
    snapshot_ready: bool,
    query_timeout: Duration,
) -> TxResult<OpenBlockTx> {
    let mode = WriteMode::for_slot(slot);
    let started = Instant::now();
    let guard = BLOCK_TX_LOCK.lock().await;
    observe(mode, "lock_wait", started);

    let target = conflict_target(pool).await?;
    let mut tx = bounded!(query_timeout, "begin", pool.begin())?;
    let slot_i64 = slot as i64;

    let phase = Instant::now();
    if !deletes.routed_pubkeys.is_empty() {
        let query = sqlx::query(
            "DELETE FROM accounts a
             USING unnest($1::bytea[], $2::bytea[]) AS k(pubkey, owner)
             WHERE a.owner = k.owner AND a.pubkey = k.pubkey AND a.slot <= $3",
        )
        .bind(&deletes.routed_pubkeys)
        .bind(&deletes.routed_owners)
        .bind(slot_i64)
        .execute(&mut *tx);
        bounded!(query_timeout, "delete routed closed", query)?;
    }
    if !deletes.unrouted_pubkeys.is_empty() {
        let query =
            sqlx::query("DELETE FROM accounts WHERE pubkey = ANY($1::bytea[]) AND slot <= $2")
                .bind(&deletes.unrouted_pubkeys)
                .bind(slot_i64)
                .execute(&mut *tx);
        bounded!(query_timeout, "delete unrouted closed", query)?;
    }
    observe(mode, "delete_closed", phase);

    let phase = Instant::now();
    if !rows.is_empty() {
        match mode {
            WriteMode::Unnest => {
                let sql = upsert_sql(
                    "SELECT k.pubkey, k.owner, k.lamports, $9, k.executable, k.rent_epoch::numeric,
                            k.data, k.write_version, k.txn_signature
                     FROM unnest($1::bytea[], $2::bytea[], $3::int8[], $4::bool[], $5::text[],
                                 $6::bytea[], $7::int8[], $8::bytea[])
                          AS k(pubkey, owner, lamports, executable, rent_epoch, data,
                               write_version, txn_signature)",
                    target,
                );
                let query = sqlx::query(&sql)
                    .bind(&rows.pubkeys)
                    .bind(&rows.owners)
                    .bind(&rows.lamports)
                    .bind(&rows.executable)
                    .bind(&rows.rent_epoch)
                    .bind(&rows.data)
                    .bind(&rows.write_version)
                    .bind(&rows.txn_signature)
                    .bind(slot_i64)
                    .execute(&mut *tx);
                bounded!(query_timeout, "upsert unnest", query)?;
            }
            WriteMode::Copy => {
                let copy_started = Instant::now();
                let buf = copy_binary(rows);
                bounded!(query_timeout, "copy stage", copy_stage(&mut tx, buf))?;
                observe(mode, "copy_in", copy_started);

                let sql = upsert_sql(
                    "SELECT s.pubkey, s.owner, s.lamports, $1, s.executable, s.rent_epoch::numeric,
                            s.data, s.write_version, s.txn_signature
                     FROM block_accounts_stage AS s",
                    target,
                );
                let query = sqlx::query(&sql).bind(slot_i64).execute(&mut *tx);
                bounded!(query_timeout, "upsert from stage", query)?;
            }
        }
    }
    observe(mode, "upsert", phase);

    let phase = Instant::now();
    if snapshot_ready {
        let mut pubkeys = rows.pubkeys.clone();
        pubkeys.extend(deletes.routed_pubkeys.iter().cloned());
        let mut owners = rows.owners.clone();
        owners.extend(deletes.routed_owners.iter().cloned());
        if !pubkeys.is_empty() {
            let query = sqlx::query(
                "DELETE FROM snapshot_accounts a
                 USING unnest($1::bytea[], $2::bytea[]) AS k(pubkey, owner)
                 WHERE a.owner = k.owner AND a.pubkey = k.pubkey",
            )
            .bind(&pubkeys)
            .bind(&owners)
            .execute(&mut *tx);
            bounded!(query_timeout, "delete routed snapshot", query)?;
        }
        if !deletes.unrouted_pubkeys.is_empty() {
            let query =
                sqlx::query("DELETE FROM snapshot_accounts WHERE pubkey = ANY($1::bytea[])")
                    .bind(&deletes.unrouted_pubkeys)
                    .execute(&mut *tx);
            bounded!(query_timeout, "delete unrouted snapshot", query)?;
        }
    }
    observe(mode, "delete_snapshot", phase);

    Ok(OpenBlockTx {
        tx,
        _guard: guard,
        mode,
        started,
        query_timeout,
    })
}

impl OpenBlockTx {
    /// Writes the confirmed slot row in the block's transaction and commits.
    pub fn publish(
        self,
        slot: u64,
        block_time: i64,
        blockhash: Option<String>,
        healthy: bool,
        commitment: i32,
    ) -> BoxFuture<'static, TxResult<()>> {
        Box::pin(self.publish_inner(slot, block_time, blockhash, healthy, commitment))
    }

    async fn publish_inner(
        mut self,
        slot: u64,
        block_time: i64,
        blockhash: Option<String>,
        healthy: bool,
        commitment: i32,
    ) -> TxResult<()> {
        let phase = Instant::now();
        let query = sqlx::query(
            "INSERT INTO slots (slot, commitment, block_time, health, blockhash)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (commitment) DO UPDATE
             SET slot = excluded.slot, block_time = excluded.block_time, blockhash = excluded.blockhash
             WHERE excluded.slot > slots.slot",
        )
        .bind(slot as i64)
        .bind(commitment)
        .bind(block_time)
        .bind(healthy)
        .bind(blockhash)
        .execute(&mut *self.tx);
        bounded!(self.query_timeout, "slot row", query)?;
        observe(self.mode, "slot", phase);

        let phase = Instant::now();
        bounded!(self.query_timeout, "commit", self.tx.commit())?;
        observe(self.mode, "commit", phase);
        observe(self.mode, "total", self.started);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_binary_frames_header_rows_and_trailer() {
        let mut rows = BlockRows::default();
        rows.push(vec![1; 32], vec![2; 32], 5, true, 7, vec![9, 9], 11, None);
        let buf = copy_binary(&rows);
        assert!(buf.starts_with(b"PGCOPY\n\xff\r\n\0"));
        assert!(buf.ends_with(&(-1i16).to_be_bytes()));
        // header 19 + field count 2 + 8 length words + payloads (32+32+8+1+1+2+8) + trailer 2
        assert_eq!(buf.len(), 19 + 2 + 8 * 4 + 84 + 2);
    }

    #[test]
    fn upsert_updates_owner_only_without_partitions() {
        assert!(upsert_sql("SELECT 1", "pubkey").contains("owner = EXCLUDED.owner"));
        assert!(!upsert_sql("SELECT 1", "owner, pubkey").contains("owner = EXCLUDED.owner"));
    }

    async fn write(
        pool: &PgPool,
        slot: u64,
        rows: Vec<(&Vec<u8>, &Vec<u8>, u64)>,
        deletes: BlockDeletes,
    ) {
        let mut block = BlockRows::default();
        for (pubkey, owner, lamports) in rows {
            block.push(
                pubkey.clone(),
                owner.clone(),
                lamports,
                false,
                u64::MAX,
                vec![7; 3000],
                1,
                None,
            );
        }
        let open = write_block(pool, slot, &block, &deletes, true, Duration::from_secs(5))
            .await
            .unwrap();
        open.publish(slot, 0, None, true, 1).await.unwrap();
    }

    type State = (Vec<(Vec<u8>, Vec<u8>, i64, i64)>, i64, i64);

    async fn state(pool: &PgPool) -> State {
        let rows =
            sqlx::query_as("SELECT pubkey, owner, lamports, slot FROM accounts ORDER BY pubkey")
                .fetch_all(pool)
                .await
                .unwrap();
        let snapshot = sqlx::query_scalar("SELECT count(*) FROM snapshot_accounts")
            .fetch_one(pool)
            .await
            .unwrap();
        let slot = sqlx::query_scalar("SELECT slot FROM slots WHERE commitment = 1")
            .fetch_one(pool)
            .await
            .unwrap();
        (rows, snapshot, slot)
    }

    /// Needs a migrated Postgres: `SINGLE_VERSION_TEST_DATABASE_URL=postgres://... cargo test -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn block_transactions_keep_one_row_per_account() {
        let url = std::env::var("SINGLE_VERSION_TEST_DATABASE_URL").expect("database url");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql("TRUNCATE accounts, snapshot_accounts, slots")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(
            "INSERT INTO snapshot_accounts (pubkey, owner, lamports, slot, executable, rent_epoch, data, write_version)
             VALUES ('\\x0101', '\\xaa', 1, 1, false, 0, '\\x', 0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let timeout = Duration::from_secs(5);
        let (x, y, w, z) = (vec![1, 1], vec![2, 2], vec![3, 3], vec![4, 4]);
        let (a, b) = (vec![0xaa], vec![0xbb]);

        // Slot 10 (unnest): three new rows; the snapshot row of `x` goes.
        write(
            &pool,
            10,
            vec![(&x, &a, 5), (&y, &a, 5), (&w, &a, 5)],
            BlockDeletes::default(),
        )
        .await;
        let (rows, snapshot, slot) = state(&pool).await;
        assert_eq!(rows.len(), 3);
        assert_eq!((snapshot, slot), (0, 10));

        // Slot 11 (copy): `x` updated, `y` closed, `w` moved to owner `b`, `z` closed and unknown.
        let deletes = BlockDeletes {
            routed_pubkeys: vec![y.clone(), w.clone()],
            routed_owners: vec![a.clone(), a.clone()],
            unrouted_pubkeys: vec![z.clone()],
        };
        write(&pool, 11, vec![(&x, &a, 6), (&w, &b, 8)], deletes).await;
        let (rows, _, slot) = state(&pool).await;
        assert_eq!(
            rows,
            vec![(x.clone(), a.clone(), 6, 11), (w.clone(), b.clone(), 8, 11)]
        );
        assert_eq!(slot, 11);

        // Repaired slot 9 (copy) is older: it changes neither rows nor the published slot.
        let deletes = BlockDeletes {
            routed_pubkeys: vec![w.clone()],
            routed_owners: vec![b.clone()],
            unrouted_pubkeys: vec![],
        };
        write(&pool, 9, vec![(&x, &a, 1)], deletes).await;
        let (after, _, slot) = state(&pool).await;
        assert_eq!((after, slot), (rows.clone(), 11));

        // Slot 12 (unnest) written but never published: nothing becomes visible.
        let mut block = BlockRows::default();
        block.push(x.clone(), a.clone(), 99, false, 0, vec![], 1, None);
        let open = write_block(&pool, 12, &block, &BlockDeletes::default(), true, timeout)
            .await
            .unwrap();
        drop(open);
        let (after, _, slot) = state(&pool).await;
        assert_eq!((after, slot), (rows, 11));
    }
}
