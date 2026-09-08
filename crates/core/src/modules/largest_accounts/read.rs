//! Read path for the largest_accounts table, shared with the API: the newest
//! persisted record for a mint at or below a commitment slot.

use super::decode_record;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, Value};
use solana_pubkey::Pubkey;

/// The newest record for a mint at or below a slot. `$1` = mint bytea,
/// `$2` = max slot.
const RECORD_READ_SQL: &str = "SELECT record FROM largest_accounts \
    WHERE mint = $1 AND slot = ( \
        SELECT MAX(slot) FROM largest_accounts WHERE mint = $1 AND slot <= $2 \
    )";

/// Fetches and decodes the persisted top-N record for `mint` as of `max_slot`.
/// `Ok(None)` when no record exists (untracked mint, or none persisted yet).
pub async fn fetch_record(
    db: &DatabaseConnection,
    mint: &Pubkey,
    max_slot: u64,
) -> Result<Option<Vec<(Pubkey, u64)>>, DbErr> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            RECORD_READ_SQL,
            [
                Value::from(mint.as_ref().to_vec()),
                Value::BigInt(Some(max_slot as i64)),
            ],
        ))
        .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let bytes: Vec<u8> = row.try_get("", "record")?;
    decode_record(&bytes).map(Some).ok_or_else(|| {
        DbErr::Custom(format!("corrupt largest_accounts record for mint {mint}"))
    })
}
