//! getProgramAccounts and the reads by pubkey through the JSON-RPC handler, against Postgres with the migrations'
//! schema. Each case writes one account's rows as the indexer and the snapshot loader write them, then reads at the
//! finalized slot. Agave answers with the account's state at that slot: getProgramAccounts returns the account only
//! when that state is live and passes every filter, whatever its older versions held.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cloudbreak_core::modules::mint_cache::MintCache;
use cloudbreak_core::modules::processed::ProcessedAccounts;
use cloudbreak_core::utils::token::{LEGACY_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
use cloudbreak_core::{
    AccountSelectorConfig, GpaCacheConfig, IndexConfig, MethodSection, ProcessedCommitmentBehavior,
    TryLoadConfig, UnhealthyResponseBehavior,
};
use cloudbreak_index::indexer::IndexerState;
use cloudbreak_index::modules::save_block::save_block;
use cloudbreak_snapshot::upsert_accounts_batched;
use http_body_util::BodyExt;
use rstest::rstest;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use solana_account::Account;
use solana_account_decoder::encode_ui_account;
use solana_account_decoder_client_types::{UiAccount, UiAccountEncoding};
use solana_commitment_config::CommitmentConfig;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcContextConfig, RpcProgramAccountsConfig,
};
use solana_rpc_client_api::filter::{Memcmp, RpcFilterType};
use solana_rpc_client_api::request::RpcRequest;
use solana_rpc_client_api::response::{Response, RpcKeyedAccount, RpcResponseContext};
use spl_token_interface::state::{Account as TokenAccount, AccountState};
use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
};

use super::process_single_request;
use crate::http::server::ResponseBody;
use crate::http::{CloudbreakRpcState, RequestContext};
use crate::modules::cache::GpaProcessor;
use crate::modules::vote_accounts_cache::StakesSnapshot;
use postgres::start_database;

mod postgres;

/// The slot of the version a later row replaces.
const OLDER_SLOT: i64 = 10;
/// The slot of a version between that one and the account's newest row.
const MIDDLE_SLOT: i64 = 15;
/// The slot of the account's newest row.
const NEWER_SLOT: i64 = 20;
/// The finalized slot every request reads at.
const READ_SLOT: u64 = 30;
const LAMPORTS: i64 = 1_000_000;
const ACCOUNT: Pubkey = Pubkey::new_from_array([9; 32]);
const PROGRAM: Pubkey = Pubkey::new_from_array([7; 32]);
const OTHER_PROGRAM: Pubkey = Pubkey::new_from_array([8; 32]);
/// A token account's mint, its owner the filter selects, and another owner.
const MINT: [u8; 32] = [3; 32];
const SELECTED_OWNER: [u8; 32] = [4; 32];
const OTHER_OWNER: [u8; 32] = [5; 32];
/// The first bytes of a program account the memcmp filter selects.
const SELECTED_PREFIX: [u8; 8] = [1; 8];

/// The filters of the request.
#[derive(Clone, Copy, Debug)]
enum Filter {
    None,
    /// `dataSize` of the selected version's 40 bytes.
    DataSize,
    /// `memcmp` of the selected version's first 8 bytes, which Cloudbreak compares with `SUBSTRING(data ...)`.
    Memcmp,
    /// A token program `memcmp` of the owner at offset 32, which Cloudbreak compares with the `token_owner` column.
    TokenOwner,
}

/// Where a row of the account is written.
#[derive(Clone, Copy, Debug)]
enum Table {
    Accounts,
    SnapshotAccounts,
}

/// The account's rows up to the read slot.
#[derive(Clone, Copy, Debug)]
enum History {
    /// A version the filters select, at `NEWER_SLOT`.
    Live,
    /// A version the filters select at `OLDER_SLOT` in the table, then one they do not select at `NEWER_SLOT`.
    NoLongerSelected(Table),
    /// A version the filters select at `OLDER_SLOT` in the table, then the close row the indexer writes at
    /// `NEWER_SLOT`.
    Closed(Table, OwnerMap),
    /// A version the filters do not select at `OLDER_SLOT` in the table, one they select at `MIDDLE_SLOT`, then
    /// the close row the indexer writes at `NEWER_SLOT`.
    ClosedAfterBecomingSelected(Table, OwnerMap),
    /// A version the filters select at `OLDER_SLOT` in the table, one they do not select at `MIDDLE_SLOT`, then
    /// the close row the indexer writes at `NEWER_SLOT`.
    ClosedAfterNoLongerSelected(Table, OwnerMap),
    /// A version the filters select at `OLDER_SLOT` in the table, then the same data under another program at
    /// `NEWER_SLOT`. The indexer also writes a close row under the first program.
    MovedToAnotherProgram(Table, OwnerMap),
    /// A version the filters select at `OLDER_SLOT` in the table, the same data under another program at
    /// `MIDDLE_SLOT`, then under the first program again at `NEWER_SLOT`.
    MovedToAnotherProgramAndBack(Table, OwnerMap),
    /// A version the filters select at `OLDER_SLOT` in the table, the same data under another program at
    /// `MIDDLE_SLOT`, then the close row the indexer writes at `NEWER_SLOT`.
    ClosedAfterMovingToAnotherProgram(Table, OwnerMap),
}

/// The indexer's `accounts-owner-map-enabled`, which picks the statements that write close rows.
#[derive(Clone, Copy, Debug)]
enum OwnerMap {
    /// `insertClosedAccount.sql` and `insertMovedAccount.sql`, which read the account's owner from both tables.
    Off,
    /// `insertClosedAccountWithMap.sql`, with the owner the map holds.
    On,
}

/// What Agave answers at the read slot.
#[derive(Clone, Copy, Debug)]
enum Returned {
    /// The account with the version the filters select, under the first program.
    SelectedVersion,
    /// The account with the same version, under the program it moved to.
    MovedAccount,
    Nothing,
}

#[derive(Clone, Copy, Debug)]
enum Processor {
    Standard,
    /// The getProgramAccounts cache, which has no entry for the query yet.
    CachedFirstQuery,
}

/// The program, another program the account can move to, the account data of the version the filters select and of
/// one they do not, and the filters.
struct Fixture {
    program: Pubkey,
    other_program: Pubkey,
    selected: Vec<u8>,
    not_selected: Vec<u8>,
    filters: Option<Vec<RpcFilterType>>,
}

fn fixture(filter: Filter) -> Fixture {
    match filter {
        Filter::None => program_fixture(None),
        Filter::DataSize => program_fixture(Some(vec![RpcFilterType::DataSize(40)])),
        Filter::Memcmp => program_fixture(Some(vec![RpcFilterType::Memcmp(
            Memcmp::new_raw_bytes(0, SELECTED_PREFIX.to_vec()),
        )])),
        Filter::TokenOwner => Fixture {
            program: LEGACY_TOKEN_PROGRAM_ID,
            other_program: TOKEN_2022_PROGRAM_ID,
            selected: token_account(SELECTED_OWNER),
            not_selected: token_account(OTHER_OWNER),
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
                32,
                SELECTED_OWNER.to_vec(),
            ))]),
        },
    }
}

/// 40 bytes starting with `SELECTED_PREFIX`, and 48 bytes of 2, which neither filter selects.
fn program_fixture(filters: Option<Vec<RpcFilterType>>) -> Fixture {
    Fixture {
        program: PROGRAM,
        other_program: OTHER_PROGRAM,
        selected: [SELECTED_PREFIX.as_slice(), &[0; 32]].concat(),
        not_selected: vec![2; 48],
        filters,
    }
}

/// An initialized token account of `MINT` owned by `owner`, holding nothing.
fn token_account(owner: [u8; 32]) -> Vec<u8> {
    let account = TokenAccount {
        mint: MINT.into(),
        owner: owner.into(),
        state: AccountState::Initialized,
        ..TokenAccount::default()
    };
    let mut data = vec![0; TokenAccount::LEN];
    account.pack_into_slice(&mut data);
    data
}

/// The example indexer configuration, indexing every program and loading no snapshot, with the
/// owner map on or off.
fn indexer_config(owner_map: OwnerMap) -> IndexConfig {
    let mut config = index_config().clone();
    config.snapshot = None;
    config.programs = AccountSelectorConfig::default();
    config.accounts_owner_map_enabled = matches!(owner_map, OwnerMap::On);
    config
}

/// The account at `lamports` under `owner`, as a block carries it.
fn account_update(owner: Pubkey, lamports: u64, data: &[u8]) -> SubscribeUpdateAccountInfo {
    SubscribeUpdateAccountInfo {
        pubkey: ACCOUNT.to_bytes().to_vec(),
        lamports,
        owner: owner.to_bytes().to_vec(),
        data: data.to_vec(),
        write_version: 1,
        ..SubscribeUpdateAccountInfo::default()
    }
}

/// A closed account as a block carries it: no lamports, no data, owned by the system program.
fn close_update() -> SubscribeUpdateAccountInfo {
    account_update(Pubkey::default(), 0, &[])
}

/// Saves a block at `slot` holding one account update, as the indexer does for each block.
async fn save(
    db: &DatabaseConnection,
    config: &IndexConfig,
    state: &IndexerState,
    slot: i64,
    account: SubscribeUpdateAccountInfo,
) {
    let block = SubscribeUpdateBlock {
        slot: slot as u64,
        blockhash: format!("block-{slot}"),
        accounts: vec![account],
        ..SubscribeUpdateBlock::default()
    };
    save_block(block, db, config.clone(), state.clone()).await;
}

/// The account's version at `OLDER_SLOT`: a block's update, or a snapshot row.
async fn write_older_version(
    db: &DatabaseConnection,
    config: &IndexConfig,
    state: &IndexerState,
    table: Table,
    program: Pubkey,
    data: &[u8],
) {
    let update = account_update(program, LAMPORTS as u64, data);
    match table {
        Table::Accounts => save(db, config, state, OLDER_SLOT, update).await,
        Table::SnapshotAccounts => {
            upsert_accounts_batched(
                db,
                vec![SubscribeUpdateAccount {
                    account: Some(update),
                    slot: OLDER_SLOT as u64,
                    is_startup: true,
                    ..SubscribeUpdateAccount::default()
                }],
            )
            .await
            .expect("insert the snapshot row");
            // The snapshot loader records each account's owner in the map as it loads it.
            state.accounts_owner_map.upsert_account(
                &ACCOUNT.to_bytes().to_vec(),
                &program.to_bytes().to_vec(),
                OLDER_SLOT as u64,
            );
        }
    }
}

async fn write_history(db: &DatabaseConnection, history: History, fixture: &Fixture) {
    let Fixture {
        program,
        other_program,
        selected,
        not_selected,
        ..
    } = fixture;
    let owner_map = match history {
        History::Live | History::NoLongerSelected(_) => OwnerMap::Off,
        History::Closed(_, owner_map)
        | History::ClosedAfterBecomingSelected(_, owner_map)
        | History::ClosedAfterNoLongerSelected(_, owner_map)
        | History::MovedToAnotherProgram(_, owner_map)
        | History::MovedToAnotherProgramAndBack(_, owner_map)
        | History::ClosedAfterMovingToAnotherProgram(_, owner_map) => owner_map,
    };
    let config = indexer_config(owner_map);
    let state = IndexerState::new(db.clone(), config.clone(), 0)
        .await
        .expect("build the indexer state");

    match history {
        History::Live => {
            let live = account_update(*program, LAMPORTS as u64, selected);
            save(db, &config, &state, NEWER_SLOT, live).await;
        }
        History::NoLongerSelected(table) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            let changed = account_update(*program, LAMPORTS as u64, not_selected);
            save(db, &config, &state, NEWER_SLOT, changed).await;
        }
        History::Closed(table, _) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            save(db, &config, &state, NEWER_SLOT, close_update()).await;
        }
        History::ClosedAfterBecomingSelected(table, _) => {
            write_older_version(db, &config, &state, table, *program, not_selected).await;
            let changed = account_update(*program, LAMPORTS as u64, selected);
            save(db, &config, &state, MIDDLE_SLOT, changed).await;
            save(db, &config, &state, NEWER_SLOT, close_update()).await;
        }
        History::ClosedAfterNoLongerSelected(table, _) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            let changed = account_update(*program, LAMPORTS as u64, not_selected);
            save(db, &config, &state, MIDDLE_SLOT, changed).await;
            save(db, &config, &state, NEWER_SLOT, close_update()).await;
        }
        History::MovedToAnotherProgram(table, _) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            let moved = account_update(*other_program, LAMPORTS as u64, selected);
            save(db, &config, &state, NEWER_SLOT, moved).await;
        }
        History::MovedToAnotherProgramAndBack(table, _) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            let moved = account_update(*other_program, LAMPORTS as u64, selected);
            save(db, &config, &state, MIDDLE_SLOT, moved).await;
            let moved_back = account_update(*program, LAMPORTS as u64, selected);
            save(db, &config, &state, NEWER_SLOT, moved_back).await;
        }
        History::ClosedAfterMovingToAnotherProgram(table, _) => {
            write_older_version(db, &config, &state, table, *program, selected).await;
            let moved = account_update(*other_program, LAMPORTS as u64, selected);
            save(db, &config, &state, MIDDLE_SLOT, moved).await;
            save(db, &config, &state, NEWER_SLOT, close_update()).await;
        }
    }
}

/// The indexer's example configuration, which the indexer's own functions read.
fn index_config() -> &'static IndexConfig {
    static CONFIG: OnceLock<IndexConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        IndexConfig::try_load(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../example.cloudbreak.index.toml"
        ))
        .expect("load the example index configuration")
    })
}

/// The live account under `owner`, as Agave encodes it for a request with `encoding`.
fn ui_account(owner: Pubkey, data: &[u8], encoding: UiAccountEncoding) -> UiAccount {
    encode_ui_account(
        &ACCOUNT,
        &Account {
            lamports: LAMPORTS as u64,
            data: data.to_vec(),
            owner,
            executable: false,
            rent_epoch: 0,
        },
        encoding,
        None,
        None,
    )
}

fn expected_accounts(returned: Returned, fixture: &Fixture) -> Vec<RpcKeyedAccount> {
    match returned {
        Returned::SelectedVersion => vec![RpcKeyedAccount {
            pubkey: ACCOUNT.to_string(),
            account: ui_account(
                fixture.program,
                &fixture.selected,
                UiAccountEncoding::Base64,
            ),
        }],
        Returned::MovedAccount => vec![RpcKeyedAccount {
            pubkey: ACCOUNT.to_string(),
            account: ui_account(
                fixture.other_program,
                &fixture.selected,
                UiAccountEncoding::Base64,
            ),
        }],
        Returned::Nothing => Vec::new(),
    }
}

fn gpa_processor(processor: Processor) -> GpaProcessor {
    match processor {
        Processor::Standard => GpaProcessor::new(None),
        Processor::CachedFirstQuery => GpaProcessor::new(Some(GpaCacheConfig {
            max_total_bytes: 1 << 20,
            min_bytes_per_query: 0,
            max_bytes_query_cleanup: None,
            max_pinned_bytes_ratio: None,
        })),
    }
}

fn state(database: DatabaseConnection, processor: Processor) -> CloudbreakRpcState {
    CloudbreakRpcState::new(
        database,
        Duration::from_secs(30),
        None,
        None,
        Arc::new(AccountSelectorConfig::default()),
        1,
        None,
        Duration::from_secs(30),
        ProcessedCommitmentBehavior::default(),
        UnhealthyResponseBehavior::default(),
        gpa_processor(processor),
        String::new(),
        false,
        Arc::new(std::sync::RwLock::new(Arc::new(StakesSnapshot::empty()))),
        100,
        false,
        false,
        Arc::default(),
        MethodSection::default(),
        MethodSection::default(),
        ProcessedAccounts::default(),
        MintCache::default(),
    )
}

#[derive(Deserialize)]
struct Answer<T> {
    result: Response<T>,
}

/// The context of getAccountInfo, getMultipleAccounts and getBalance, which send no API version.
const CONTEXT_WITHOUT_API_VERSION: RpcResponseContext = RpcResponseContext {
    slot: READ_SLOT,
    api_version: None,
};

/// Data in `encoding`, read at the finalized slot.
fn account_config(encoding: UiAccountEncoding) -> RpcAccountInfoConfig {
    RpcAccountInfoConfig {
        encoding: Some(encoding),
        commitment: Some(CommitmentConfig::finalized()),
        ..RpcAccountInfoConfig::default()
    }
}

/// Sends one request through the JSON-RPC handler and returns the body of its answer.
async fn answer_body(
    db: DatabaseConnection,
    processor: Processor,
    method: RpcRequest,
    params: serde_json::Value,
) -> ResponseBody {
    let request =
        serde_json::from_value(method.build_request_json(1, params)).expect("parse the request");
    let context = RequestContext {
        subscription_id: "test".into(),
        request_id: "test".into(),
        client_ip: "127.0.0.1".into(),
    };
    let response = process_single_request(
        request,
        &Arc::new(state(db, processor)),
        &Arc::new(context),
        false,
    )
    .await;

    response.body
}

/// The result of getProgramAccounts of `program` at the finalized slot.
async fn get_program_accounts(
    db: DatabaseConnection,
    processor: Processor,
    program: Pubkey,
    filters: Option<Vec<RpcFilterType>>,
) -> Response<Vec<RpcKeyedAccount>> {
    let config = RpcProgramAccountsConfig {
        filters,
        account_config: account_config(UiAccountEncoding::Base64),
        with_context: Some(true),
        ..RpcProgramAccountsConfig::default()
    };
    let params = serde_json::json!([program.to_string(), config]);
    let body = answer_body(db, processor, RpcRequest::GetProgramAccounts, params).await;

    let ResponseBody::Streaming(body) = body else {
        panic!("getProgramAccounts answered with a buffered body, which holds an error");
    };
    let bytes = body.collect().await.expect("read the body").to_bytes();
    let Answer { result } = serde_json::from_slice(&bytes).expect("parse the answer");
    result
}

/// The result of a read by pubkey, which answers with a buffered body.
async fn read_by_pubkey<T: DeserializeOwned>(
    db: DatabaseConnection,
    method: RpcRequest,
    params: serde_json::Value,
) -> Response<T> {
    let body = answer_body(db, Processor::Standard, method, params).await;

    let ResponseBody::Buffered(bytes) = body else {
        panic!("{method} answered with a streamed body");
    };
    let Answer { result } = serde_json::from_slice(&bytes).expect("parse the answer");
    result
}

#[rstest]
#[case::no_filter_live(Filter::None, History::Live, Returned::SelectedVersion)]
#[case::no_filter_closed_over_accounts_without_owner_map(
    Filter::None,
    History::Closed(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::no_filter_closed_over_accounts_with_owner_map(
    Filter::None,
    History::Closed(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::no_filter_closed_over_snapshot_without_owner_map(
    Filter::None,
    History::Closed(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::no_filter_closed_over_snapshot_with_owner_map(
    Filter::None,
    History::Closed(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::no_filter_moved_over_accounts_with_owner_map(
    Filter::None,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::no_filter_moved_over_snapshot_with_owner_map(
    Filter::None,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_live(Filter::DataSize, History::Live, Returned::SelectedVersion)]
#[case::memcmp_live(Filter::Memcmp, History::Live, Returned::SelectedVersion)]
#[case::token_owner_live(Filter::TokenOwner, History::Live, Returned::SelectedVersion)]
#[case::data_size_closed_over_accounts_without_owner_map(
    Filter::DataSize,
    History::Closed(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_over_accounts_with_owner_map(
    Filter::DataSize,
    History::Closed(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_closed_over_snapshot_without_owner_map(
    Filter::DataSize,
    History::Closed(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_over_snapshot_with_owner_map(
    Filter::DataSize,
    History::Closed(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_moved_over_accounts_with_owner_map(
    Filter::DataSize,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_moved_over_snapshot_with_owner_map(
    Filter::DataSize,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_closed_over_accounts_without_owner_map(
    Filter::Memcmp,
    History::Closed(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_over_accounts_with_owner_map(
    Filter::Memcmp,
    History::Closed(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_closed_over_snapshot_without_owner_map(
    Filter::Memcmp,
    History::Closed(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_over_snapshot_with_owner_map(
    Filter::Memcmp,
    History::Closed(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_moved_over_accounts_with_owner_map(
    Filter::Memcmp,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_moved_over_snapshot_with_owner_map(
    Filter::Memcmp,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_closed_over_accounts_without_owner_map(
    Filter::TokenOwner,
    History::Closed(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_over_accounts_with_owner_map(
    Filter::TokenOwner,
    History::Closed(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_closed_over_snapshot_without_owner_map(
    Filter::TokenOwner,
    History::Closed(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_over_snapshot_with_owner_map(
    Filter::TokenOwner,
    History::Closed(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_moved_over_accounts_with_owner_map(
    Filter::TokenOwner,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_moved_over_snapshot_with_owner_map(
    Filter::TokenOwner,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_closed_after_becoming_selected_over_accounts_without_owner_map(
    Filter::DataSize,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_after_becoming_selected_over_accounts_with_owner_map(
    Filter::DataSize,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_closed_after_becoming_selected_over_snapshot_without_owner_map(
    Filter::DataSize,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_after_becoming_selected_over_snapshot_with_owner_map(
    Filter::DataSize,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_closed_after_becoming_selected_over_accounts_without_owner_map(
    Filter::Memcmp,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_after_becoming_selected_over_accounts_with_owner_map(
    Filter::Memcmp,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_closed_after_becoming_selected_over_snapshot_without_owner_map(
    Filter::Memcmp,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_after_becoming_selected_over_snapshot_with_owner_map(
    Filter::Memcmp,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_closed_after_becoming_selected_over_accounts_without_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_after_becoming_selected_over_accounts_with_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterBecomingSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_closed_after_becoming_selected_over_snapshot_without_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_after_becoming_selected_over_snapshot_with_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterBecomingSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_no_longer_selected_over_accounts(
    Filter::DataSize,
    History::NoLongerSelected(Table::Accounts),
    Returned::Nothing
)]
#[case::data_size_no_longer_selected_over_snapshot(
    Filter::DataSize,
    History::NoLongerSelected(Table::SnapshotAccounts),
    Returned::Nothing
)]
#[case::data_size_closed_after_no_longer_selected_over_accounts_without_owner_map(
    Filter::DataSize,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_after_no_longer_selected_over_accounts_with_owner_map(
    Filter::DataSize,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::data_size_closed_after_no_longer_selected_over_snapshot_without_owner_map(
    Filter::DataSize,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_closed_after_no_longer_selected_over_snapshot_with_owner_map(
    Filter::DataSize,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_no_longer_selected_over_accounts(
    Filter::Memcmp,
    History::NoLongerSelected(Table::Accounts),
    Returned::Nothing
)]
#[case::memcmp_no_longer_selected_over_snapshot(
    Filter::Memcmp,
    History::NoLongerSelected(Table::SnapshotAccounts),
    Returned::Nothing
)]
#[case::memcmp_closed_after_no_longer_selected_over_accounts_without_owner_map(
    Filter::Memcmp,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_after_no_longer_selected_over_accounts_with_owner_map(
    Filter::Memcmp,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::memcmp_closed_after_no_longer_selected_over_snapshot_without_owner_map(
    Filter::Memcmp,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_closed_after_no_longer_selected_over_snapshot_with_owner_map(
    Filter::Memcmp,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_no_longer_selected_over_accounts(
    Filter::TokenOwner,
    History::NoLongerSelected(Table::Accounts),
    Returned::Nothing
)]
#[case::token_owner_no_longer_selected_over_snapshot(
    Filter::TokenOwner,
    History::NoLongerSelected(Table::SnapshotAccounts),
    Returned::Nothing
)]
#[case::token_owner_closed_after_no_longer_selected_over_accounts_without_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_after_no_longer_selected_over_accounts_with_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterNoLongerSelected(Table::Accounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::token_owner_closed_after_no_longer_selected_over_snapshot_without_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_closed_after_no_longer_selected_over_snapshot_with_owner_map(
    Filter::TokenOwner,
    History::ClosedAfterNoLongerSelected(Table::SnapshotAccounts, OwnerMap::On),
    Returned::Nothing
)]
#[case::no_filter_moved_over_accounts_without_owner_map(
    Filter::None,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::no_filter_moved_over_snapshot_without_owner_map(
    Filter::None,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_moved_over_accounts_without_owner_map(
    Filter::DataSize,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::data_size_moved_over_snapshot_without_owner_map(
    Filter::DataSize,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_moved_over_accounts_without_owner_map(
    Filter::Memcmp,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::memcmp_moved_over_snapshot_without_owner_map(
    Filter::Memcmp,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_moved_over_accounts_without_owner_map(
    Filter::TokenOwner,
    History::MovedToAnotherProgram(Table::Accounts, OwnerMap::Off),
    Returned::Nothing
)]
#[case::token_owner_moved_over_snapshot_without_owner_map(
    Filter::TokenOwner,
    History::MovedToAnotherProgram(Table::SnapshotAccounts, OwnerMap::Off),
    Returned::Nothing
)]
#[tokio::test]
async fn get_program_accounts_answers_the_account_state_at_the_read_slot(
    #[case] filter: Filter,
    #[case] history: History,
    #[case] expected: Returned,
    #[values(Processor::Standard, Processor::CachedFirstQuery)] processor: Processor,
) {
    let db = start_database().await;
    let fixture = fixture(filter);
    write_history(&db, history, &fixture).await;

    let result =
        get_program_accounts(db, processor, fixture.program, fixture.filters.clone()).await;

    assert_eq!(
        result,
        Response {
            context: RpcResponseContext::new(READ_SLOT),
            value: expected_accounts(expected, &fixture),
        }
    );
}

// The program an account moved to returns it while the account is live under that program.

#[rstest]
#[case::moved_without_owner_map(
    History::MovedToAnotherProgram,
    OwnerMap::Off,
    Returned::MovedAccount
)]
#[case::moved_with_owner_map(History::MovedToAnotherProgram, OwnerMap::On, Returned::MovedAccount)]
#[case::moved_and_back_without_owner_map(
    History::MovedToAnotherProgramAndBack,
    OwnerMap::Off,
    Returned::Nothing
)]
#[case::moved_and_back_with_owner_map(
    History::MovedToAnotherProgramAndBack,
    OwnerMap::On,
    Returned::Nothing
)]
#[case::closed_after_moving_without_owner_map(
    History::ClosedAfterMovingToAnotherProgram,
    OwnerMap::Off,
    Returned::Nothing
)]
#[case::closed_after_moving_with_owner_map(
    History::ClosedAfterMovingToAnotherProgram,
    OwnerMap::On,
    Returned::Nothing
)]
#[tokio::test]
async fn get_program_accounts_of_the_other_program_answers_the_account_state_at_the_read_slot(
    #[case] history: fn(Table, OwnerMap) -> History,
    #[case] owner_map: OwnerMap,
    #[case] expected: Returned,
    #[values(Table::Accounts, Table::SnapshotAccounts)] table: Table,
) {
    let db = start_database().await;
    let fixture = fixture(Filter::None);
    write_history(&db, history(table, owner_map), &fixture).await;

    let result = get_program_accounts(db, Processor::Standard, fixture.other_program, None).await;

    assert_eq!(
        result,
        Response {
            context: RpcResponseContext::new(READ_SLOT),
            value: expected_accounts(expected, &fixture),
        }
    );
}

// An account that moves to another program gets a close row under the first program at the slot
// of its new live row. Agave answers a read by pubkey with the live account. A jsonParsed read
// runs the statement that also joins the mint data.

#[rstest]
#[case::without_owner_map(OwnerMap::Off)]
#[case::with_owner_map(OwnerMap::On)]
#[tokio::test]
async fn get_account_info_answers_a_moved_account_from_its_live_row(
    #[case] owner_map: OwnerMap,
    #[values(Table::Accounts, Table::SnapshotAccounts)] table: Table,
    #[values(UiAccountEncoding::Base64, UiAccountEncoding::JsonParsed)] encoding: UiAccountEncoding,
) {
    let db = start_database().await;
    let fixture = fixture(Filter::None);
    let moved = History::MovedToAnotherProgram(table, owner_map);
    write_history(&db, moved, &fixture).await;

    let params = serde_json::json!([ACCOUNT.to_string(), account_config(encoding)]);
    let result: Response<Option<UiAccount>> =
        read_by_pubkey(db, RpcRequest::GetAccountInfo, params).await;

    assert_eq!(
        result,
        Response {
            context: CONTEXT_WITHOUT_API_VERSION,
            value: Some(ui_account(
                fixture.other_program,
                &fixture.selected,
                encoding
            )),
        }
    );
}

#[rstest]
#[case::without_owner_map(OwnerMap::Off)]
#[case::with_owner_map(OwnerMap::On)]
#[tokio::test]
async fn get_multiple_accounts_answers_a_moved_account_from_its_live_row(
    #[case] owner_map: OwnerMap,
    #[values(Table::Accounts, Table::SnapshotAccounts)] table: Table,
    #[values(UiAccountEncoding::Base64, UiAccountEncoding::JsonParsed)] encoding: UiAccountEncoding,
) {
    let db = start_database().await;
    let fixture = fixture(Filter::None);
    let moved = History::MovedToAnotherProgram(table, owner_map);
    write_history(&db, moved, &fixture).await;

    let params = serde_json::json!([[ACCOUNT.to_string()], account_config(encoding)]);
    let result: Response<Vec<Option<UiAccount>>> =
        read_by_pubkey(db, RpcRequest::GetMultipleAccounts, params).await;

    assert_eq!(
        result,
        Response {
            context: CONTEXT_WITHOUT_API_VERSION,
            value: vec![Some(ui_account(
                fixture.other_program,
                &fixture.selected,
                encoding
            ))],
        }
    );
}

#[rstest]
#[case::without_owner_map(OwnerMap::Off)]
#[case::with_owner_map(OwnerMap::On)]
#[tokio::test]
async fn get_balance_answers_a_moved_account_from_its_live_row(
    #[case] owner_map: OwnerMap,
    #[values(Table::Accounts, Table::SnapshotAccounts)] table: Table,
) {
    let db = start_database().await;
    let fixture = fixture(Filter::None);
    let moved = History::MovedToAnotherProgram(table, owner_map);
    write_history(&db, moved, &fixture).await;

    let config = RpcContextConfig {
        commitment: Some(CommitmentConfig::finalized()),
        min_context_slot: None,
    };
    let params = serde_json::json!([ACCOUNT.to_string(), config]);
    let result: Response<u64> = read_by_pubkey(db, RpcRequest::GetBalance, params).await;

    assert_eq!(
        result,
        Response {
            context: CONTEXT_WITHOUT_API_VERSION,
            value: LAMPORTS as u64,
        }
    );
}
