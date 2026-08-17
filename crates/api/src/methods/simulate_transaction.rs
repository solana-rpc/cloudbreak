use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use agave_feature_set::{FEATURE_NAMES, FeatureSet};
use solana_syscalls::create_program_runtime_environment;
use base64::Engine as _;
use rust_decimal::prelude::ToPrimitive;
use sea_orm::sqlx::{self, Row};
use solana_account::{AccountSharedData, ReadableAccount};
use solana_account_decoder::{UiAccountEncoding, encode_ui_account};
use solana_account_decoder_client_types::{UiAccount, token::UiTokenAmount};
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_clock::{Epoch, Slot};
use solana_commitment_config::CommitmentLevel;
use solana_compute_budget_instruction::instructions_processor::process_compute_budget_instructions;
use solana_fee_structure::FeeDetails;
use solana_hash::Hash;
use solana_loader_v3_interface::state::UpgradeableLoaderState;
use solana_message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_message::{AddressLoader, VersionedMessage};
use solana_precompile_error::PrecompileError;
use solana_program_runtime::execution_budget::{
    SVMTransactionExecutionBudget, SVMTransactionExecutionCost,
};
use solana_program_runtime::loaded_programs::{
    BlockRelation, ForkGraph, ProgramRuntimeEnvironments,
};
use solana_program_runtime::program_cache_entry::ProgramCacheEntry;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcSimulateTransactionConfig;
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcSimulateTransactionResult,
};
use solana_svm::account_loader::{CheckedTransactionDetails, TransactionCheckResult};
use solana_svm::transaction_balances::{BalanceCollector, SvmTokenInfo};
use solana_svm::transaction_processing_result::ProcessedTransaction;
use solana_svm::transaction_processor::{
    ExecutionRecordingConfig, LoadAndExecuteSanitizedTransactionsOutput, TransactionBatchProcessor,
    TransactionProcessingConfig, TransactionProcessingEnvironment,
};
use solana_svm_callback::{InvokeContextCallback, TransactionProcessingCallback};
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_nonce_account::verify_nonce_account;
use solana_transaction::sanitized::{MessageHash, SanitizedTransaction};
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_error::{AddressLoaderError, TransactionError};
use solana_transaction_status_client_types::option_serializer::OptionSerializer;
use solana_transaction_status_client_types::{
    UiCompiledInstruction, UiInnerInstructions, UiInstruction, UiLoadedAddresses,
    UiReturnDataEncoding, UiTransactionEncoding, UiTransactionError, UiTransactionReturnData,
    UiTransactionTokenBalance,
};
use tokio::time::timeout;
use tracing::Instrument;

use crate::db_query;
use crate::error::RpcError;
use crate::http::{CachedFeatureSet, CloudbreakRpcState};

/// Default cluster lamports-per-signature
const LAMPORTS_PER_SIGNATURE: u64 = 5000;
const MAX_PROCESSING_AGE: u64 = 150;

#[tracing::instrument(name = "simtx_rpc", skip_all)]
pub async fn simulate_transaction(
    state: &CloudbreakRpcState,
    transaction: String,
    config: Option<RpcSimulateTransactionConfig>,
) -> Result<RpcResponse<RpcSimulateTransactionResult>, RpcError> {
    if !state.simulation_supported {
        return Err(RpcError::InvalidParamsWithMessage(
            "simulateTransaction is not supported on this node (requires a full, unfiltered index)"
                .to_string(),
        ));
    }

    let config = config.unwrap_or_default();

    if config.sig_verify && config.replace_recent_blockhash {
        return Err(RpcError::InvalidParamsWithMessage(
            "sigVerify may not be used with replaceRecentBlockhash".to_string(),
        ));
    }

    let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base58);
    let tx_bytes = decode_transaction(&transaction, encoding)?;
    let mut versioned_tx: VersionedTransaction = bincode::deserialize(&tx_bytes).map_err(|e| {
        RpcError::InvalidParamsWithMessage(format!("failed to deserialize transaction: {e}"))
    })?;

    if config.sig_verify && !versioned_tx.verify_with_results().iter().all(|ok| *ok) {
        return Err(RpcError::InvalidParamsWithMessage(
            "Transaction signature verification failure".to_string(),
        ));
    }

    let requested_addresses: Vec<Pubkey> = match &config.accounts {
        Some(accounts_config) => {
            let encoding = accounts_config.encoding.unwrap_or(UiAccountEncoding::Base64);
            if matches!(
                encoding,
                UiAccountEncoding::Binary | UiAccountEncoding::Base58
            ) {
                return Err(RpcError::InvalidParamsWithMessage(
                    "base58 encoding not supported".to_string(),
                ));
            }
            accounts_config
                .addresses
                .iter()
                .map(|addr| {
                    addr.parse::<Pubkey>().map_err(|e| {
                        RpcError::InvalidParamsWithMessage(format!("invalid account address: {e}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => Vec::new(),
    };

    let original_blockhash = *versioned_tx.message.recent_blockhash();

    let slot = resolve_slot(state, &config).await?;

    let tip = latest_blockhash(state, slot).await?;
    let replacement_blockhash = if config.replace_recent_blockhash {
        let (hash, block_height) = tip.ok_or(RpcError::InternalError)?;
        versioned_tx
            .message
            .set_recent_blockhash(hash.to_bytes().into());
        Some(RpcBlockhash {
            blockhash: hash.to_string(),
            last_valid_block_height: block_height.saturating_add(MAX_PROCESSING_AGE),
        })
    } else {
        None
    };

    let env_blockhash = match tip {
        Some((hash, _)) => hash,
        None => Hash::new_from_array(original_blockhash.to_bytes()),
    };

    let address_table_lookups: Vec<MessageAddressTableLookup> = match &versioned_tx.message {
        VersionedMessage::V0(m) => m.address_table_lookups.clone(),
        // v1 resolves addresses inline rather than through lookup tables.
        VersionedMessage::Legacy(_) | VersionedMessage::V1(_) => Vec::new(),
    };
    let table_keys: Vec<Pubkey> = address_table_lookups.iter().map(|l| l.account_key).collect();
    let tables = fetch_lookup_tables(state, slot, &table_keys).await?;
    let address_loader = SimulationAddressLoader::new(tables);

    let loaded_addresses = address_loader
        .clone()
        .load_addresses(&address_table_lookups)
        .map(|la| UiLoadedAddresses {
            writable: la.writable.iter().map(|p| p.to_string()).collect(),
            readonly: la.readonly.iter().map(|p| p.to_string()).collect(),
        })
        .unwrap_or_else(|_| UiLoadedAddresses {
            writable: Vec::new(),
            readonly: Vec::new(),
        });

    // Real cluster feature set, not all_enabled(): the latter enables gates mainnet hasn't (e.g. direct mapping) and diverges from mainnet.
    let feature_set = active_feature_set(state, slot).await?.as_ref().clone();

    let mut reserved = agave_reserved_account_keys::ReservedAccountKeys::default();
    reserved.update_active_set(&feature_set);
    let reserved_keys: HashSet<Pubkey> = reserved.active;

    let sanitized_tx = SanitizedTransaction::try_create(
        versioned_tx,
        MessageHash::Compute,
        None,
        address_loader,
        &reserved_keys,
    )
    .map_err(|e| RpcError::InvalidParamsWithMessage(format!("invalid transaction: {e}")))?;

    if requested_addresses.len() > sanitized_tx.message().account_keys().len() {
        return Err(RpcError::InvalidParamsWithMessage(
            "Too many accounts provided; max is the number of accounts in the transaction"
                .to_string(),
        ));
    }

    let mut account_keys: Vec<Pubkey> = sanitized_tx
        .message()
        .account_keys()
        .iter()
        .copied()
        .collect();
    account_keys.extend(sysvar_account_ids());
    account_keys.extend(requested_addresses.iter().copied());
    let mut fetched = fetch_accounts(state, slot, &account_keys).await?;

    let programdata_keys = programdata_addresses(&fetched);
    if !programdata_keys.is_empty() {
        fetched.extend(fetch_accounts(state, slot, &programdata_keys).await?);
    }

    let fallback_accounts: HashMap<Pubkey, (AccountSharedData, Slot)> = requested_addresses
        .iter()
        .filter_map(|key| fetched.get(key).map(|entry| (*key, entry.clone())))
        .collect();

    let nonce = resolve_nonce(&sanitized_tx, &fetched);
    let blockhash_recent = config.replace_recent_blockhash
        || blockhash_is_recent(state, slot, &original_blockhash.to_string()).await?;
    let (nonce_address, blockhash_lamports_per_signature, runnable) = if blockhash_recent {
        (None, LAMPORTS_PER_SIGNATURE, true)
    } else if let Some((address, lamports_per_signature)) = nonce {
        (Some(address), lamports_per_signature, true)
    } else {
        (None, LAMPORTS_PER_SIGNATURE, false)
    };

    let bank = SimulationBank::new(fetched, feature_set.clone());

    let compute_budget_limits = match process_compute_budget_instructions(
        sanitized_tx.program_instructions_iter(),
        &feature_set,
    ) {
        Ok(limits) => limits,
        Err(e) => {
            return Ok(response(
                slot,
                error_only(e, &config, loaded_addresses, replacement_blockhash),
            ));
        }
    };

    let signature_fee = sanitized_tx
        .num_transaction_signatures()
        .saturating_mul(blockhash_lamports_per_signature);
    let fee_details = FeeDetails::new(
        signature_fee,
        compute_budget_limits.get_prioritization_fee(),
    );
    let budget_and_limits = compute_budget_limits.get_compute_budget_and_limits(
        compute_budget_limits.loaded_accounts_bytes,
        fee_details,
        feature_set.is_active(&agave_feature_set::raise_cpi_nesting_limit_to_8::id()),
    );

    let check_result: TransactionCheckResult = if runnable {
        Ok(CheckedTransactionDetails::new(
            nonce_address,
            budget_and_limits,
        ))
    } else {
        Err(TransactionError::BlockhashNotFound)
    };

    let epoch = solana_epoch_schedule::EpochSchedule::default().get_epoch(slot);
    let output = execute(
        &bank,
        &sanitized_tx,
        slot,
        epoch,
        env_blockhash,
        blockhash_lamports_per_signature,
        &feature_set,
        check_result,
    )?;

    let value = map_result(
        output,
        &config,
        loaded_addresses,
        replacement_blockhash,
        &fallback_accounts,
    )?;
    Ok(response(slot, value))
}

const FEATURE_SET_TTL: Duration = Duration::from_secs(120);

// Feature program id as raw bytes (avoids a cross-crate Pubkey version mismatch).
const FEATURE_PROGRAM_ID: [u8; 32] = [
    3, 192, 160, 205, 203, 6, 210, 218, 239, 174, 130, 209, 111, 238, 122, 207, 97, 236, 115, 123,
    35, 72, 27, 33, 148, 106, 118, 112, 0, 0, 0, 0,
];

// Mirrors Bank::compute_active_feature_set; requires the full unfiltered index.
async fn active_feature_set(
    state: &CloudbreakRpcState,
    slot: Slot,
) -> Result<Arc<FeatureSet>, RpcError> {
    // Scoped so the read guard is dropped before the await below.
    {
        let guard = state.feature_set_cache.read().unwrap();
        if let Some(cached) = guard.as_ref()
            && cached.built_at.elapsed() < FEATURE_SET_TTL
        {
            return Ok(cached.set.clone());
        }
    }

    let feature_set = Arc::new(build_active_feature_set(state, slot).await?);
    *state.feature_set_cache.write().unwrap() = Some(CachedFeatureSet {
        set: feature_set.clone(),
        built_at: Instant::now(),
    });
    Ok(feature_set)
}

async fn build_active_feature_set(
    state: &CloudbreakRpcState,
    slot: Slot,
) -> Result<FeatureSet, RpcError> {
    let feature_ids: Vec<Pubkey> = FEATURE_NAMES.keys().copied().collect();
    let accounts = fetch_accounts(state, slot, &feature_ids).await?;

    let mut feature_set = FeatureSet::default();
    for (feature_id, (account, _account_slot)) in &accounts {
        if account.owner().to_bytes() != FEATURE_PROGRAM_ID {
            continue;
        }
        if let Some(activated_at) = parse_feature_activated_at(account.data())
            && activated_at <= slot
        {
            feature_set.activate(feature_id, activated_at);
        }
    }
    Ok(feature_set)
}

// Feature account `activated_at`: bincode `Option<u64>` — 1 tag byte, then LE slot.
fn parse_feature_activated_at(data: &[u8]) -> Option<u64> {
    if data.len() < 9 || data[0] == 0 {
        return None;
    }
    Some(u64::from_le_bytes(data[1..9].try_into().ok()?))
}

async fn resolve_slot(
    state: &CloudbreakRpcState,
    config: &RpcSimulateTransactionConfig,
) -> Result<u64, RpcError> {
    let commitment = config
        .commitment
        .map(|c| crate::methods::resolve_commitment(c.commitment, state.processed_commitment))
        .transpose()?
        .unwrap_or(CommitmentLevel::Finalized);

    let (slot, _) = state.latest_slot_and_block_time(commitment).await?;

    if let Some(min_context_slot) = config.min_context_slot
        && slot < min_context_slot
    {
        return Err(RpcError::RpcSlotBehindMinContextSlot { rpc_slot: slot });
    }

    Ok(slot)
}

async fn blockhash_is_recent(
    state: &CloudbreakRpcState,
    slot: Slot,
    blockhash: &str,
) -> Result<bool, RpcError> {
    let pool = state.database.get_postgres_connection_pool();
    timeout(state.queries_timeout, async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM \
             (SELECT blockhash FROM recent_blockhashes WHERE slot <= $1 \
              ORDER BY slot DESC LIMIT $2) recent \
             WHERE recent.blockhash = $3)",
        )
        .bind(slot as i64)
        .bind(MAX_PROCESSING_AGE as i64)
        .bind(blockhash)
        .fetch_one(pool)
        .await
    })
    .await
    .map_err(|_| {
        tracing::error!(target: "simtx", "recent_blockhashes query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!(target: "simtx", "recent_blockhashes query error: {e}");
        RpcError::InternalError
    })
}

async fn latest_blockhash(
    state: &CloudbreakRpcState,
    slot: Slot,
) -> Result<Option<(Hash, u64)>, RpcError> {
    let pool = state.database.get_postgres_connection_pool();
    let row = timeout(state.queries_timeout, async {
        sqlx::query(
            "SELECT blockhash, COALESCE(block_height, slot) AS height \
             FROM recent_blockhashes WHERE slot <= $1 ORDER BY slot DESC LIMIT 1",
        )
        .bind(slot as i64)
        .fetch_optional(pool)
        .await
    })
    .await
    .map_err(|_| {
        tracing::error!(target: "simtx", "latest_blockhash query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!(target: "simtx", "latest_blockhash query error: {e}");
        RpcError::InternalError
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let blockhash: String = row
        .try_get("blockhash")
        .map_err(|_| RpcError::InternalError)?;
    let height: i64 = row.try_get("height").map_err(|_| RpcError::InternalError)?;
    let hash = Hash::from_str(&blockhash).map_err(|_| RpcError::InternalError)?;
    Ok(Some((hash, height as u64)))
}

fn resolve_nonce(
    sanitized_tx: &SanitizedTransaction,
    accounts: &HashMap<Pubkey, (AccountSharedData, Slot)>,
) -> Option<(Pubkey, u64)> {
    let message = sanitized_tx.message();
    let nonce_address = message.get_durable_nonce()?;
    let (account, _) = accounts.get(nonce_address)?;
    let data = verify_nonce_account(account, message.recent_blockhash())?;
    message
        .get_ix_signers(0)
        .any(|signer| signer.to_bytes() == data.authority.to_bytes())
        .then_some((*nonce_address, data.fee_calculator.lamports_per_signature))
}

/// Decode the wire transaction per the requested encoding (base58 or base64).
fn decode_transaction(data: &str, encoding: UiTransactionEncoding) -> Result<Vec<u8>, RpcError> {
    match encoding {
        UiTransactionEncoding::Base58 | UiTransactionEncoding::Binary => bs58::decode(data)
            .into_vec()
            .map_err(|e| RpcError::InvalidParamsWithMessage(format!("invalid base58: {e}"))),
        UiTransactionEncoding::Base64 => base64::prelude::BASE64_STANDARD
            .decode(data)
            .map_err(|e| RpcError::InvalidParamsWithMessage(format!("invalid base64: {e}"))),
        other => Err(RpcError::InvalidParamsWithMessage(format!(
            "unsupported transaction encoding: {other:?}"
        ))),
    }
}

fn response(
    slot: u64,
    value: RpcSimulateTransactionResult,
) -> RpcResponse<RpcSimulateTransactionResult> {
    RpcResponse {
        context: RpcResponseContext {
            slot,
            api_version: None,
        },
        value,
    }
}

fn error_only(
    err: TransactionError,
    config: &RpcSimulateTransactionConfig,
    loaded_addresses: UiLoadedAddresses,
    replacement_blockhash: Option<RpcBlockhash>,
) -> RpcSimulateTransactionResult {
    RpcSimulateTransactionResult {
        err: Some(err.into()),
        logs: Some(Vec::new()),
        accounts: config
            .accounts
            .as_ref()
            .map(|c| vec![None; c.addresses.len()]),
        units_consumed: Some(0),
        loaded_accounts_data_size: Some(0),
        return_data: None,
        inner_instructions: None,
        replacement_blockhash,
        fee: None,
        pre_balances: None,
        post_balances: None,
        pre_token_balances: None,
        post_token_balances: None,
        loaded_addresses: Some(loaded_addresses),
    }
}

/// Map the SVM output for the single simulated transaction into the RPC result
fn map_result(
    output: LoadAndExecuteSanitizedTransactionsOutput,
    config: &RpcSimulateTransactionConfig,
    loaded_addresses: UiLoadedAddresses,
    replacement_blockhash: Option<RpcBlockhash>,
    fallback_accounts: &HashMap<Pubkey, (AccountSharedData, Slot)>,
) -> Result<RpcSimulateTransactionResult, RpcError> {
    let (pre_balances, post_balances, pre_token_balances, post_token_balances) =
        extract_balances(output.balance_collector);

    let processing = output
        .processing_results
        .into_iter()
        .next()
        .ok_or(RpcError::InternalError)?;

    let none_accounts = config
        .accounts
        .as_ref()
        .map(|c| vec![None; c.addresses.len()]);

    #[allow(clippy::type_complexity)]
    let (
        err,
        logs,
        accounts,
        units_consumed,
        loaded_accounts_data_size,
        fee,
        return_data,
        inner_instructions,
    ): (
        Option<UiTransactionError>,
        Option<Vec<String>>,
        Option<Vec<Option<UiAccount>>>,
        Option<u64>,
        Option<u32>,
        Option<u64>,
        Option<UiTransactionReturnData>,
        Option<Vec<UiInnerInstructions>>,
    ) = match processing {
        Ok(ProcessedTransaction::Executed(executed)) => {
            let details = &executed.execution_details;
            let err: Option<UiTransactionError> =
                details.status.as_ref().err().cloned().map(Into::into);
            let logs = Some(details.log_messages.clone().unwrap_or_default());
            let return_data = details
                .return_data
                .as_ref()
                .map(|rd| UiTransactionReturnData {
                    program_id: rd.program_id.to_string(),
                    data: (
                        base64::prelude::BASE64_STANDARD.encode(&rd.data),
                        UiReturnDataEncoding::Base64,
                    ),
                });
            let inner_instructions = config
                .inner_instructions
                .then(|| {
                    details
                        .inner_instructions
                        .as_ref()
                        .map(map_inner_instructions)
                })
                .flatten();
            let accounts = if err.is_some() {
                none_accounts
            } else {
                build_requested_accounts(
                    config,
                    &executed.loaded_transaction.accounts,
                    fallback_accounts,
                )
            };
            (
                err,
                logs,
                accounts,
                Some(details.executed_units),
                Some(executed.loaded_transaction.loaded_accounts_data_size),
                Some(executed.loaded_transaction.fee_details.total_fee()),
                return_data,
                inner_instructions,
            )
        }
        Ok(ProcessedTransaction::FeesOnly(fees)) => (
            Some(UiTransactionError::from(TransactionError::clone(
                &fees.load_error,
            ))),
            Some(Vec::new()),
            none_accounts,
            Some(0),
            Some(
                fees.rollback_accounts
                    .iter()
                    .map(|(_, account)| account.data().len())
                    .sum::<usize>() as u32,
            ),
            Some(fees.fee_details.total_fee()),
            None,
            None,
        ),
        Err(e) => (
            Some(UiTransactionError::from(e)),
            Some(Vec::new()),
            none_accounts,
            Some(0),
            Some(0),
            None,
            None,
            None,
        ),
    };

    Ok(RpcSimulateTransactionResult {
        err,
        logs,
        accounts,
        units_consumed,
        loaded_accounts_data_size,
        return_data,
        inner_instructions,
        replacement_blockhash,
        fee,
        pre_balances,
        post_balances,
        pre_token_balances,
        post_token_balances,
        loaded_addresses: Some(loaded_addresses),
    })
}

#[allow(clippy::type_complexity)]
fn extract_balances(
    collector: Option<BalanceCollector>,
) -> (
    Option<Vec<u64>>,
    Option<Vec<u64>>,
    Option<Vec<UiTransactionTokenBalance>>,
    Option<Vec<UiTransactionTokenBalance>>,
) {
    let Some(collector) = collector else {
        return (None, None, None, None);
    };
    let (mut native_pre, mut native_post, mut token_pre, mut token_post) = collector.into_vecs();
    let native = |v: &mut Vec<Vec<u64>>| (!v.is_empty()).then(|| v.swap_remove(0));
    let token = |v: &mut Vec<Vec<SvmTokenInfo>>| {
        (!v.is_empty()).then(|| v.swap_remove(0).iter().map(to_ui_token_balance).collect())
    };
    (
        native(&mut native_pre),
        native(&mut native_post),
        token(&mut token_pre),
        token(&mut token_post),
    )
}

fn to_ui_token_balance(info: &SvmTokenInfo) -> UiTransactionTokenBalance {
    UiTransactionTokenBalance {
        account_index: info.account_index,
        mint: info.mint.to_string(),
        ui_token_amount: ui_token_amount(info.amount, info.decimals),
        owner: OptionSerializer::Some(info.owner.to_string()),
        program_id: OptionSerializer::Some(info.program_id.to_string()),
    }
}

fn ui_token_amount(amount: u64, decimals: u8) -> UiTokenAmount {
    let ui_amount_string =
        rust_decimal::Decimal::try_from_i128_with_scale(amount as i128, decimals as u32)
            .map(|d| d.normalize().to_string())
            .unwrap_or_else(|_| amount.to_string());
    let ui_amount = ui_amount_string.parse::<f64>().ok();
    UiTokenAmount {
        ui_amount,
        decimals,
        amount: amount.to_string(),
        ui_amount_string,
    }
}

/// Map recorded inner instructions; the outer index is the top-level instruction
/// index, and empty inner lists are skipped (matching Agave).
fn map_inner_instructions(
    list: &solana_message::inner_instruction::InnerInstructionsList,
) -> Vec<UiInnerInstructions> {
    list.iter()
        .enumerate()
        .filter(|(_, inner)| !inner.is_empty())
        .map(|(index, inner)| UiInnerInstructions {
            index: index as u8,
            instructions: inner
                .iter()
                .map(|ix| {
                    UiInstruction::Compiled(UiCompiledInstruction::from(
                        &ix.instruction,
                        Some(u32::from(ix.stack_height)),
                    ))
                })
                .collect(),
        })
        .collect()
}

/// Post-execution state of each requested address, or `None` if the tx did not load it
fn build_requested_accounts(
    config: &RpcSimulateTransactionConfig,
    post_accounts: &[(Pubkey, AccountSharedData)],
    fallback_accounts: &HashMap<Pubkey, (AccountSharedData, Slot)>,
) -> Option<Vec<Option<UiAccount>>> {
    let accounts_config = config.accounts.as_ref()?;
    let encoding = accounts_config
        .encoding
        .unwrap_or(UiAccountEncoding::Base64);
    let out = accounts_config
        .addresses
        .iter()
        .map(|addr| {
            let pubkey: Pubkey = addr.parse().ok()?;
            let account = post_accounts
                .iter()
                .find(|(key, _)| key == &pubkey)
                .map(|(_, acc)| acc)
                .or_else(|| fallback_accounts.get(&pubkey).map(|(acc, _)| acc))?;
            Some(encode_ui_account(&pubkey, account, encoding, None, None))
        })
        .collect();
    Some(out)
}

/// In-memory account store the SVM reads through; the callback only serves what was pre-fetched here
struct SimulationBank {
    accounts: HashMap<Pubkey, (AccountSharedData, Slot)>,
    feature_set: FeatureSet,
}

impl SimulationBank {
    fn new(accounts: HashMap<Pubkey, (AccountSharedData, Slot)>, feature_set: FeatureSet) -> Self {
        Self {
            accounts,
            feature_set,
        }
    }
}

impl InvokeContextCallback for SimulationBank {
    fn is_precompile(&self, program_id: &Pubkey) -> bool {
        agave_precompiles::is_precompile(program_id, |feature_id| {
            self.feature_set.is_active(feature_id)
        })
    }

    fn process_precompile(
        &self,
        program_id: &Pubkey,
        data: &[u8],
        instruction_datas: Vec<&[u8]>,
    ) -> Result<(), PrecompileError> {
        match agave_precompiles::get_precompile(program_id, |feature_id| {
            self.feature_set.is_active(feature_id)
        }) {
            Some(precompile) => precompile.verify(data, &instruction_datas, &self.feature_set),
            None => Err(PrecompileError::InvalidPublicKey),
        }
    }
}

impl TransactionProcessingCallback for SimulationBank {
    fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)> {
        self.accounts.get(pubkey).cloned()
    }
}

/// Resolves Address Lookup Tables from a pre-fetched `table -> addresses` map.
/// Cheap to clone (the trait takes `self` by value); the map is behind an `Arc`.
#[derive(Clone)]
struct SimulationAddressLoader {
    tables: Arc<HashMap<Pubkey, Vec<Pubkey>>>,
}

impl SimulationAddressLoader {
    fn new(tables: HashMap<Pubkey, Vec<Pubkey>>) -> Self {
        Self {
            tables: Arc::new(tables),
        }
    }
}

impl AddressLoader for SimulationAddressLoader {
    fn load_addresses(
        self,
        lookups: &[MessageAddressTableLookup],
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        let mut loaded = LoadedAddresses::default();
        for lookup in lookups {
            let table = self
                .tables
                .get(&lookup.account_key)
                .ok_or(AddressLoaderError::LookupTableAccountNotFound)?;
            for &index in &lookup.writable_indexes {
                loaded.writable.push(
                    *table
                        .get(index as usize)
                        .ok_or(AddressLoaderError::InvalidLookupIndex)?,
                );
            }
            for &index in &lookup.readonly_indexes {
                loaded.readonly.push(
                    *table
                        .get(index as usize)
                        .ok_or(AddressLoaderError::InvalidLookupIndex)?,
                );
            }
        }
        Ok(loaded)
    }
}

async fn fetch_accounts(
    state: &CloudbreakRpcState,
    slot_bound: u64,
    keys: &[Pubkey],
) -> Result<HashMap<Pubkey, (AccountSharedData, Slot)>, RpcError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }

    let template = include_str!("../db/getMultipleAccounts.sql");

    let mut array_literal = String::with_capacity(keys.len() * 95 + 8);
    array_literal.push_str("ARRAY[");
    for (i, pk) in keys.iter().enumerate() {
        if i > 0 {
            array_literal.push_str(", ");
        }
        array_literal.push_str(&format!("'\\x{}'::bytea", hex::encode(pk.as_ref())));
    }
    array_literal.push(']');

    let sql = template.replace("$1", &array_literal);
    let sql = sql.replace("$2", &slot_bound.to_string());
    let sql = db_query::add_trace_traceparent_to_query(&sql);

    let pool = state.database.get_postgres_connection_pool();
    let rows = timeout(state.queries_timeout, async {
        let span = tracing::info_span!("simtx_accounts_db");
        sqlx::raw_sql(&sql).fetch_all(pool).instrument(span).await
    })
    .await
    .map_err(|_elapsed| {
        tracing::error!(target: "simtx", "simulateTransaction account query timed out");
        RpcError::InternalError
    })?
    .map_err(|e| {
        tracing::error!(target: "simtx", "simulateTransaction account query error: {e}");
        RpcError::InternalError
    })?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in &rows {
        let pubkey_bytes: Vec<u8> = row.get("pubkey");
        let Ok(pubkey) = Pubkey::try_from(pubkey_bytes.as_slice()) else {
            continue;
        };
        let owner_bytes: Vec<u8> = row.get("owner");
        let owner =
            Pubkey::try_from(owner_bytes.as_slice()).map_err(|_| RpcError::InternalError)?;
        let lamports = row.get::<i64, _>("lamports") as u64;
        let slot = row.get::<i64, _>("slot") as u64;
        let executable: bool = row.get("executable");
        let rent_epoch = row
            .get::<rust_decimal::Decimal, _>("rent_epoch")
            .to_u64()
            .unwrap_or(0);
        let data: Vec<u8> = row.get("data");

        let account = AccountSharedData::create_from_existing_shared_data(
            lamports,
            Arc::new(data),
            owner,
            executable,
            rent_epoch,
        );
        map.insert(pubkey, (account, slot));
    }

    Ok(map)
}

/// Fetch the lookup-table accounts and decode each into its ordered address list.
async fn fetch_lookup_tables(
    state: &CloudbreakRpcState,
    slot_bound: u64,
    table_keys: &[Pubkey],
) -> Result<HashMap<Pubkey, Vec<Pubkey>>, RpcError> {
    let accounts = fetch_accounts(state, slot_bound, table_keys).await?;
    let mut tables = HashMap::with_capacity(accounts.len());
    for (key, (account, _)) in &accounts {
        let table = AddressLookupTable::deserialize(account.data()).map_err(|e| {
            RpcError::InvalidParamsWithMessage(format!("invalid address lookup table {key}: {e}"))
        })?;
        tables.insert(*key, table.addresses.to_vec());
    }
    Ok(tables)
}

/// Sysvar accounts the SVM sysvar cache reads through the callback. Fetched
/// explicitly since transactions rarely list them; the Instructions sysvar is
/// synthesised per-tx by the runtime and is not fetched.
fn sysvar_account_ids() -> [Pubkey; 9] {
    [
        Pubkey::from_str_const("SysvarC1ock11111111111111111111111111111111"),
        Pubkey::from_str_const("SysvarEpochSchedu1e111111111111111111111111"),
        Pubkey::from_str_const("SysvarEpochRewards1111111111111111111111111"),
        Pubkey::from_str_const("SysvarRent111111111111111111111111111111111"),
        Pubkey::from_str_const("SysvarS1otHashes111111111111111111111111111"),
        Pubkey::from_str_const("SysvarStakeHistory1111111111111111111111111"),
        Pubkey::from_str_const("SysvarLastRestartS1ot1111111111111111111111"),
        Pubkey::from_str_const("SysvarRecentB1ockHashes11111111111111111111"),
        Pubkey::from_str_const("SysvarFees111111111111111111111111111111111"),
    ]
}

/// ProgramData addresses of upgradeable programs in the fetched map (their ELF
/// lives in a separate account not listed by the transaction).
fn programdata_addresses(accounts: &HashMap<Pubkey, (AccountSharedData, Slot)>) -> Vec<Pubkey> {
    let loader = solana_sdk_ids::bpf_loader_upgradeable::id();
    let mut out = Vec::new();
    for (_key, (account, _)) in accounts {
        if account.owner() != &loader {
            continue;
        }
        if let Ok(UpgradeableLoaderState::Program {
            programdata_address,
        }) = bincode::deserialize::<UpgradeableLoaderState>(account.data())
        {
            out.push(programdata_address);
        }
    }
    out
}

// Engine: stand up a `TransactionBatchProcessor` (builtins, program-runtime environment, sysvar cache) over the pre-fetched bank and execute read-only.

struct SimulationForkGraph;

impl ForkGraph for SimulationForkGraph {
    fn relationship(&self, a: Slot, b: Slot) -> BlockRelation {
        match a.cmp(&b) {
            std::cmp::Ordering::Less => BlockRelation::Ancestor,
            std::cmp::Ordering::Equal => BlockRelation::Equal,
            std::cmp::Ordering::Greater => BlockRelation::Descendant,
        }
    }
}

/// Build the processor and execute the transaction read-only; nothing is committed.
#[allow(clippy::too_many_arguments)]
fn execute(
    bank: &SimulationBank,
    sanitized_tx: &SanitizedTransaction,
    slot: Slot,
    epoch: Epoch,
    blockhash: Hash,
    blockhash_lamports_per_signature: u64,
    feature_set: &FeatureSet,
    check_result: TransactionCheckResult,
) -> Result<LoadAndExecuteSanitizedTransactionsOutput, RpcError> {
    let svm_feature_set = feature_set.runtime_features();
    let simd_0268 = feature_set.is_active(&agave_feature_set::raise_cpi_nesting_limit_to_8::id());

    let program_runtime_env = create_program_runtime_environment(
        &svm_feature_set,
        &SVMTransactionExecutionBudget::new_with_defaults(simd_0268),
        false,
        false,
    )
    .map_err(|e| {
        tracing::error!(target: "simtx", "failed to create program runtime env: {e}");
        RpcError::InternalError
    })?;

    let fork_graph = Arc::new(RwLock::new(SimulationForkGraph));
    let mut processor =
        TransactionBatchProcessor::<SimulationForkGraph>::new_uninitialized(slot, epoch);
    processor.set_execution_cost(SVMTransactionExecutionCost::default());
    {
        let mut cache = processor.global_program_cache.write().unwrap();
        cache.set_fork_graph(Arc::downgrade(&fork_graph));
        cache.latest_root_slot = slot;
    }

    processor.set_program_runtime_environment(program_runtime_env);

    for builtin in solana_builtins::BUILTINS {
        processor.add_builtin(
            builtin.program_id,
            ProgramCacheEntry::new_builtin(0, builtin.name.len(), builtin.register_fn),
        );
    }

    processor.fill_missing_sysvar_cache_entries(bank);

    let environment = TransactionProcessingEnvironment {
        blockhash,
        blockhash_lamports_per_signature,
        alpenglow_migration_succeeded: false,
        epoch_total_stake: 0,
        feature_set: svm_feature_set,
        program_runtime_environments: ProgramRuntimeEnvironments::new(
            processor.program_runtime_environment_for_epoch(epoch),
            processor.program_runtime_environment_for_epoch(epoch),
        ),
        rent: solana_rent::Rent::default(),
    };

    let config = TransactionProcessingConfig {
        account_overrides: None,
        check_program_deployment_slot: false,
        log_messages_bytes_limit: None,
        limit_to_load_programs: true,
        recording_config: ExecutionRecordingConfig::new_single_setting(true),
        drop_on_failure: false,
        all_or_nothing: false,
        strict_nonce_size_check: false,
    };

    Ok(processor.load_and_execute_sanitized_transactions(
        bank,
        std::slice::from_ref(sanitized_tx),
        vec![check_result],
        &environment,
        &config,
    ))
}
