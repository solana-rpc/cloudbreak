# Integration Tests

Benchmarking and correctness testing tool for Solana RPC endpoints.

## Features

- **Constant-rate load generation**: Spawns requests at a configurable RPS rate decoupled from response latency. A ticker fires at the target interval and dispatches requests regardless of how long previous ones take, with a semaphore limiting maximum in-flight concurrency.
- **Bandwidth limit (Gbit/s)**: An optional `[benchmark].target_gbits` cap throttles load based on the **actual received rpc1 response bytes**, acting as a second ceiling alongside `target_rps`. If byte throughput reaches the cap below the target RPS, the spawner slows down so the average stays under the limit — useful for reproducing a link/NIC saturation ceiling instead of a pure request-rate one. Implemented as a debt-based token bucket over real wire bytes (no estimation). The run's **average Gbit/s** is always reported in the summary next to Effective RPS, whether or not a cap is set.
- **Response comparison**: Optionally sends the same request to a second endpoint and compares the responses. Comparison is order-agnostic — accounts are matched by pubkey, so different ordering between endpoints is not flagged as a mismatch.
- **Slot compensation**: When two endpoints return different data, the tool checks if the difference is due to a slot lag. If enabled, it fires concurrent requests to **both** endpoints at regular intervals, collecting all responses. It then searches for the first pair (one from each endpoint) that share the same slot, and uses that pair for comparison. This avoids the ping-pong problem where sequential retries on the behind endpoint can never converge because the chain keeps advancing (especially with `processed` commitment). The initial slot difference is still recorded in statistics. **Important**: slot compensation only activates when both responses include `result.context.slot` (i.e. the request was made with `withContext: true`). If the responses have no context, mismatches are reported as-is with no retries. For `getProgramAccounts`, context is only present if the request explicitly includes `withContext: true`; `getTokenAccountsByOwner` and `getTokenAccountsByDelegate` always return context.
- **Live request sources**: Requests can come from a static JSON file or be fetched live from VictoriaLogs. The VictoriaLogs source refreshes every 60 seconds in the background, so the request pool evolves during long runs.
- **Automatic context injection**: When `inject_context = true` on a VictoriaLogs source, the tool automatically retries no-context mismatches by re-sending the request with `withContext: true` injected and running slot compensation. Only after this second attempt does it decide whether the mismatch is real — suppressing false positives caused by slot lag on requests that were originally sent without context.
- **Latency histograms**: At the end of a run, prints avg, P50, P90, P99 latencies broken down by endpoint name, response size category, and encoding.
- **Correctness summary**: Reports match/mismatch counts, the percentage of responses that include context slots, and a histogram of slot differences between endpoints. Mismatches where both responses lack context are tracked separately as "no-context mismatches" since the difference may be caused by unverifiable slot lag.
- **Granular logging**: Uses `tracing` with per-target filtering (`bench_request`, `bench_compare::*`, `bench_source`, `bench_sample`) so you can tune verbosity with `RUST_LOG` without touching code. The `[print_config]` TOML section can also inject `<target>=off` directives that **override** `RUST_LOG` via `EnvFilter`'s longest-target-prefix rule — letting you silence specific event classes (matches, mismatches, rescues, errors, …) from config alone.
- **Print filtering**: A `[print_config]` section lets you suppress noise by setting minimum thresholds on response size, duration, and account count — all three must be met for a log line to appear.
- **Periodic full-payload sampling**: `[print_config].sample_every_secs` periodically dumps one full `(request, response1, response2)` tuple via the `bench_sample` tracing target, throttled atomically across all concurrent tasks, so you can eyeball what's actually being compared without flooding the logs.
- **Retry in place**: `[retry_in_place]` re-fires a request once and only counts it as a mismatch if the retry also fails. Two modes — on-mismatch (immediate rescue) and scheduled (`retry_after_ms` fires a retry at exactly `original_send_time + N` ms for every request, doubling traffic, to probe how much temporal inconsistency remains as a function of time). Optional `save_rescued = true` dumps every recovered request to a `rescued_<program_id>_<ts>.json` for offline inspection.
- **Slot-compensation iteration logs**: `[comparison].save_compensation_iterations = true` records every individual `rpc1+rpc2` send that happened during slot compensation (and the no-context retry) into the saved mismatch/rescue file under an `iterations: [...]` array, each entry timestamped with `fired_at` (ISO-8601 with ms) and a `phase` tag. Lets you separate "transient slot-lag, eventually picked a matching pair" from "stable same-slot disagreement that slot compensation papered over".
- **Per-iteration DB cross-check (`getBalance` only)**: `[comparison].save_db_probe_iterations = true` adds a third arm to the `tokio::join!` of every iteration: a SQL probe against `[db_check].db_url` that captures the contents of the `slots` table plus the top-20 newest `(slot, lamports)` rows for the queried pubkey from `accounts` and `snapshot_accounts`. Embedded as `db_probe: {...}` on each iteration entry — lets you see directly whether the DB has a row at the slot the RPC returned, ruling slot compensation in or out as the cause of a mismatch.

## How Comparison Works

1. Every request is sent to `rpc1` and its latency/size are recorded.
2. Based on the configured `ratio`, a random fraction of requests are also sent to `rpc2`.
3. Both responses are compared:
   - **Context check**: Verifies both responses either have or lack a `result.context` field. If one has context and the other doesn't, it's a mismatch.
   - **Account comparison**: Builds a pubkey-keyed map of accounts from both responses and checks that every pubkey exists in both with identical account data. Order does not matter.
   - **zstd decompression**: When the request encoding is `base64+zstd`, compressed account data is decompressed before comparison. This is necessary because zstd compression is non-deterministic — different implementations (or even different versions of the same library) can produce different compressed bytes for identical input data. Without decompression, every `base64+zstd` response would be flagged as a mismatch even when the actual account data is byte-for-byte identical. For non-zstd encodings (`base64`, `base58`, `jsonParsed`), the raw JSON values are compared directly with no overhead.
4. If the responses differ and `enable_slot_compensation` is on, the tool checks if both responses have `result.context.slot`. If they do and the slots differ, it fires concurrent requests to **both** endpoints at each retry interval, collecting all responses. It then finds the first pair (one from each endpoint) that share the same slot, and uses that pair for the final comparison. If no slot match is found after all retries, it falls back to the latest responses from each. If either response lacks context, slot compensation is skipped entirely.
5. **Context injection retry**: If `inject_context = true` is set on the source and the initial comparison is a no-context mismatch (both responses lack `result.context`), the tool clones the request, injects `withContext: true`, re-sends it to both endpoints, and runs full slot compensation. Only after this retry does it decide match/mismatch — the warning log, mismatch file, and stats are all based on the final result. This eliminates false positives caused by slot lag on requests that were originally sent without context.
6. Mismatches where both responses lack context (no `result.context.slot`) and `inject_context` is not enabled are classified as **no-context mismatches**. These are still saved to disk, but logged with a ⚠️ warning icon instead of ❌ and counted separately in the summary, since the difference may be caused by slot lag that cannot be verified or compensated for.
7. All mismatches (after compensation, if enabled) can be saved to disk as JSON files containing both responses and the original request, for later analysis.

### Slot monitoring

Every compared request records the initial slot difference (`rpc1 slot - rpc2 slot`) from the `result.context.slot` field. This value is captured **before** any slot compensation retries, so the summary always reflects the real-time lag between endpoints. At the end of the run, the summary reports:

- What percentage of compared responses included context slots.
- A histogram of non-zero slot differences, bucketed into `rpc1 behind by >5`, `behind by 1-5`, `ahead by 1-5`, and `ahead by >5`, with counts and percentages.

This lets you quickly see if one endpoint is consistently lagging behind the other in slot progression, and by how much.

## Commands

### `benchmark`

The main command. Sends RPC requests at a constant rate to one or two endpoints, collects latency statistics, and optionally compares responses for correctness.

```sh
cargo run --bin integration_tests -- benchmark gpa
cargo run --bin integration_tests -- benchmark gtabo
cargo run --bin integration_tests -- benchmark gtabd
cargo run --bin integration_tests -- benchmark gpa-token-owner
cargo run --bin integration_tests -- benchmark gpa-token-mint
cargo run --bin integration_tests -- benchmark get-account-info
cargo run --bin integration_tests -- benchmark get-multiple-accounts
cargo run --bin integration_tests -- benchmark get-balance
cargo run --bin integration_tests -- benchmark get-token-account-balance
cargo run --bin integration_tests -- benchmark simulate-transaction
cargo run --bin integration_tests -- benchmark -c custom.toml gpa
```

**Arguments:**

| Argument              | Description                                                                                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<request_type>`      | Required. One of: `gpa`, `gtabo`, `gtabd`, `gpa-token-owner`, `gpa-token-mint`, `get-account-info`, `get-multiple-accounts`, `get-balance`, `get-token-account-balance`, `simulate-transaction`                       |
| `-c, --config <path>` | Path to TOML config file (default: `cloudbreak.integration_tests.toml`)                                                                                                                       |

**Request types:**

| Type                         | RPC Method                                                                              |
| ---------------------------- | --------------------------------------------------------------------------------------- |
| `gpa`                        | `getProgramAccounts`                                                                    |
| `gtabo`                      | `getTokenAccountsByOwner`                                                               |
| `gtabd`                      | `getTokenAccountsByDelegate`                                                            |
| `gpa-token-owner`            | `getProgramAccounts` (token owner filter, offset 32)                                    |
| `gpa-token-mint`             | `getProgramAccounts` (token mint filter, non-32 offset)                                 |
| `get-account-info`           | `getAccountInfo` (single pubkey)                                                        |
| `get-multiple-accounts`      | `getMultipleAccounts` (up to `[server].max-multiple-accounts` pubkeys, default `100`; positional comparison) |
| `get-balance`                | `getBalance` (returns `u64`, no encoding)                                                |
| `get-token-account-balance`  | `getTokenAccountBalance` (returns `UiTokenAmount`, no encoding, no `minContextSlot`)    |
| `simulate-transaction`       | `simulateTransaction` (replays real captured requests; see [Replaying simulateTransaction](#replaying-simulatetransaction)) |

**Per-method response comparison** (when `[comparison]` is configured):

| Type                          | Comparison shape                                                                                                |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `gpa` / `gtabo` / `gtabd` / `gpa-token-*` | Array of `{pubkey, account}` — unordered, pubkey-keyed                                            |
| `get-multiple-accounts`       | Positional array of `UiAccount \| null` — order is meaningful (matches the request's pubkey order)              |
| `get-account-info`            | Single `UiAccount \| null` — direct compare with zstd-aware account-equality                                    |
| `get-balance`                 | `u64` — direct JSON equality                                                                                    |
| `get-token-account-balance`   | `UiTokenAmount` object — direct JSON equality                                                                    |
| `simulate-transaction`        | `result.value` field-by-field: `err`, `logs`, `unitsConsumed`, `loadedAccountsDataSize`, `returnData`, `fee`, `loadedAddresses`, `innerInstructions`, `accounts`, `pre`/`postBalances`; `pre`/`postTokenBalances` are `(accountIndex, mint)`-keyed so ordering is ignored. `replacementBlockhash` is compared only for presence, since each endpoint substitutes its own blockhash. |

**Default encodings used by `extract_encoding_from_request` when the request omits `encoding`** (mirrors Agave's per-method defaults):

| Type                          | Default encoding                                                                                                |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `gpa` / `gpa-token-*`         | `base58`                                                                                                        |
| `gtabo` / `gtabd`             | `jsonParsed`                                                                                                    |
| `get-account-info`            | `binary` (deprecated base58 plain-string; Agave's default)                                                       |
| `get-multiple-accounts`       | `base64`                                                                                                        |
| `get-balance` / `get-token-account-balance` | `none` (these methods have no `encoding` field)                                                   |

### `compare` (legacy)

Compares the full set of pubkeys returned by `getProgramAccounts` between two endpoints, then checks transaction history for any differences.

```sh
cargo run --bin integration_tests -- compare
cargo run --bin integration_tests -- compare -c custom.toml
```

### `get-slot` (legacy)

Polls `getSlot` on rpc1 every 100ms and prints the result. Useful for monitoring slot progression.

```sh
cargo run --bin integration_tests -- get-slot
cargo run --bin integration_tests -- get-slot -c custom.toml
```

## Configuration

All configuration is read from a TOML file. See `example.cloudbreak.integration_tests.toml` for a full example.

### `[rpc1]` (required)

The primary endpoint to benchmark.

| Field  | Description                           |
| ------ | ------------------------------------- |
| `url`  | RPC endpoint URL                      |
| `name` | Display name used in logs and reports |

### `[rpc2]` (optional)

Second endpoint, required only when `[comparison]` is set.

| Field  | Description                           |
| ------ | ------------------------------------- |
| `url`  | RPC endpoint URL                      |
| `name` | Display name used in logs and reports |

### `[benchmark]`

Controls load generation.

| Field           | Default      | Description                                                                                     |
| --------------- | ------------ | ----------------------------------------------------------------------------------------------- |
| `target_rps`    | _(required)_ | Target requests per second. Requests are spawned at this rate independently of response latency |
| `max_in_flight` | `100`        | Maximum concurrent in-flight requests. Requests beyond this limit are dropped                   |
| `duration_secs` | `0`          | How long to run in seconds. `0` exits immediately (should be set)                               |
| `target_gbits`  | _(none)_     | Optional bandwidth cap in **Gbit/s**, enforced against the actual received rpc1 response bytes. Works together with `target_rps`: if throughput hits this limit below the target RPS, effective RPS is throttled so the average stays under the cap. Omit (or `0`) to disable — the average Gbit/s is still reported either way |

### `[source]`

Where to load request bodies from. Uses a tagged union (`type` field).

#### `type = "json_file"`

Loads a static array of JSON-RPC request bodies from a file. Requests cycle indefinitely. The file can contain any valid JSON-RPC request bodies (`getProgramAccounts`, `getTokenAccountsByOwner`, or any other method) — they are sent as-is to the endpoints without transformation. The `request_type` CLI argument is only used for encoding extraction and VictoriaLogs query filtering, not to validate or modify the request payload.

| Field  | Description                                                |
| ------ | ---------------------------------------------------------- |
| `path` | Path to a JSON file containing an array of request objects |

#### `type = "victoria_logs"`

Fetches real request bodies from a VictoriaLogs instance. Refreshes automatically every `poll_interval_secs` (default 60) during the benchmark run with fresh logs.

| Field                | Default      | Description                                                                                                                                                                                                                                                        |
| -------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `url`                | _(required)_ | VictoriaLogs query endpoint URL                                                                                                                                                                                                                                    |
| `minutes`            | _(none)_     | Time window in minutes for the log query. If omitted, no time constraint is applied                                                                                                                                                                                |
| `window_seconds`     | _(none)_     | Trailing time window in seconds (`_time:>now-Ns`). Takes precedence over `minutes`. Use for request types that expire quickly                                                                                                                                       |
| `limit`              | `1000`       | Maximum number of log entries to fetch per query                                                                                                                                                                                                                   |
| `min_request_size`   | _(none)_     | Only include requests where the response was at least this many bytes                                                                                                                                                                                              |
| `max_request_size`   | _(none)_     | Only include requests where the response was at most this many bytes                                                                                                                                                                                               |
| `encoding`           | _(none)_     | Filter requests by encoding. See [VictoriaLogs encoding filter](#victorialogs-encoding-filter) below. Supports `\|` for multiple (e.g. `"base58\|jsonParsed"`) |
| `inject_context`     | `false`      | If `true`, when a no-context mismatch is detected, re-sends the request with `withContext: true` injected and runs slot compensation before deciding if it's a real mismatch. Eliminates false positives from slot lag on requests originally sent without context |
| `poll_interval_secs` | `60`         | How often the background fetcher re-queries VictoriaLogs                                                                                                                                                                                                            |
| `replay_once`        | `false`      | Deduplicate by the log's `req_id` and send each request exactly once instead of cycling the pool. Required for requests that expire                                                                                                                                 |

##### Replaying simulateTransaction

A `simulateTransaction` request carries a `recentBlockhash` that the cluster only accepts for
~150 blocks (~60s). VictoriaLogs ingestion lag is 1–9s, so a request must be replayed within
seconds of being logged. Three settings make that work, and all three are required:

- `window_seconds = 30` — a short trailing window instead of whole minutes.
- `poll_interval_secs = 5` — fetch new arrivals promptly.
- `replay_once = true` — send each `req_id` once. Without it the spawner cycles the pool at
  `target_rps`, replaying the same transactions until their blockhashes expire.

Pair with `[benchmark].start_on_first_request = true` so the `duration_secs` countdown begins
when the first request is picked up rather than while waiting for the first poll.

Two known, non-bug divergence classes when comparing against an Agave validator:

- **`AlreadyProcessed` on the Agave side.** Clients typically submit the transaction a second
  after simulating it, so by replay time it has landed and Agave's signature status cache
  rejects it. Cloudbreak has no status cache and re-executes. Only affects requests that do
  *not* set `replaceRecentBlockhash: true` (replacing the blockhash changes the status-cache key).
- **`BlockhashNotFound` on one side only** when a request's original blockhash expires mid-run.

##### VictoriaLogs tips

**Broad queries without time constraints**: Omit the `minutes` field to remove the time window from the VictoriaLogs query entirely. This fetches logs from any time period (up to `limit`), which is useful when you want a larger and more diverse set of requests rather than just recent traffic:

```toml
[source]
type = "victoria_logs"
url = "http://victoria-logs:9428/select/logsql/query"
# minutes omitted — no time constraint, fetches up to `limit` entries from any time
limit = 5000
encoding = "base64"
```

When `minutes` is set, the query looks at the window `[now - minutes, now - (minutes - 1)]` (e.g., `minutes = 2` queries logs from 2 minutes ago to 1 minute ago). The background refresher still runs every 60 seconds regardless.

##### VictoriaLogs encoding filter

The `encoding` field applies a two-stage filter:

1. **VictoriaLogs regex** (coarse): The query includes `body:~'"encoding":\s*"<value>"'` to pre-filter on the server side. This is fast but can produce **false positives** — for example, a request with `"encoding": "base58"` inside a `memcmp` filter object would match a regex search for `base58`, even though the actual response encoding is `base64`.

2. **Rust-side validation** (exact): After fetching results, each request body is parsed and the actual response encoding is extracted from the correct parameter position (`params[1].encoding` for GPA / gAI / gMA, `params[2].encoding` for GTABO/GTABD). Requests whose actual encoding doesn't match the filter are discarded. If a request has no explicit encoding, the Agave default for that method is used (`base58` for GPA, `jsonParsed` for GTABO/GTABD, `binary` for `getAccountInfo`, `base64` for `getMultipleAccounts`). `getBalance` and `getTokenAccountBalance` carry no encoding at all — the helper reports `none` for them and they pass any `encoding` filter unchanged (i.e. the filter is effectively a no-op).

This means the `encoding` filter is always accurate — the regex is just a performance optimization to reduce data transferred from VictoriaLogs. You can use `|` to match multiple encodings (e.g. `"base58|jsonParsed"`), which works in both the regex and the Rust-side filter.

#### `type = "mismatch_dir"`

Reads all `mismatch_*.json` files from a directory (typically the output of a previous benchmark run with `save_mismatches = true`) and extracts the `request` field from each. This lets you re-run only the requests that previously produced mismatches to verify if the differences persist.

| Field            | Default                                              | Description                                                                                                                                                                      |
| ---------------- | ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`           | `crates/integration_tests/compare_responses_results` | Directory containing mismatch JSON files                                                                                                                                         |
| `inject_context` | `false`                                              | If `true`, injects `withContext: true` into each request's params. Useful for GPA requests that were originally sent without context, so that slot compensation works on re-runs |

### `[comparison]` (optional)

Enables response correctness checking by sending the same request to both `rpc1` and `rpc2` and comparing the results. If this section is omitted, only `rpc1` is used (pure load testing).

| Field                           | Default                                              | Description                                                                                                                                               |
| ------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ratio`                         | `1.0`                                                | Fraction of requests that are also sent to rpc2 for comparison (`0.0` to `1.0`). E.g. `0.8` means ~80% of requests are compared                           |
| `enable_slot_compensation`      | _(required)_                                         | If `true`, when responses differ and slots don't match, fires concurrent requests to both endpoints until a slot-matching pair is found, then re-compares |
| `slot_compensation_max_retries` | `50`                                                 | Maximum number of concurrent retry rounds during slot compensation                                                                                        |
| `slot_compensation_interval_ms` | `100`                                                | Milliseconds to wait between slot compensation retry rounds                                                                                               |
| `save_mismatches`               | `true`                                               | Write mismatched response pairs to disk as JSON files                                                                                                     |
| `save_compensation_iterations`  | `false`                                              | When `true`, every saved `mismatch_*.json` / `rescued_*.json` file also contains an `iterations: [...]` array on each block (original & retry) capturing every `rpc1+rpc2` send that happened during slot compensation and the no-context retry, each with its own ISO-8601-with-ms `fired_at` and a `phase` tag (`"initial"` / `"with_context_retry"`). Pre-existing top-level fields are preserved; the iterations array is additive. |
| `save_db_probe_iterations`      | `false`                                              | `getBalance` only. When `true` AND `[db_check]` is configured, every `iterations[]` entry gets an additional `db_probe: {...}` field built from a SQL query fired as the third arm of the same `tokio::join!` as rpc1/rpc2. Captures the `slots` table contents and the top-20 newest `(slot, lamports)` rows for the queried pubkey from `accounts` + `snapshot_accounts`. Requires `save_compensation_iterations = true` to actually appear on disk. |
| `mismatch_output_dir`           | `crates/integration_tests/compare_responses_results` | Directory for mismatch output files                                                                                                                       |

### `[db_check]` (optional)

When present, enables per-account database inspection on mismatches. After slot compensation, if responses still differ, the tool identifies which accounts have different data and queries both the `accounts` and `snapshot_accounts` Postgres tables for each one (using the latest slot across both), printing the DB slot, lamports, and how far behind the response context it is. Optionally, it can also call `getSignaturesForAddress` on an RPC endpoint (typically Agave) to find the slot of the last transaction for each differing account — this acts as the Agave source of truth for when the account was last updated.

| Field                | Default  | Description                                                                                                                                                                                              |
| -------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_url`             | _(req)_  | Postgres connection string (e.g. `postgres://user:pw@host/db`)                                                                                                                                           |
| `rpc_url`            | _(none)_ | RPC endpoint for `getSignaturesForAddress` calls (e.g. the Agave validator URL)                                                                                                                          |
| `get_last_signature` | `false`  | If `true` (and `rpc_url` is set), calls `getSignaturesForAddress` for each differing account and lists all transaction slots newer than what the DB has — showing exactly which transactions were missed |

The output uses the `bench_db_check` tracing target at `INFO` level. Example:

```
 INFO bench_db_check: 🔍 3 differing accounts | response slots: Cloudbreak = 312456789 | Agave = 312456787 | db confirmed: 312456790 | db finalized: 312456750
 INFO bench_db_check:   5Kd3NBUq...abc | mismatched data | db slot: 312456789 (0 slots behind) | lamports: 58320881436
 INFO bench_db_check:     └─ no missed txs | latest tx slot: 312456789 (0 slots behind)
 INFO bench_db_check:   7Hj2PQmx...def | only in Cloudbreak | db slot: 312456700 (89 slots behind) | lamports: 63767191
 INFO bench_db_check:     └─ 3 missed txs at slots: [312456750, 312456780, 312456788] | latest tx slot: 312456788 (1 slots behind)
 INFO bench_db_check:   9Xn4RTgy...ghi | only in Agave | not in db
 INFO bench_db_check:     └─ 1 missed txs at slots: [312456785] | no txs found via RPC
```

The first line shows the DB's confirmed and finalized slot from the `slots` table. Each account line shows:
- The account pubkey
- The diff kind: `mismatched data` (both endpoints returned the account but data differs), `only in <name>` (account only present in one endpoint's response)
- The DB slot (from the latest row across both `accounts` and `snapshot_accounts` tables), how far behind the response context it is, and the account lamports. If the account is not in the DB, it shows `not in db`

The `└─` sub-line (when `get_last_signature = true`) shows:
- The number of missed transactions (on-chain tx slots newer than the DB slot for that account)
- The latest tx slot seen via `getSignaturesForAddress` and how far behind the response context it is. If no signatures are found, it shows `no txs found via RPC`. If there are no missed transactions, the account data may have been changed via a CPI or program invocation that doesn't produce a direct signature for that address.

### `[retry_in_place]` (optional)

Re-runs a request once and only counts it as a mismatch if the retry also mismatches. The retry goes through the full comparison pipeline (rpc1+rpc2, slot compensation, no-context retry), so a "rescue" is a real same-slot match — not a temporal coincidence.

Two modes (controlled by whether `retry_after_ms` is set):

| Field            | Default  | Description                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`        | `false`  | Master switch. When `false` the feature is fully off regardless of `retry_after_ms`                                                                                                                                                                                                                                                                                                               |
| `retry_after_ms` | _(none)_ | If set, schedules a retry for **every** request fired at exactly `original_send_time + retry_after_ms`. Often fires while the original is still in flight. ⚠️ **Doubles request volume** on both endpoints — lower `target_rps` accordingly. If unset and `enabled = true`, the retry only fires when the original comparison mismatched ("rescue only" mode, no extra traffic on the happy path) |
| `save_rescued`   | `false`  | When `true`, writes a `rescued_<program_id>_<timestamp>.json` file into `comparison.mismatch_output_dir` for every request where the retry flipped the verdict from mismatch → match. Same on-disk schema as a `mismatch_*.json` (both `original` and `retry` blocks) so the file is replayable through the `mismatch_dir` source. Independent of `comparison.save_mismatches`.                    |

**Verdict & stats:**

- Original matches → counted as a match.
- Original mismatched, retry matched → counted as a match, with the `Recovered by retry: N` line in the summary tracking the rescue rate.
- Original and retry both mismatched → counted as a mismatch; the `bench_compare` log line is `❌ retry-also-failed` and the per-mismatch JSON file contains both attempts side-by-side.

The retry's latency goes into the same buckets as the original, so when `retry_after_ms` is set the latency table reflects the real (~2x) load.

**Log format with retry:**

```
INFO  bench_compare: ✅ rescued 9xQeWvG8...Fin [base64] [finalized] Cloudbreak (0.10KB 207ms) | Agave (0.12KB 232ms) (slot diff: 0) 🔁 retry(200 ms after start, 198ms+228ms, +12 slots)
INFO  bench_compare: ❌ retry-also-failed 9xQeWvG8...Fin [base64] [finalized] Cloudbreak (0.10KB 207ms) | Agave (0.12KB 232ms) (slot diff: 0) 🔁 retry(200 ms after start, 198ms+228ms, +12 slots)
```

The `🔁 retry(...)` annotation reports:
1. How many ms after the original's send time the retry was actually fired (the `200 ms` here matches `retry_after_ms`; for on-mismatch retries it's the original's end-to-end latency).
2. The retry's rpc1 and rpc2 round-trip times.
3. **The slot delta between the original and the retry on rpc1** (the endpoint under test) — this is the key signal for the "temporal inconsistency over time" study.

**Mismatch save format when retry is enabled:**

```json
{
  "request": { ... },
  "original": {
    "context_matches": true,
    "sizes": { "Cloudbreak": 92, "Agave": 113 },
    "durations_ms": { "Cloudbreak": 207, "Agave": 232 },
    "slots": { "Cloudbreak": 335418200, "Agave": 335418200 },
    "responses": { "Cloudbreak": { ... }, "Agave": { ... } }
  },
  "retry": {
    "fire_after_ms": 200,
    "context_matches": true,
    "sizes": { ... },
    "durations_ms": { ... },
    "slots": { "Cloudbreak": 335418212, "Agave": 335418212 },
    "responses": { "Cloudbreak": { ... }, "Agave": { ... } }
  }
}
```

The retry block is omitted when no retry was performed (legacy mismatch shape).

**Rescued save format (`save_rescued = true`):**

When the retry flipped the verdict from mismatch → match, the file is named
`rescued_<program_id>_<timestamp>.json` (instead of `mismatch_…`) and lives in
the same `comparison.mismatch_output_dir`. The JSON schema is **identical** to
the retry-enabled mismatch shape above — both `original` and `retry` blocks
are present — so you can:

- Eyeball what the `original.responses.Cloudbreak` looked like at the bad slot vs. what the retry recovered.
- Re-feed the directory back through the `mismatch_dir` source to replay only requests that originally needed a rescue.

Because the prefix differs, mismatches and rescues never collide. Toggle this independently from `comparison.save_mismatches` — you can save rescues without saving the actual failures, or vice-versa.

#### `save_compensation_iterations` — full slot-compensation history per file

By default a saved file shows the **final** rpc1+rpc2 pair that slot compensation settled on (under `original.responses` / `retry.responses`). When the picked pair already matches the source-of-truth via slot compensation, you're seeing the *result* of the algorithm, not the input — which can hide whether the actual disagreement is a transient (one of the endpoints catching up) or a stable, same-slot divergence.

Set `comparison.save_compensation_iterations = true` to record **every individual `rpc1+rpc2` send** that happened during the original (and, if applicable, the retry) comparison pass. The schema is additive — each block gains a new sibling field next to `responses` / `slots` / `durations_ms` / `sizes`:

```json
{
  "request": { ... },
  "original": {
    "context_matches": false,
    "responses": { "Cloudbreak": { ... final picked ... }, "Agave": { ... final picked ... } },
    "slots":     { "Cloudbreak": 422549003, "Agave": 422549003 },
    "iterations": [
      {
        "phase": "initial",
        "fired_at": "2026-05-27T17:34:44.044Z",
        "responses":     { "Cloudbreak": { ... }, "Agave": { ... } },
        "slots":         { "Cloudbreak": 422549003, "Agave": 422549004 },
        "durations_ms":  { "Cloudbreak": 207,       "Agave": 232       },
        "sizes":         { "Cloudbreak": 90,        "Agave": 111       }
      },
      {
        "phase": "initial",
        "fired_at": "2026-05-27T17:34:44.064Z",
        "responses":     { "Cloudbreak": { ... }, "Agave": { ... } },
        "slots":         { "Cloudbreak": 422549003, "Agave": 422549003 },
        "durations_ms":  { "Cloudbreak": 199,       "Agave": 215       },
        "sizes":         { "Cloudbreak": 90,        "Agave": 111       }
      }
    ]
  },
  "retry": { "...same shape, also with iterations[]..." }
}
```

Field semantics inside each `iterations[]` entry:

| Field          | Meaning                                                                                                                                                                                                              |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `phase`        | `"initial"` for the first slot-compensation pass; `"with_context_retry"` for the second pass triggered when `inject_context = true` resolves a no-context mismatch by re-issuing the request with `withContext:true` |
| `fired_at`     | ISO-8601 / RFC-3339 timestamp with millisecond precision, captured **once** immediately before the `tokio::join!` (both rpc1 and rpc2 leave the client effectively simultaneously)                                  |
| `responses`    | Raw RPC response per endpoint (or `null` if that endpoint errored on this iteration)                                                                                                                                 |
| `slots`        | `result.context.slot` per endpoint (or `null` if the response had no context)                                                                                                                                        |
| `durations_ms` | Per-endpoint round-trip latency in ms for this iteration                                                                                                                                                             |
| `sizes`        | Per-endpoint response body length in bytes                                                                                                                                                                           |

Notes:

- The first `iterations[]` entry always corresponds to the very first send (the "seed" pair) — `iterations[0]` is what was sent before slot compensation kicked in.
- Subsequent entries are slot-compensation retries (one per round of `slot_compensation_interval_ms`).
- The number of entries is bounded by `slot_compensation_max_retries + 1` per phase (and there's at most one `with_context_retry` phase).
- When `enable_slot_compensation = false` or the initial pair already matched, only `iterations[0]` will be present — still emitted so the file shape stays consistent.
- This array has a real memory cost (it clones every response while the comparison is in flight) — leave it off for high-RPS / large-payload runs (e.g. gPA) and turn it on for targeted investigations.

#### `save_db_probe_iterations` — direct DB cross-check per iteration (getBalance only)

When you're triaging a `getBalance` mismatch, the `iterations[]` array alone tells you what each RPC *returned* — it doesn't tell you whether Cloudbreak's *database* actually has the row at the slot the RPC claimed. To close that gap, set `comparison.save_db_probe_iterations = true`.

For every iteration captured under `iterations[]`, a third arm gets added to the `tokio::join!` that fires rpc1 and rpc2: a SQL probe against `[db_check].db_url` that reads the `slots` table and the top-20 newest `(slot, lamports)` rows for the queried pubkey from both `accounts` and `snapshot_accounts`. Each iteration entry then gains a `db_probe: {...}` field:

```json
{
  "phase": "initial",
  "fired_at": "2026-05-27T17:34:44.044Z",
  "responses": { ... },
  "slots":     { "Cloudbreak": 422549003, "Agave": 422549004 },
  "db_probe": {
    "probed_at": "2026-05-27T17:34:44.045Z",
    "slots_table": {
      "Processed": 422549004,
      "Confirmed": 422549003,
      "Finalized": 422549001
    },
    "accounts": [
      { "table": "accounts",          "slot": 422549002, "lamports": 40318369496 },
      { "table": "accounts",          "slot": 422549001, "lamports": 40318374496 },
      { "table": "accounts",          "slot": 422549000, "lamports": 40318379496 },
      { "table": "snapshot_accounts", "slot": 422530000, "lamports": 40318379496 }
    ]
  }
}
```

Field semantics inside `db_probe`:

| Field         | Meaning                                                                                                                                                                                                                       |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `probed_at`   | ISO-8601 / RFC-3339 ms-precision timestamp captured server-side right before the SQL statements run. Differs from `fired_at` by sub-millisecond in practice (both arms start in the same `tokio::join!`).                     |
| `slots_table` | All rows from the `slots` table at probe time, keyed by commitment name (`Processed` / `Confirmed` / `Finalized`). Confirms what the indexer believes the latest slot is, independent of the RPC's view.                      |
| `accounts`    | Top-20 newest `(slot, lamports)` rows for the queried pubkey from `accounts`, plus top-20 from `snapshot_accounts`, sorted newest-first by `slot`. Lets you see directly whether the row at `responses.Cloudbreak.context.slot` exists in the DB and what its lamports value is. |

Constraints:

- **Request type must be `getBalance`** — the probe knows only the single-pubkey shape. Other request types fail validation at startup if this flag is on.
- **`[db_check]` must be configured** — the probe reuses `db_check.db_url`. Missing it is a startup error.
- **Requires `save_compensation_iterations = true`** to actually appear on disk — `db_probe` lives inside each `iterations[]` entry. Setting it alone is a no-op.
- **Per-iteration cost**: one extra SQL round-trip per iteration (two queries inside, run in parallel). Acceptable at the `target_rps` ranges typical for `getBalance` debugging; not recommended for sustained high RPS.
- **Failures are silent after the first warn** — a single `WARN` fires on the first DB error of a run; subsequent failures record `"db_probe": null` and don't disrupt the comparison itself.

What this rules in or out:

- If `db_probe.accounts[0].slot == responses.Cloudbreak.context.slot` but the lamports differ between Cloudbreak and Agave at the same slot → real data corruption / replay-divergence, **not** slot compensation noise.
- If `db_probe.accounts` has no row at `responses.Cloudbreak.context.slot` (i.e. the RPC returned slot `N` but the DB's newest row for that pubkey is `N-1`) → the indexer is reporting the row at `N-1` as the value for slot `N` (Geyser pre-tx-state hypothesis), independent of how the RPC built the response.
- If `db_probe.slots_table.Finalized > responses.Cloudbreak.context.slot` → the RPC chose a stale slot for its own reasons (likely a stale snapshot inside `getBalance.sql`), not the indexer.

### `[print_config]`

Two responsibilities:

1. **Suppression thresholds** for the per-request `bench_request` line — all three `min_*` values must be met (logical AND) for a request to be logged. Set all to `0` to see every request that passes the `log_individual_requests` toggle.
2. **Per-event log toggles** for `bench_compare::*` and `bench_request` events. Each `log_*` flag defaults to `false` (opt-in). When `false`, a `<target>=off` directive is appended to `EnvFilter` **after** `RUST_LOG` is parsed; when `true` the flag is a no-op and the event fires at its natural `INFO` level.

| Field                       | Default  | Description                                                                                                                                                                                                          |
| --------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `min_request_bytes`         | _(req)_  | Minimum response size in bytes                                                                                                                                                                                       |
| `min_request_duration_ms`   | _(req)_  | Minimum request duration in milliseconds                                                                                                                                                                             |
| `min_request_account_count` | _(req)_  | Minimum number of accounts in the response                                                                                                                                                                           |
| `sample_every_secs`         | _(none)_ | If set (and > 0), emits one full `(request, response1, response2)` tuple via the `bench_sample` tracing target at most once every N seconds across all concurrent tasks. Only fires when `[comparison]` is enabled. Omit or set to `0` to disable. |
| `log_matches`               | `false`  | Allow `bench_compare::match` events — ✅ both endpoints matched on the first try (no retry needed)                                                                                                                  |
| `log_mismatches`            | `false`  | Allow `bench_compare::mismatch` events — ❌ post-compensation mismatch (covers plain mismatches *and* `retry-also-failed`)                                                                                          |
| `log_rescues`               | `false`  | Allow `bench_compare::rescued` events — ✅ `[retry_in_place]` flipped the verdict                                                                                                                                   |
| `log_no_context_mismatches` | `false`  | Allow `bench_compare::no_context_mismatch` events — ⚠️ mismatch where both responses lacked `result.context`                                                                                                       |
| `log_compare_errors`        | `false`  | Allow `bench_compare::error` events — 💥 / 💥💥 one or both endpoints returned an error in the comparison                                                                                                            |
| `log_individual_requests`   | `false`  | Allow `bench_request` events — per-rpc1 line (still gated by the `min_*` thresholds when this is `true`)                                                                                                            |

#### TOML vs `RUST_LOG` precedence

The flags are translated into `EnvFilter` directives at startup, **appended after** the directives parsed from `RUST_LOG`. `EnvFilter` resolves each event by picking the **most specific** matching directive (longest target prefix wins), so:

- `RUST_LOG=info,bench_compare=debug` + TOML `log_matches = false` → `bench_compare::match` events are suppressed (the TOML directive targets a longer prefix), other `bench_compare::*` events still fire at DEBUG.
- TOML `log_mismatches = true` + `RUST_LOG=info,bench_compare=off` → mismatches **are silenced** because the `RUST_LOG`-level `bench_compare=off` is more specific than the natural `INFO` of the call site (and the `true` flag adds no directive of its own). Use this as a "kill switch" override when you really want quiet.

In short: **TOML can hide specific subtargets `RUST_LOG` wants to show; `RUST_LOG` can hide entire parent targets the TOML wants to show.** Both directions work.

The targets that are *not* under `[print_config]` flags (`bench_sample`, `bench_source`, `bench_db_check`, and bare-target operational `error!` lines) are controlled entirely by `RUST_LOG`.

## Logging

Logging uses `tracing` with the `RUST_LOG` environment variable for granular control. Each log category uses a separate tracing target so you can enable/disable them independently.

| Target                              | Level   | Gated by TOML flag           | Content                                                              |
| ----------------------------------- | ------- | ---------------------------- | -------------------------------------------------------------------- |
| `bench_request`                     | `INFO`  | `log_individual_requests`    | Per-rpc1 line, success or error (also gated by `min_*` thresholds)   |
| `bench_compare::match`              | `INFO`  | `log_matches`                | ✅ both endpoints matched on the first try                            |
| `bench_compare::mismatch`           | `INFO`  | `log_mismatches`             | ❌ post-compensation mismatch (covers `retry-also-failed`)            |
| `bench_compare::rescued`            | `INFO`  | `log_rescues`                | ✅ `[retry_in_place]` flipped the verdict                             |
| `bench_compare::no_context_mismatch`| `INFO`  | `log_no_context_mismatches`  | ⚠️ mismatch with no `result.context` (slot lag unverifiable)         |
| `bench_compare::error`              | `INFO`  | `log_compare_errors`         | 💥 one endpoint errored / 💥💥 both endpoints errored                 |
| `bench_source`                      | `INFO`  | _(RUST_LOG only)_            | VictoriaLogs / mismatch-dir refresh events                            |
| `bench_source`                      | `ERROR` | _(RUST_LOG only)_            | Source refresh failures                                              |
| `bench_db_check`                    | `INFO`  | _(RUST_LOG only)_            | Per-account DB slot/lamports for mismatched accounts                  |
| `bench_db_check`                    | `ERROR` | _(RUST_LOG only)_            | DB connection or query failures                                       |
| `bench_sample`                      | `INFO`  | _(RUST_LOG; rate-limited)_   | One full `(request, response1, response2)` tuple every `sample_every_secs` |

**`RUST_LOG` examples:**

By default, **no** `bench_compare::*` or `bench_request` events are printed — toggle them in `[print_config]` first. `RUST_LOG` then controls everything that the TOML hasn't explicitly suppressed (sources, db_check, sample, etc.) plus acts as a global backstop.

```sh
# Pure default (no log_* flags set in TOML, no RUST_LOG) — only source/db_check
# events and the BENCHMARK SUMMARY block are printed
cargo run --bin integration_tests -- benchmark gpa

# Once you've set `log_mismatches = true` (etc.) in the TOML, INFO is enough
# Pure default — visible at default INFO
cargo run --bin integration_tests -- benchmark gpa

# Crank up the source/db_check verbosity without touching TOML
RUST_LOG=debug cargo run --bin integration_tests -- benchmark gpa

# Hide the per-account db_check chatter while keeping bench_compare flags
RUST_LOG=info,bench_db_check=off cargo run --bin integration_tests -- benchmark gpa

# Global kill-switch on bench_compare (overrides any log_* = true in TOML)
RUST_LOG=info,bench_compare=off cargo run --bin integration_tests -- benchmark gpa

# Silence periodic sample dumps when sample_every_secs is set
RUST_LOG=info,bench_sample=off cargo run --bin integration_tests -- benchmark gpa
```

### Run logs: request results (`bench_request`)

Each request to `rpc1` produces a log line (if it passes the `[print_config]` thresholds):

```
DEBUG bench_request: ⚡⚡⚡ cloudbreak [jsonParsed] 45.23KB | 320ms | 128 accounts slot:335418200
```

**Format:** `{speed} {endpoint_name} [{encoding}] {size}KB | {duration}ms | {account_count} accounts {slot}`

| Field         | Description                                         |
| ------------- | --------------------------------------------------- |
| Speed icons   | Latency tier of the request (see table below)       |
| Endpoint name | The `name` from the `[rpc1]` config                 |
| Encoding      | Request encoding (`base64`, `base58`, `jsonParsed`) |
| Size          | Response body size in KB                            |
| Duration      | Round-trip time in milliseconds                     |
| Account count | Number of accounts in the response array            |
| Slot          | The `result.context.slot` value (omitted if absent) |

**Speed icons (absolute latency):**

| Icon     | Duration range |
| -------- | -------------- |
| ⚡⚡⚡⚡ | 0 - 100ms      |
| ⚡⚡⚡   | 101 - 500ms    |
| ⚡⚡     | 501ms - 2s     |
| ⚡       | 2s - 5s        |
| 🐢       | > 5s           |

**Error responses** use the same `DEBUG` level as successes, with a different format:

```
DEBUG bench_request: 💥 cloudbreak TokenkegQ...5DA [jsonParsed] 320ms - {"code":-32600,"message":"Invalid request"}
```

### Run logs: comparison results (`bench_compare`)

When comparison is enabled, each compared request produces a log line:

```
DEBUG bench_compare: ✅⚡⚡ TokenkegQ...5DA [jsonParsed] [finalized] cloudbreak (45.23KB 320ms) | reference (44.98KB 580ms) (slot diff: 0)
```

```
INFO  bench_compare: ❌🐢 TokenkegQ...5DA [base64] [processed] cloudbreak (120.50KB 890ms) | reference (120.50KB 410ms) 🔀 2 slots diff 🔄(3 retries)
```

No-context mismatches (both responses lack context, so slot lag cannot be verified):

```
INFO  bench_compare: ⚠️⚡⚡⚡⚡ KLend2g3...jD [base64] [finalized] cloudbreak (630.06KB 725ms) | reference (630.06KB 15255ms) (no context — possible slot lag)
```

**Format:** `{match}{speed} {param_id} [{encoding}] [{commitment}] {rpc1_info} | {rpc2_info} {slot_info} {retry_info}`

| Field      | Description                                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------------------------- |
| Match icon | ✅ responses match, ❌ responses don't match, ⚠️ no-context mismatch (possible slot lag)                            |
| Speed icon | How much faster/slower `rpc1` is relative to `rpc2` (see table below)                                               |
| Param ID   | First parameter of the request (usually the program ID or token owner address)                                      |
| Encoding   | Request encoding                                                                                                    |
| Commitment | Commitment level from the request (`finalized`, `confirmed`, `processed`). Defaults to `finalized` if not specified |
| rpc1 info  | `{name} ({size}KB {duration}ms)` for rpc1                                                                           |
| rpc2 info  | `{name} ({size}KB {duration}ms)` for rpc2                                                                           |
| Slot info  | 🔀 if slots differ (with the difference, even if it's 0), omitted if no context                                     |
| Retry info | 🔄 with retry count if slot compensation was attempted                                                              |

**Speed icons (relative latency, `rpc1` compared to `rpc2`):**

| Icon     | rpc1/rpc2 duration ratio | Meaning                      |
| -------- | ------------------------ | ---------------------------- |
| ⚡⚡⚡⚡ | <= 0.25                  | rpc1 is 4x+ faster than rpc2 |
| ⚡⚡⚡   | <= 0.50                  | rpc1 is 2-4x faster          |
| ⚡⚡     | <= 0.75                  | rpc1 is moderately faster    |
| ⚡       | <= 0.95                  | rpc1 is slightly faster      |
| _(none)_ | 0.95 - 1.05              | roughly equal                |
| 🐢       | <= 1.33                  | rpc1 is slightly slower      |
| 🐢🐢     | <= 2.00                  | rpc1 is moderately slower    |
| 🐢🐢🐢   | <= 4.00                  | rpc1 is 2-4x slower          |
| 🐢🐢🐢🐢 | > 4.00                   | rpc1 is 4x+ slower than rpc2 |

**Error comparisons** use 💥: one endpoint errored → `WARN`; both errored (aligned invalid requests) → `DEBUG` like a match.

```
WARN  bench_compare: 💥 TokenkegQ...5DA [jsonParsed] [finalized] 💥 cloudbreak (320ms) | reference (44.98KB 580ms)
```

```
DEBUG bench_compare: 💥💥 TokenkegQ...5DA [jsonParsed] [finalized] 💥 cloudbreak (320ms) | 💥 reference (580ms)
```

`💥` = one endpoint errored, `💥💥` = both endpoints errored.

### Source refresh logs (`bench_source`)

When using VictoriaLogs, the background fetcher logs each refresh:

```
INFO  bench_source: Refreshed 847 requests from VictoriaLogs
ERROR bench_source: Failed to refresh requests: connection refused
```

### Periodic sample dumps (`bench_sample`)

When `[print_config].sample_every_secs` is set, the benchmark periodically prints one full request and both response payloads so you can spot-check what's actually being compared. The dump is throttled atomically across all concurrent request tasks, so you get exactly one sample per window regardless of RPS or `max_in_flight`.

```
INFO  bench_sample: 📋 SAMPLE ✅
--- request ---
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getAccountInfo",
  "params": [
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
    { "encoding": "base64", "commitment": "confirmed" }
  ]
}
--- Cloudbreak response (208ms) ---
{ "jsonrpc": "2.0", "result": { "context": { "slot": 335418200 }, "value": { ... } }, "id": 1 }
--- Agave response (65ms) ---
{ "jsonrpc": "2.0", "result": { "context": { "slot": 335418200 }, "value": { ... } }, "id": 1 }
```

The match icon (`✅` / `❌` / `⚠️`) reflects the comparator's verdict for the dumped pair. Only fires when `[comparison]` is enabled (so `rpc2` exists). To silence: omit the field, set it to `0`, or pass `RUST_LOG=...,bench_sample=off`.

## Output Summary

At the end of a run, a summary is printed to stdout. It has three sections:

### 1. Overview

```
==========================================================================================
BENCHMARK SUMMARY
==========================================================================================
Duration: 100.0s | Total requests: 1847 | Effective RPS: 18.5 | Avg: 3.421 Gbit/s (cap 5.000 Gbit/s)
Dropped requests (backpressure): 53 (2.8% of attempted)
Comparisons: 1423 matches, 5 mismatches, 7 no-context mismatches (possible slot lag)
Recovered by retry: 18 (would have been mismatches without retry_in_place) (1.26% of compared)
```

The **Recovered by retry** line is only printed when `[retry_in_place]` is enabled and at least one mismatch was rescued by the retry. It contributes to `matches`, not on top of it.

| Field            | Description                                                                                                                                                                                                                                 |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Duration         | Actual wall-clock time of the run                                                                                                                                                                                                           |
| Total requests   | Total number of requests sent (to both endpoints combined)                                                                                                                                                                                  |
| Effective RPS    | `total_requests / duration` — the actual throughput achieved                                                                                                                                                                                |
| Avg Gbit/s       | Average rpc1 throughput over the run, computed from the actual received response wire bytes (`total_rpc1_bytes * 8 / duration / 1e9`). When `target_gbits` is set, the configured cap is shown in parentheses                                 |
| Dropped requests | Requests that were not sent because `max_in_flight` was full (only shown if > 0). This can indicate that `max_in_flight` is too low for the target RPS, or that the server is responding too slowly and in-flight requests are accumulating |
| Comparisons      | Match/mismatch/no-context mismatch counts. No-context mismatches are responses that differ but lack `result.context`, meaning the difference may be due to slot lag that cannot be verified or compensated for                              |

### 2. Latency table

When two endpoints are configured, the table shows both side by side for the same `(size, encoding)` category, with a **P50 diff** column showing how rpc1 compares to rpc2 (negative = rpc1 is faster). Each endpoint shows its own request count (`N`), since rpc2 only receives a fraction of requests based on the `ratio` setting.

```
                                 -- cloudbreak                                -- reference
Size            Encoding               N      Avg      P50      P90      P99         N      Avg      P50      P90      P99  P50 diff
--------------------------------------------------------------------------------------------------------------------------------------------
0-1KB           base64               178     93.0       57       86      145       139   7990.0     6696    15515    29931     -99%
1-10KB          base64               237    218.2       59      115     5249       188   8300.9     6469    16203    27753     -99%
10-100KB        base64               195    282.9      116      191     1790       159   7900.1     6442    14805    27262     -98%
100KB-1MB       base64                35    222.0      215      319      506        32   4919.3     3011    11799    17461     -93%
200MB-500MB     base64                28  15589.3    13044    32722    36897        19   9946.4     8554    17822    19236     +52%
100KB-1MB       base64+zstd            2    215.0      234      234      234         2    212.5      229      229      229      +2%
0-1KB           jsonParsed             6     55.3       54       66       66         6  13053.8    13037    23936    23936     -99%
10-100KB        jsonParsed            18    167.3      174      208      308        16  10765.4     8757    24242    26090     -98%
============================================================================================================================================
```

If only one endpoint is configured (no `[comparison]`), a simpler single-column table is shown instead.

Rows are grouped by encoding and sorted by size category. If a category only has data for one endpoint (e.g. due to the comparison `ratio`), the other side shows `-` and no diff is computed.

**Size categories:** `0-1KB`, `1-10KB`, `10-100KB`, `100KB-1MB`, `1MB-10MB`, `10MB-50MB`, `50MB-100MB`, `100MB-200MB`, `200MB-500MB`, `500MB+`

### 3. Slot comparison (only with `[comparison]`)

```
Comparisons: 1423 matches, 5 mismatches, 7 no-context mismatches (possible slot lag)
Context slot: 1380/1435 compared requests have context (96.2%)

Slot difference (rpc1 - rpc2, non-zero only):
  (uses the 1st iteration slot value, the slot compensation doesn't affect the result)

  Count: 245 | Avg: -1.2 | Min: -8 | Max: 3
  Distribution:
    rpc1 behind by >5:       4 (1.6%)
    rpc1 behind by 1-5:    198 (80.8%)
    rpc1 ahead by 1-5:      38 (15.5%)
    rpc1 ahead by >5:        5 (2.0%)
```

| Field                   | Description                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------- |
| Context slot percentage | How many compared responses included `result.context.slot` in both endpoints                       |
| Slot difference         | `rpc1 slot - rpc2 slot` for requests where both had context and slots differed                     |
| Avg / Min / Max         | Statistics over the non-zero slot differences                                                      |
| Distribution            | Histogram of how far apart the endpoints are — positive means rpc1 is ahead, negative means behind |

If all compared requests had matching slots, it prints: `Slot difference: all compared requests had matching slots`

## Example Config

```toml
[rpc1]
url = "http://localhost:4000"
name = "cloudbreak"

[rpc2]
url = "http://localhost:4001"
name = "reference"

[benchmark]
target_rps = 5.0
max_in_flight = 100
duration_secs = 100
# target_gbits = 1.0  # Optional bandwidth cap (Gbit/s) enforced on actual
                      # received rpc1 bytes; throttles RPS to stay under it.

[source]
type = "json_file"
path = "crates/integration_tests/gpa_benchmark_requests.json"

# [source]
# type = "victoria_logs"
# url = "http://victoria-logs:9428/select/logsql/query"
# minutes = 1
# limit = 1000
# min_request_size = 100000
# encoding = "jsonParsed"
# inject_context = true

[comparison]
enable_slot_compensation = true
slot_compensation_max_retries = 50
slot_compensation_interval_ms = 100
ratio = 0.8
save_mismatches = true
# save_compensation_iterations = true  # Record every rpc1+rpc2 send (with
                                       # ISO-8601-ms `fired_at` per iteration)
                                       # into each saved mismatch / rescue file.
# save_db_probe_iterations    = true   # getBalance only. Add a `db_probe`
                                       # block to each `iterations[]` entry
                                       # with `slots` table contents + top-20
                                       # (slot, lamports) rows for the pubkey.
                                       # Requires [db_check] + the two flags
                                       # above to be on.
mismatch_output_dir = "crates/integration_tests/compare_responses_results"

[print_config]
min_request_bytes = 0
min_request_duration_ms = 100
min_request_account_count = 0
# sample_every_secs = 30  # Periodically dump 1 (request, response1, response2)
                          # tuple via `bench_sample` tracing target.

# Per-event log toggles (all default false). Each false → `<target>=off` is
# appended to EnvFilter after RUST_LOG. The TOML wins by being more specific.
log_matches               = false  # ✅ both endpoints matched on the first try
log_mismatches            = true   # ❌ post-compensation mismatch (covers retry-also-failed)
log_rescues               = true   # ✅ rescued by [retry_in_place]
log_no_context_mismatches = true   # ⚠️ no-context mismatch (slot lag unverifiable)
log_compare_errors        = true   # 💥 one or both endpoints errored
log_individual_requests   = false  # per-rpc1 line (also gated by min_* thresholds)

# Optional: re-run a request once and only count it as a mismatch if the retry
# also mismatches. Add `retry_after_ms` to fire the retry for EVERY request
# (warning: doubles request volume on both endpoints). `save_rescued` writes
# rescued_<program_id>_<ts>.json into mismatch_output_dir whenever the retry
# flips a mismatch to a match (independent of `save_mismatches`).
# [retry_in_place]
# enabled        = true
# retry_after_ms = 200
# save_rescued   = false
```
