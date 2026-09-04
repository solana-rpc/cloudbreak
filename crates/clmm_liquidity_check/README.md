# CLMM liquidity check

Third-party code. Copied from <https://gist.github.com/rudy5348/56e8c7f16aca825ba609fd44ecb1a644> so we can run it ourselves and verify its results. `check.rs`, `layout.rs` and `rpc.rs` are verbatim.

## What it does

It is not a two-endpoint comparison. It fetches **one** `getProgramAccounts` response from **one** endpoint and asks whether that snapshot is internally consistent. Every account it uses comes from that single request, so everything it compares is at the same `result.context.slot`.

Accounts are classified by data length alone — 1544 pool state, 281 personal position, 10240 tick array — and everything else is ignored. Each decoder asserts its total span matches the constant, so a layout drift panics instead of producing silent nonsense.

It then checks two Raydium CLMM invariants.

**Pool liquidity.** For each pool, sum the `liquidity` of every personal position whose range brackets the pool's current tick (`tick_lower <= tick_current < tick_upper`). That sum must equal the pool's own `liquidity` field.

**Tick bookkeeping.** Rebuild each tick's `liquidity_net` and `liquidity_gross` from the positions — at `tick_lower` add L to net and gross, at `tick_upper` subtract L from net and add L to gross — then compare against what the on-chain tick arrays store. Reported as `error 5` (net) and `error 6` (gross).

## Why it complements the streaming comparison

`compare-gpa-streaming` proves two endpoints return the same bytes at the same slot. This proves one endpoint's response is *coherent*. They catch different faults: a torn read, where a gPA scan mixes accounts from different slots, produces a response that is internally inconsistent even though each individual account is a real account.

Run it against both endpoints. Because the streaming comparison has shown cloudbreak and Agave byte-identical at the same slot, any invariant failure should reproduce on both. A failure on cloudbreak alone would contradict that result and needs explaining.

## Running

```bash
cargo run --release -p clmm_liquidity_check -- http://ams381.rpcpool.wg:30790   # cloudbreak
cargo run --release -p clmm_liquidity_check -- http://fra218.rpcpool.wg:8899    # Agave
```

Needs roughly 10 GB of free memory: it buffers the entire decoded response before parsing, and the CAMM response is ~8.8 GB.

## Local changes

Only two, both listed here so the copy stays auditable.

- The RPC endpoint moved from a hardcoded `https://rpc` placeholder to the first command-line argument, so the same binary can run against either endpoint.
- `[profile.release]` was dropped from `Cargo.toml`. Cargo ignores profile sections in non-root workspace members; the workspace root profile applies instead.

## Known gaps

Found while reviewing, not fixed, so the copy stays faithful to the original.

1. **The account discriminator is never checked.** Types are told apart by data length only, and the leading 8-byte Anchor discriminator is skipped rather than verified. Any other CLMM account that happens to be 1544, 281 or 10240 bytes would be silently misread.
2. **The tick check is one-directional.** It iterates the on-chain tick arrays and asks whether the positions explain them. It never iterates the position-derived ticks, so liquidity that the positions imply at a tick the chain reports as zero — or in a tick array missing from the response — goes unreported.
3. **The whole body is buffered before parsing.** `read_to_end` into a `Vec<u8>` needs the full ~8.8 GB resident. The parse itself is streaming and allocation-light, so this is avoidable.
4. **`slot` silently defaults to 0** when `result.context` is absent, rather than failing. The request sets `withContext: true`, so this only masks an endpoint that ignores it.
5. **The parse does not call `Deserializer::end()`.** A truncated body still fails with an EOF error, so truncation is caught, but trailing bytes after the JSON document would be ignored.
