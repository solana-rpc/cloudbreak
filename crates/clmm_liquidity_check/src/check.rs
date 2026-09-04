use std::collections::BTreeMap;

use crate::layout::{
    self, PERSONAL_POSITION_SPAN, POOL_INFO_SPAN, TICK_ARRAY_SPAN,
};

#[derive(Default)]
pub struct Collected {
    pub pools: BTreeMap<[u8; 32], layout::PoolInfo>,
    pub positions: Vec<layout::PersonalPosition>,
    pub tick_cache: BTreeMap<[u8; 32], BTreeMap<i32, TickLiquidity>>,
    pub account_count: usize,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TickLiquidity {
    pub net: i128,
    pub gross: u128,
}

impl Collected {
    pub fn push_account(&mut self, pubkey: &str, data: &[u8]) {
        self.account_count += 1;

        match data.len() {
            POOL_INFO_SPAN => {
                let key = match bs58::decode(pubkey).into_vec() {
                    Ok(v) if v.len() == 32 => {
                        let mut k = [0u8; 32];
                        k.copy_from_slice(&v);
                        k
                    }
                    _ => {
                        println!("skip pool, bad pubkey {pubkey}");
                        return;
                    }
                };
                self.pools.insert(key, layout::decode_pool_info(data));
            }
            PERSONAL_POSITION_SPAN => {
                self.positions.push(layout::decode_personal_position(data));
            }
            TICK_ARRAY_SPAN => {
                let tick_array = layout::decode_tick_array(data);
                let entry = self.tick_cache.entry(tick_array.pool_id).or_default();
                for t in &tick_array.ticks {
                    if t.liquidity_gross == 0 && t.liquidity_net == 0 {
                        continue;
                    }
                    entry.insert(
                        t.tick,
                        TickLiquidity {
                            net: t.liquidity_net,
                            gross: t.liquidity_gross,
                        },
                    );
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PoolAgg {
    found: bool,
    liquidity: u128,
    tick_current: i32,
    owner_liquidity: u128,
    owner_count: u64,
    owner_count_current: u64,
}

pub fn check_clmm_liquidity(collected: &Collected) -> Vec<String> {
    let mut has_error: Vec<String> = Vec::new();

    let mut pool_infos: BTreeMap<[u8; 32], PoolAgg> = collected
        .pools
        .iter()
        .map(|(id, p)| {
            (
                *id,
                PoolAgg {
                    found: true,
                    liquidity: p.liquidity,
                    tick_current: p.tick_current,
                    ..Default::default()
                },
            )
        })
        .collect();

    println!("all pools {}", pool_infos.len());

    for p in &collected.positions {
        let agg = pool_infos.entry(p.pool_id).or_default();

        agg.owner_count += 1;
        if agg.tick_current < p.tick_upper && agg.tick_current >= p.tick_lower {
            match agg.owner_liquidity.checked_add(p.liquidity) {
                Some(v) => agg.owner_liquidity = v,
                None => has_error.push(format!(
                    "owner liquidity overflow {}",
                    b58(&p.pool_id)
                )),
            }
            agg.owner_count_current += 1;
        }
    }

    for (pool_id, pool_info) in &pool_infos {
        if !pool_info.found {
            has_error.push(format!("not found pool info {}", b58(pool_id)));
            continue;
        }

        if pool_info.liquidity != pool_info.owner_liquidity {
            has_error.push(format!(
                "{} -> liquidity: {} user liquidity: {} tick current: {} owner count: {} owner current count: {}",
                b58(pool_id),
                pool_info.liquidity,
                pool_info.owner_liquidity,
                pool_info.tick_current,
                pool_info.owner_count,
                pool_info.owner_count_current,
            ));
        }
    }

    let mut position_p_cache: BTreeMap<[u8; 32], BTreeMap<i32, TickLiquidity>> = BTreeMap::new();

    for p in &collected.positions {
        if p.liquidity == 0 {
            continue;
        }

        let liq_i = match i128::try_from(p.liquidity) {
            Ok(v) => v,
            Err(_) => {
                has_error.push(format!(
                    "liquidity too large {} {}",
                    b58(&p.pool_id),
                    p.liquidity
                ));
                continue;
            }
        };
        let pool = position_p_cache.entry(p.pool_id).or_default();
        let mut overflow = false;

        {
            let lower = pool.entry(p.tick_lower).or_default();
            match (
                lower.net.checked_add(liq_i),
                lower.gross.checked_add(p.liquidity),
            ) {
                (Some(net), Some(gross)) => {
                    lower.net = net;
                    lower.gross = gross;
                }
                _ => overflow = true,
            }
        }
        {
            let upper = pool.entry(p.tick_upper).or_default();
            match (
                upper.net.checked_sub(liq_i),
                upper.gross.checked_add(p.liquidity),
            ) {
                (Some(net), Some(gross)) => {
                    upper.net = net;
                    upper.gross = gross;
                }
                _ => overflow = true,
            }
        }

        if overflow {
            has_error.push(format!(
                "position cache overflow {} {} {}",
                b58(&p.pool_id),
                p.tick_lower,
                p.tick_upper
            ));
        }
    }

    for (pool_id, ticks) in &collected.tick_cache {
        let position_p = position_p_cache.get(pool_id);

        for (tick, value) in ticks {
            let pp = position_p.and_then(|m| m.get(tick));

            if value.net != 0 && pp.map(|x| x.net) != Some(value.net) {
                has_error.push(format!(
                    "error 5, {}, {}, {}, {}",
                    b58(pool_id),
                    tick,
                    value.net,
                    opt(pp.map(|x| x.net)),
                ));
            }
            if value.gross != 0 && pp.map(|x| x.gross) != Some(value.gross) {
                has_error.push(format!(
                    "error 6, {}, {}, {}, {}",
                    b58(pool_id),
                    tick,
                    value.gross,
                    opt(pp.map(|x| x.gross)),
                ));
            }
        }
    }

    has_error
}

fn b58(key: &[u8; 32]) -> String {
    bs58::encode(key).into_string()
}

fn opt<T: std::fmt::Display>(v: Option<T>) -> String {
    match v {
        Some(v) => v.to_string(),
        None => "undefined".to_string(),
    }
}
