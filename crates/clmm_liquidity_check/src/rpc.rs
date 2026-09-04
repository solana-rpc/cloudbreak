use std::fmt;
use std::io::Read;
use std::time::{Duration, Instant};

use base64::Engine;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use clmm_liquidity_check::check::Collected;
use clmm_liquidity_check::layout::TICK_ARRAY_SPAN;

pub fn get_program_accounts(
    url: &str,
    program_id: &str,
    timeout_secs: u64,
    collected: &mut Collected,
) -> Result<u64, String> {
    let req = serde_json::json!({
        "id": "R",
        "jsonrpc": "2.0",
        "method": "getProgramAccounts",
        "params": [
            program_id,
            { "commitment": "confirmed", "encoding": "base64", "withContext": true }
        ]
    });
    println!("{}", req);

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(30))
        .timeout_read(Duration::from_secs(timeout_secs))
        .build();

    let t = Instant::now();
    let resp = agent
        .post(url)
        .set("Content-Type", "application/json")
        .send_string(&req.to_string())
        .map_err(|e| format!("rpc request error: {e}"))?;

    let encoding = resp.header("Content-Encoding").unwrap_or("none").to_string();

    let mut body = Vec::new();
    resp.into_reader()
        .read_to_end(&mut body)
        .map_err(|e| format!("rpc read error: {e}"))?;

    println!(
        "rpc response {} bytes, content-encoding: {}, {:.1}s",
        body.len(),
        encoding,
        t.elapsed().as_secs_f64()
    );

    let t = Instant::now();
    let mut de = serde_json::Deserializer::from_slice(&body);
    let slot = ResponseSeed(collected)
        .deserialize(&mut de)
        .map_err(|e| format!("rpc parse error: {e}"))?;
    println!("json parsed {:.1}s", t.elapsed().as_secs_f64());

    Ok(slot)
}

struct ResponseSeed<'c>(&'c mut Collected);

impl<'de, 'c> DeserializeSeed<'de> for ResponseSeed<'c> {
    type Value = u64;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<u64, D::Error> {
        d.deserialize_map(ResponseVisitor(self.0))
    }
}

struct ResponseVisitor<'c>(&'c mut Collected);

impl<'de, 'c> Visitor<'de> for ResponseVisitor<'c> {
    type Value = u64;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a json-rpc response object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let mut slot: Option<u64> = None;
        let mut rpc_error: Option<serde_json::Value> = None;

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "result" => slot = Some(map.next_value_seed(ResultSeed(&mut *self.0))?),
                "error" => rpc_error = Some(map.next_value()?),
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        if let Some(e) = rpc_error {
            return Err(de::Error::custom(format!("rpc returned error: {e}")));
        }
        slot.ok_or_else(|| de::Error::custom("rpc response missing `result`"))
    }
}

struct ResultSeed<'c>(&'c mut Collected);

impl<'de, 'c> DeserializeSeed<'de> for ResultSeed<'c> {
    type Value = u64;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<u64, D::Error> {
        d.deserialize_map(ResultVisitor(self.0))
    }
}

struct ResultVisitor<'c>(&'c mut Collected);

impl<'de, 'c> Visitor<'de> for ResultVisitor<'c> {
    type Value = u64;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a gpa result object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u64, A::Error> {
        let mut slot: Option<u64> = None;

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "context" => slot = Some(map.next_value::<Context>()?.slot),
                "value" => map.next_value_seed(AccountsSeed(&mut *self.0))?,
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(slot.unwrap_or(0))
    }
}

#[derive(Deserialize)]
struct Context {
    #[serde(default)]
    slot: u64,
}

struct AccountsSeed<'c>(&'c mut Collected);

impl<'de, 'c> DeserializeSeed<'de> for AccountsSeed<'c> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_seq(AccountsVisitor(self.0))
    }
}

struct AccountsVisitor<'c>(&'c mut Collected);

impl<'de, 'c> Visitor<'de> for AccountsVisitor<'c> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an array of keyed accounts")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut scratch = vec![0u8; TICK_ARRAY_SPAN * 2];

        loop {
            let seed = AccountSeed {
                collected: &mut *self.0,
                scratch: &mut scratch,
            };
            if seq.next_element_seed(seed)?.is_none() {
                break;
            }
        }
        Ok(())
    }
}

struct AccountSeed<'c> {
    collected: &'c mut Collected,
    scratch: &'c mut Vec<u8>,
}

impl<'de, 'c> DeserializeSeed<'de> for AccountSeed<'c> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        #[derive(Deserialize)]
        struct Raw<'a> {
            pubkey: &'a str,
            #[serde(borrow)]
            account: RawAccount<'a>,
        }
        #[derive(Deserialize)]
        struct RawAccount<'a> {
            #[serde(borrow)]
            data: (&'a str, &'a str),
        }

        let raw = Raw::deserialize(d)?;
        if raw.account.data.1 != "base64" {
            return Err(de::Error::custom(format!(
                "unexpected account encoding: {}",
                raw.account.data.1
            )));
        }

        let encoded = raw.account.data.0;
        let need = encoded.len() / 4 * 3 + 3;
        if self.scratch.len() < need {
            self.scratch.resize(need, 0);
        }

        let len = base64::engine::general_purpose::STANDARD
            .decode_slice(encoded.as_bytes(), self.scratch)
            .map_err(|e| de::Error::custom(format!("base64 decode {}: {e}", raw.pubkey)))?;

        self.collected.push_account(raw.pubkey, &self.scratch[..len]);
        Ok(())
    }
}
