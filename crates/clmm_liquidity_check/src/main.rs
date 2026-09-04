//! Verbatim copy of https://gist.github.com/rudy5348/56e8c7f16aca825ba609fd44ecb1a644
//! See README.md in this folder. Only the RPC endpoint was changed, from a
//! hardcoded placeholder to a command-line argument, so the same check can run
//! against both cloudbreak and Agave.

mod rpc;

use clmm_liquidity_check::check;

const CLMM_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

const RPC_TIMEOUT_SECS: u64 = 120;

fn main() {
    let Some(rpc) = std::env::args().nth(1) else {
        eprintln!("usage: clmm_liquidity_check <rpc-url>");
        std::process::exit(2);
    };

    println!("start");

    let mut collected = check::Collected::default();
    let slot =
        rpc::get_program_accounts(&rpc, CLMM_PROGRAM_ID, RPC_TIMEOUT_SECS, &mut collected).unwrap();

    println!("all account {} {}", slot, collected.account_count);

    let has_error = check::check_clmm_liquidity(&collected);

    if !has_error.is_empty() {
        println!("check clmm liquidity error, count {}", has_error.len());
        for line in &has_error {
            println!("{line}");
        }
        return;
    }

    println!("over");
}
