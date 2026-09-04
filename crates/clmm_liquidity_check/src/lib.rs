//! Third-party CLMM invariant check, exposed as a library so the streaming gPA
//! comparison in `integration_tests` can feed it accounts as they arrive
//! instead of buffering an 8.8 GB response first.
//!
//! `check` and `layout` are byte-identical to the original gist. The only
//! integration point needed is `check::Collected::push_account(pubkey, data)`,
//! which already takes exactly what a streaming decoder holds.

pub mod check;
pub mod layout;
