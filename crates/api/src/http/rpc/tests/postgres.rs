//! A Postgres database with the migrations' schema for each test, copied from a template in one
//! container that the test binary shares.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use migration::{Migrator, MigratorTrait};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use cloudbreak_index::db_queries;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::UnixTimestamp;

use super::{READ_SLOT, index_config};

/// The template database every test copies: the migrations' schema and `READ_SLOT` as the
/// finalized slot.
const TEMPLATE_DATABASE: &str = "cloudbreak_template";

/// One Postgres 16 container for the test binary, holding `TEMPLATE_DATABASE`. Starting a
/// container takes about 800 ms and copying the template tens of milliseconds, so the tests share
/// the container and each copies the template into a database of its own.
struct SharedPostgres {
    container_id: String,
    /// `postgres://user:password@host:port`, without a database.
    server_url: String,
}

static SHARED_POSTGRES: OnceLock<SharedPostgres> = OnceLock::new();
static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

/// A database of its own for one test, copied from the shared template.
pub(super) async fn start_database() -> DatabaseConnection {
    let shared = shared_postgres();
    let name = format!(
        "test_{}",
        NEXT_DATABASE.fetch_add(1, AtomicOrdering::Relaxed)
    );
    let admin = Database::connect(format!("{}/postgres", shared.server_url))
        .await
        .expect("connect to Postgres");
    admin
        .execute_unprepared(&format!(
            "CREATE DATABASE {name} TEMPLATE {TEMPLATE_DATABASE}"
        ))
        .await
        .expect("copy the template database");
    admin.close().await.expect("close the admin connection");
    Database::connect(format!("{}/{name}", shared.server_url))
        .await
        .expect("connect to the test database")
}

/// Starts the shared container on first use. Each `#[tokio::test]` drops its runtime when it
/// ends, so the container is started on a thread with a runtime of its own.
fn shared_postgres() -> &'static SharedPostgres {
    SHARED_POSTGRES.get_or_init(|| {
        std::thread::spawn(|| {
            tokio::runtime::Runtime::new()
                .expect("build a runtime")
                .block_on(start_shared_postgres())
        })
        .join()
        .expect("start the shared Postgres")
    })
}

async fn start_shared_postgres() -> SharedPostgres {
    let container = Postgres::default()
        .with_tag("16")
        .with_cmd(["-c", "fsync=off", "-c", "max_connections=1000"])
        .start()
        .await
        .expect("start Postgres");
    let server_url = format!(
        "postgres://postgres:postgres@{}:{}",
        container.get_host().await.expect("Postgres host"),
        container
            .get_host_port_ipv4(5432)
            .await
            .expect("Postgres port")
    );
    let admin = Database::connect(format!("{server_url}/postgres"))
        .await
        .expect("connect to Postgres");
    admin
        .execute_unprepared(&format!("CREATE DATABASE {TEMPLATE_DATABASE}"))
        .await
        .expect("create the template database");
    admin.close().await.expect("close the admin connection");

    let template = Database::connect(format!("{server_url}/{TEMPLATE_DATABASE}"))
        .await
        .expect("connect to the template database");
    Migrator::up(&template, None)
        .await
        .expect("run the migrations");
    db_queries::insert_slot(
        READ_SLOT,
        Some(UnixTimestamp { timestamp: 0 }),
        None,
        CommitmentLevel::Finalized,
        true,
        &template,
        index_config(),
    )
    .await;
    // A template with an open connection cannot be copied.
    template
        .close()
        .await
        .expect("close the template connection");

    let container_id = container.id().to_string();
    // Dropping the handle would remove the container when this thread's runtime ends; it is
    // removed when the test binary exits instead, or on Ctrl-C by the testcontainers watchdog.
    std::mem::forget(container);
    // SAFETY: registers a plain `extern "C"` function with no arguments, as `atexit` expects.
    unsafe { libc::atexit(remove_shared_postgres) };
    SharedPostgres {
        container_id,
        server_url,
    }
}

/// Removes the shared container when the test binary exits.
extern "C" fn remove_shared_postgres() {
    let Some(shared) = SHARED_POSTGRES.get() else {
        return;
    };
    let removed = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", &shared.container_id])
        .output();
    if !removed.is_ok_and(|output| output.status.success()) {
        eprintln!(
            "could not remove the test Postgres container {}; remove it with docker rm --force",
            shared.container_id
        );
    }
}
