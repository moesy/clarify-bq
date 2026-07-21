use clap::Parser;
use clarify_bq::cli::{Cli, Command, ConnArgs, ExitCode, Format};
use clarify_bq::config::Config;
use clarify_bq::{commands, orchestrate, spool};
use std::sync::Arc;

const CLARIFY_BASE: &str = "https://api.clarify.ai/v1";
const BIGQUERY_BASE: &str = "https://bigquery.googleapis.com";
const SECRETMANAGER_BASE: &str = "https://secretmanager.googleapis.com";

fn init_tracing(cli: &Cli) {
    let level = match (cli.verbose, cli.quiet) {
        (_, q) if q > 0 => "warn",
        (0, _) => "info",
        (1, _) => "debug",
        _ => "trace",
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    match cli.log_format {
        Format::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init(),
        Format::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
    }
}

fn exit_config(msg: &str) -> ! {
    eprintln!("config error: {msg}");
    std::process::exit(ExitCode::ConfigAuth.code());
}

struct Gcp {
    provider: Arc<dyn bq_sink::TokenProvider>,
    sink: bq_sink::BqSink,
}

async fn gcp(cfg: &Config) -> Gcp {
    let provider: Arc<dyn bq_sink::TokenProvider> = match bq_sink::GcpAuthProvider::new().await {
        Ok(p) => Arc::new(p),
        Err(e) => exit_config(&format!(
            "{e}. Application Default Credentials are required \
             (gcloud auth application-default login, or GOOGLE_APPLICATION_CREDENTIALS)"
        )),
    };
    let sink = bq_sink::BqSink::new(
        provider.clone(),
        BIGQUERY_BASE.into(),
        cfg.project.clone(),
        cfg.dataset.clone(),
        cfg.location.clone(),
    );
    Gcp { provider, sink }
}

async fn clarify_client(
    cfg: &Config,
    provider: &Arc<dyn bq_sink::TokenProvider>,
) -> clarify_client::ClarifyClient {
    let api_key = match (&cfg.api_key_override, &cfg.secret) {
        (Some(key), _) => key.clone(),
        (None, Some(secret)) => {
            match bq_sink::fetch_secret(SECRETMANAGER_BASE, provider.as_ref(), secret).await {
                Ok(k) => k,
                Err(e) => exit_config(&format!("reading {}: {e}", secret.resource_name())),
            }
        }
        (None, None) => unreachable!("Config::resolve guarantees one source"),
    };
    match clarify_client::ClarifyClient::new(CLARIFY_BASE.into(), api_key, cfg.workspace.clone()) {
        Ok(c) => c,
        Err(e) => exit_config(&e.to_string()),
    }
}

fn resolve(conn: &ConnArgs) -> Config {
    match Config::resolve(conn, std::env::var("CLARIFY_API_KEY").ok()) {
        Ok(c) => c,
        Err(e) => exit_config(&e),
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(&cli);

    let exit = match &cli.command {
        Command::Backup(args) => {
            let cfg = resolve(&args.conn);
            let g = gcp(&cfg).await;
            let client = clarify_client(&cfg, &g.provider).await;
            let spool_root = args
                .spool_dir
                .clone()
                .unwrap_or_else(spool::default_spool_root);
            let run = orchestrate::run_backup(&client, &g.sink, args, &spool_root);
            let result = match args.timeout {
                Some(dur) => match tokio::time::timeout(dur, run).await {
                    Ok(r) => r,
                    Err(_) => {
                        eprintln!(
                            "run deadline of {} exceeded",
                            humantime::format_duration(dur)
                        );
                        std::process::exit(ExitCode::Failed.code());
                    }
                },
                None => run.await,
            };
            match args.output {
                Format::Json => println!("{}", result.summary),
                Format::Text => {
                    println!(
                        "run {} finished: {}",
                        result.summary["run_id"].as_str().unwrap_or("?"),
                        result.summary["status"]
                            .as_str()
                            .unwrap_or("see errors above")
                    );
                    if let Some(resources) = result.summary["resources"].as_array() {
                        for r in resources {
                            println!(
                                "  {:<28} {:<8} {:>8} rows  {}",
                                r["resource"].as_str().unwrap_or("?"),
                                r["status"].as_str().unwrap_or("?"),
                                r["count"].as_u64().unwrap_or(0),
                                r["error"].as_str().unwrap_or("")
                            );
                        }
                    }
                }
            }
            result.exit
        }
        Command::Objects { conn } => {
            let cfg = resolve(conn);
            let client = if cfg.api_key_override.is_some() {
                // No GCP needed when the key comes from the environment.
                match clarify_client::ClarifyClient::new(
                    CLARIFY_BASE.into(),
                    cfg.api_key_override.clone().expect("checked above"),
                    cfg.workspace.clone(),
                ) {
                    Ok(c) => c,
                    Err(e) => exit_config(&e.to_string()),
                }
            } else {
                let g = gcp(&cfg).await;
                clarify_client(&cfg, &g.provider).await
            };
            let (exit, out) = commands::run_objects(&client).await;
            print!("{out}");
            exit
        }
        Command::Check { conn } => {
            let cfg = resolve(conn);
            let g = gcp(&cfg).await;
            let (exit, report) = commands::run_check(
                &cfg,
                g.provider.as_ref(),
                SECRETMANAGER_BASE,
                CLARIFY_BASE,
                &g.sink,
            )
            .await;
            print!("{report}");
            exit
        }
        Command::MarkComplete { run_id, conn } => {
            let cfg = resolve(conn);
            let g = gcp(&cfg).await;
            let (exit, msg) =
                commands::run_mark_complete(&g.sink, run_id, &spool::default_spool_root()).await;
            println!("{msg}");
            exit
        }
    };
    std::process::exit(exit.code());
}
