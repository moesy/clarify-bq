use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "clarify-bq",
    about = "Append-only backup of a Clarify CRM workspace into BigQuery (unofficial)",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub quiet: u8,
    #[arg(long, value_enum, default_value = "text", global = true)]
    pub log_format: Format,
}

#[derive(Copy, Clone, PartialEq, ValueEnum)]
pub enum Format {
    Text,
    Json,
}

#[derive(Args, Clone)]
pub struct ConnArgs {
    #[arg(long, env = "CLARIFY_WORKSPACE")]
    pub workspace: String,
    #[arg(long, env = "BQ_PROJECT")]
    pub project: String,
    /// Secret Manager resource: projects/<p>/secrets/<name>[/versions/<v>]
    #[arg(long, env = "CLARIFY_SECRET")]
    pub secret: Option<String>,
    #[arg(long, env = "BQ_DATASET", default_value = "clarify_crm")]
    pub dataset: String,
    #[arg(long, env = "BQ_LOCATION", default_value = "US")]
    pub location: String,
}

#[derive(Args)]
pub struct BackupArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
    /// Comma-separated object slugs to restrict record backup to
    #[arg(long, value_delimiter = ',')]
    pub objects: Vec<String>,
    /// Resources to skip: records, schemas, lists, list_rows, users, workflows,
    /// settings, activities, attachments, or records:<object>
    #[arg(long, value_delimiter = ',')]
    pub skip: Vec<String>,
    /// Resolve config, discover objects, print the plan; write nothing
    #[arg(long)]
    pub dry_run: bool,
    /// Whole-run deadline, e.g. "2h30m"
    #[arg(long, value_parser = humantime::parse_duration)]
    pub timeout: Option<std::time::Duration>,
    #[arg(long)]
    pub spool_dir: Option<std::path::PathBuf>,
    #[arg(long)]
    pub no_lock: bool,
    /// Suspicious-shrink threshold in percent, per resource
    #[arg(long, default_value_t = 5.0)]
    pub shrink_threshold: f64,
    #[arg(long)]
    pub no_shrink_check: bool,
    /// Partition expiration in days; 0 = keep forever
    #[arg(long, default_value_t = 400)]
    pub partition_expiration_days: u32,
    /// Dataset for latest-snapshot flat views (default: <dataset>_latest)
    #[arg(long)]
    pub views_dataset: Option<String>,
    /// Skip refreshing the latest-snapshot views after the backup
    #[arg(long)]
    pub no_views: bool,
    #[arg(long, value_enum, default_value = "text")]
    pub output: Format,
}

#[derive(Subcommand)]
pub enum Command {
    /// Snapshot the workspace into BigQuery
    Backup(Box<BackupArgs>),
    /// List discoverable object types
    Objects {
        #[command(flatten)]
        conn: ConnArgs,
    },
    /// Verify auth and permissions on both sides
    Check {
        #[command(flatten)]
        conn: ConnArgs,
    },
    /// Write the runs row for a run whose data loaded but marker write failed
    MarkComplete {
        run_id: String,
        #[command(flatten)]
        conn: ConnArgs,
    },
    /// Create/refresh the latest-snapshot flat views
    Views {
        #[command(flatten)]
        conn: ConnArgs,
        /// Dataset for the views (default: <dataset>_latest)
        #[arg(long)]
        views_dataset: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitCode {
    Complete = 0,
    Failed = 1,
    Partial = 2,
    ConfigAuth = 3,
    ShrinkCheck = 4,
    LockHeld = 5,
}

impl ExitCode {
    pub fn code(self) -> i32 {
        self as i32
    }
}
