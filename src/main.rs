use api_to_sql::{fetch, sql, unify};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "api_to_sql")]
#[command(about = "Fetch JSON from an API, unify object arrays, and generate SQL schema")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Fetch JSON from an API and write to returnval.json
    Fetch {
        /// API URL that returns JSON
        #[arg(long)]
        url: String,

        /// Bearer token for API authentication
        #[arg(long)]
        bearer_token: Option<String>,

        /// Output file path (default: returnval.json)
        #[arg(long, default_value = "returnval.json")]
        out: PathBuf,
    },

    /// Unify an array of objects from returnval.json into one object and write unified.json
    Unify {
        /// Input JSON file path (default: returnval.json)
        #[arg(long, default_value = "returnval.json")]
        input: PathBuf,

        /// Path to array of objects (dot path like properties.periods or JSON pointer like /properties/periods)
        #[arg(long)]
        path: String,

        /// Output file path (default: unified.json)
        #[arg(long, default_value = "unified.json")]
        out: PathBuf,
    },

    /// Generate CREATE TABLE SQL from unified.json
    Sql {
        /// Input JSON file path (default: unified.json)
        #[arg(long, default_value = "unified.json")]
        input: PathBuf,

        /// Target SQL table name
        #[arg(long, default_value = "api_result")]
        table: String,

        /// Optional output .sql file path. If omitted, prints SQL to stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

/// Main entry point for the api_to_sql CLI application.
/// Parses command line arguments and dispatches to the appropriate subcommand handler.
/// Supports three main operations: fetch API data, unify JSON objects, and generate SQL schemas.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch { url, bearer_token, out } => {
            fetch::fetch_to_file(&url, bearer_token.as_deref(), &out).await?
        }
        Commands::Unify { input, path, out } => {
            unify::unify_to_file(&input, &path, &out)?
        }
        Commands::Sql { input, table, out } => {
            sql::sql_from_file(&input, &table, out.as_ref())?
        }
    }

    Ok(())
}
