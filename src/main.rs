use api_to_sql::{fetch, sql, unify};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "api_to_sql")]
#[command(version)]
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

        /// API key value sent as x-api-key header
        #[arg(long)]
        x_api_key: Option<String>,

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

        /// Maximum depth to flatten nested JSON objects (default: no limit)
        #[arg(long)]
        max_depth: Option<usize>,

        /// Output .sql file path (default: create_table.sql)
        #[arg(long, default_value = "create_table.sql")]
        out: PathBuf,
    },

    /// Generate OPENJSON INSERT SQL to parse API JSON rows into the target table
    ParseSql {
        /// Input JSON file path (default: unified.json)
        #[arg(long, default_value = "unified.json")]
        input: PathBuf,

        /// Target SQL table name
        #[arg(long, default_value = "api_result")]
        table: String,

        /// Maximum depth to flatten nested JSON objects (optional, default: no limit)
        #[arg(long)]
        max_depth: Option<usize>,

        /// SQL variable/expression containing full JSON payload (default: @returnval)
        #[arg(long, default_value = "@returnval")]
        return_var: String,

        /// SQL variable/expression used by JSON_QUERY to locate row array (default: @DataPath)
        #[arg(long, default_value = "@DataPath")]
        data_path: String,

        /// Output .sql file path (default: parse_rows.sql)
        #[arg(long, default_value = "parse_rows.sql")]
        out: PathBuf,
    },
}

/// Main entry point for the api_to_sql CLI application.
/// Parses command line arguments and dispatches to the appropriate subcommand handler.
/// Supports four main operations: fetch API data, unify JSON objects, and generate SQL scripts.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch {
            url,
            bearer_token,
            x_api_key,
            out,
        } => {
            fetch::fetch_to_file(&url, bearer_token.as_deref(), x_api_key.as_deref(), &out).await?
        }
        Commands::Unify { input, path, out } => {
            unify::unify_to_file(&input, &path, &out)?
        }
        Commands::Sql { input, table, max_depth, out } => {
            sql::sql_from_file(&input, &table, max_depth, &out)?
        }
        Commands::ParseSql {
            input,
            table,
            max_depth,
            return_var,
            data_path,
            out,
        } => {
            sql::parse_sql_from_file(
                &input,
                &table,
                max_depth,
                &return_var,
                &data_path,
                &out,
            )?
        }
    }

    Ok(())
}
