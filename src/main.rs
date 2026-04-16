use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{Map, Value};
use std::fs;
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
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch { url, bearer_token, out } => fetch_to_file(&url, bearer_token.as_deref(), &out).await?,
        Commands::Unify { input, path, out } => unify_to_file(&input, &path, &out)?,
        Commands::Sql { input, table, out } => sql_from_file(&input, &table, out.as_ref())?,
    }

    Ok(())
}

/// Fetches JSON data from a REST API endpoint and saves it to a file.
/// Makes an HTTP GET request with a User-Agent header to comply with API requirements.
/// Optionally includes a Bearer token in the Authorization header for authenticated APIs.
/// The response is parsed as JSON and written to the output file in pretty-printed format.
///
/// # Arguments
/// * `url` - The API endpoint URL to fetch data from
/// * `bearer_token` - Optional Bearer token for API authentication
/// * `out` - Path to the output file where JSON will be saved
///
/// # Returns
/// Returns `Ok(())` on success, or an error if the request fails, returns non-200 status,
/// contains invalid JSON, or file writing fails.
async fn fetch_to_file(url: &str, bearer_token: Option<&str>, out: &PathBuf) -> Result<()> {
    let client = reqwest::Client::new();
    let mut request = client
        .get(url)
        .header("User-Agent", "api_to_sql/0.1.0 (test@example.com)");

    if let Some(token) = bearer_token {
        request = request.header("Authorization", format!("Bearer {}", token));
    }

    let json: Value = request
        .send()
        .await
        .context("request failed")?
        .error_for_status()
        .context("request returned non-success status")?
        .json()
        .await
        .context("response was not valid JSON")?;

    fs::write(out, serde_json::to_string_pretty(&json)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote API response JSON to {}", out.display());
    Ok(())
}
/// Reads a JSON file, extracts an array of objects at the specified path,
/// unifies them into a single object containing all unique fields, and saves the result.
///
/// # Arguments
/// * `input` - Path to the input JSON file
/// * `path` - JSON path to the array of objects (supports dot notation like "data.items" or JSON Pointer like "/data/items")
/// * `out` - Path to the output file where the unified JSON object will be saved
///
/// # Returns
/// Returns `Ok(())` on success, or an error if file reading fails, path resolution fails,
/// or the path doesn't point to an array of objects.
fn unify_to_file(input: &PathBuf, path: &str, out: &PathBuf) -> Result<()> {
    let root = read_json(input)?;
    let items = resolve_array_path(&root, path)?;
    let unified = unify_objects(items)?;

    fs::write(out, serde_json::to_string_pretty(&unified)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote unified object JSON to {}", out.display());
    Ok(())
}

/// Reads a unified JSON object from a file, flattens it into SQL column definitions,
/// generates a CREATE TABLE statement, and outputs it to a file or stdout.
///
/// # Arguments
/// * `input` - Path to the input JSON file containing the unified object
/// * `table` - Name of the SQL table to create
/// * `out` - Optional path to output SQL file; if None, prints to stdout
///
/// # Returns
/// Returns `Ok(())` on success, or an error if file reading fails, the JSON is not an object,
/// or file writing fails.
fn sql_from_file(input: &PathBuf, table: &str, out: Option<&PathBuf>) -> Result<()> {
    let unified = read_json(input)?;
    let obj = unified
        .as_object()
        .context("unified input must be a JSON object")?;

    let mut cols = Vec::new();
    flatten_object(obj, "", 0, &mut cols);

    if cols.is_empty() {
        bail!("unified object does not contain any fields");
    }

    let sql = build_create_table_sql(table, &cols);

    if let Some(out_path) = out {
        fs::write(out_path, &sql)
            .with_context(|| format!("failed to write {}", out_path.display()))?;
        println!("Wrote SQL schema to {}", out_path.display());
    } else {
        println!("{}", sql);
    }

    Ok(())
}

/// Reads a JSON file from disk and parses it into a serde_json::Value.
///
/// # Arguments
/// * `path` - Path to the JSON file to read
///
/// # Returns
/// Returns the parsed JSON Value on success, or an error if file reading fails
/// or the content is not valid JSON.
fn read_json(path: &PathBuf) -> Result<Value> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let value = serde_json::from_str::<Value>(&data)
        .with_context(|| format!("failed to parse JSON from {}", path.display()))?;
    Ok(value)
}

/// Resolves a JSON path to extract an array of values from a JSON document.
/// Supports both dot notation (e.g., "data.items") and JSON Pointer notation (e.g., "/data/items").
/// Traverses the JSON structure step by step, handling both object properties and array indices.
///
/// # Arguments
/// * `root` - The root JSON value to traverse
/// * `path` - The path string to resolve (dot notation or JSON Pointer)
///
/// # Returns
/// Returns a reference to the Vec<Value> at the specified path, or an error if the path
/// is invalid, empty, or doesn't resolve to an array.
fn resolve_array_path<'a>(root: &'a Value, path: &str) -> Result<&'a Vec<Value>> {
    if path.trim().is_empty() {
        return root.as_array()
            .with_context(|| "root value is not an array");
    }

    let target = if path.starts_with('/') {
        root.pointer(path)
            .with_context(|| format!("JSON pointer path not found: {path}"))?
    } else {
        let mut cur = root;
        for seg in path.split('.') {
            if seg.is_empty() {
                bail!("dot path contains empty segment: {path}");
            }

            if let Ok(index) = seg.parse::<usize>() {
                cur = cur
                    .as_array()
                    .and_then(|arr| arr.get(index))
                    .with_context(|| format!("array index segment not found: {seg}"))?;
            } else {
                cur = cur
                    .as_object()
                    .and_then(|obj| obj.get(seg))
                    .with_context(|| format!("object field segment not found: {seg}"))?;
            }
        }
        cur
    };

    target
        .as_array()
        .context("path did not resolve to a JSON array")
}

/// Unifies an array of JSON objects into a single object containing all unique fields.
/// For each object in the array, merges its structure recursively with the unified result.
/// Object fields are combined, with nested objects merged recursively.
/// Primitive values and arrays are taken from the first object that contains them.
///
/// # Arguments
/// * `items` - Slice of JSON Values representing objects to unify
///
/// # Returns
/// Returns a unified JSON object containing all fields from all input objects,
/// or an error if any item in the array is not a JSON object.
fn unify_objects(items: &[Value]) -> Result<Value> {
    let mut unified = Map::new();

    for (i, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .with_context(|| format!("array element at index {i} is not an object"))?;
        merge_object_union(&mut unified, obj);
    }

    Ok(Value::Object(unified))
}

/// Recursively merges a source JSON object into a destination object.
/// For each field in the source object:
/// - If the field doesn't exist in destination, it's added
/// - If both fields are objects, they are merged recursively
/// - Otherwise, the destination field takes precedence (no overwriting)
///
/// This ensures all unique fields from all objects are preserved in the unified result.
///
/// # Arguments
/// * `dst` - Mutable reference to the destination object to merge into
/// * `src` - Reference to the source object to merge from
fn merge_object_union(dst: &mut Map<String, Value>, src: &Map<String, Value>) {
    for (key, src_val) in src {
        match dst.get_mut(key) {
            None => {
                dst.insert(key.clone(), src_val.clone());
            }
            Some(dst_val) => {
                if let (Some(dst_obj), Some(src_obj)) =
                    (dst_val.as_object_mut(), src_val.as_object())
                {
                    merge_object_union(dst_obj, src_obj);
                }
            }
        }
    }
}

/// Recursively flattens a JSON object into a list of SQL column definitions.
/// Traverses the object hierarchy, converting nested objects into flattened column names
/// with underscores. Stops recursing at depth 3 to prevent infinite nesting.
/// Arrays and deeply nested objects (>3 levels) are stored as NVARCHAR(MAX).
///
/// # Arguments
/// * `obj` - The JSON object to flatten
/// * `prefix` - Current prefix for nested field names (empty string for root level)
/// * `depth` - Current nesting depth (starts at 0)
/// * `out` - Mutable vector to collect (column_name, sql_type) tuples
fn flatten_object(obj: &Map<String, Value>, prefix: &str, depth: usize, out: &mut Vec<(String, String)>) {
    for (key, value) in obj {
        let col = if prefix.is_empty() {
            sanitize_ident(key)
        } else {
            format!("{}_{}", prefix, sanitize_ident(key))
        };

        if let Some(child) = value.as_object() {
            if depth < 3 {
                flatten_object(child, &col, depth + 1, out);
            } else {
                out.push((col, "NVARCHAR(MAX)".to_string()));
            }
        } else {
            out.push((col, infer_sql_type(value)));
        }
    }
}

/// Sanitizes a string to be a valid SQL identifier.
/// Converts to lowercase, replaces non-alphanumeric characters (except underscores) with underscores,
/// ensures it doesn't start with a digit, and provides a fallback for empty strings.
/// This ensures generated column names are valid SQL identifiers.
///
/// # Arguments
/// * `s` - The string to sanitize
///
/// # Returns
/// A sanitized string safe to use as a SQL identifier
fn sanitize_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            out.push(c.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        "field".to_string()
    } else if out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("_{}", out)
    } else {
        out
    }
}

/// Infers the appropriate MSSQL data type for a JSON value.
/// Maps JSON types to MSSQL types with sensible defaults for the target schema.
/// Handles integers, floats, strings, booleans, nulls, arrays, and objects.
///
/// # Arguments
/// * `v` - The JSON value to analyze
///
/// # Returns
/// A string representing the appropriate MSSQL data type
fn infer_sql_type(v: &Value) -> String {
    match v {
        Value::Null => "VARCHAR(1000)".to_string(),
        Value::Bool(_) => "BIT".to_string(),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "INT".to_string()
            } else {
                "DECIMAL(18,9)".to_string()
            }
        }
        Value::String(_) => "VARCHAR(1000)".to_string(),
        Value::Array(_) | Value::Object(_) => "NVARCHAR(MAX)".to_string(),
    }
}



/// Builds a complete MSSQL CREATE TABLE statement from column definitions.
/// Includes standard logging columns (LogKey primary key and LogDate timestamp)
/// followed by all the data columns. Generates properly formatted SQL with commas and semicolons.
///
/// # Arguments
/// * `table` - Name of the table to create (will be sanitized)
/// * `cols` - Vector of (column_name, sql_type) tuples defining the table columns
///
/// # Returns
/// A complete CREATE TABLE SQL statement as a string
fn build_create_table_sql(table: &str, cols: &[(String, String)]) -> String {
    let table = sanitize_ident(table);
    let mut lines = Vec::with_capacity(cols.len() + 4);
    lines.push(format!("CREATE TABLE {} (", table));
    lines.push("  LogKey INT IDENTITY(1,1) PRIMARY KEY,".to_string());
    lines.push("  LogDate DATETIME DEFAULT GETDATE(),".to_string());

    for (idx, (col, ty)) in cols.iter().enumerate() {
        let comma = if idx + 1 == cols.len() { "" } else { "," };
        lines.push(format!("  {} {}{}", sanitize_ident(col), ty, comma));
    }

    lines.push(");".to_string());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merges_object_union_from_array() {
        let arr = vec![
            json!({"a": 1, "b": {"x": true}}),
            json!({"c": "v", "b": {"y": 2}}),
        ];

        let unified = unify_objects(&arr).unwrap();
        assert_eq!(unified, json!({"a": 1, "b": {"x": true, "y": 2}, "c": "v"}));
    }

    #[test]
    fn generates_sql_from_unified() {
        let obj = json!({
            "number": 1,
            "name": "abc",
            "flags": [1,2,3],
            "nested": {"ok": false}
        });

        let mut cols = Vec::new();
        flatten_object(obj.as_object().unwrap(), "", 0, &mut cols);
        let sql = build_create_table_sql("forecast_periods", &cols);

        assert!(sql.contains("LogKey INT IDENTITY(1,1) PRIMARY KEY"));
        assert!(sql.contains("LogDate DATETIME DEFAULT GETDATE()"));
        assert!(sql.contains("number INT"));
        assert!(sql.contains("name VARCHAR(1000)"));
        assert!(sql.contains("flags NVARCHAR(MAX)"));
        assert!(sql.contains("nested_ok BIT"));
    }
}
