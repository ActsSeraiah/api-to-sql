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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch { url, out } => fetch_to_file(&url, &out).await?,
        Commands::Unify { input, path, out } => unify_to_file(&input, &path, &out)?,
        Commands::Sql { input, table, out } => sql_from_file(&input, &table, out.as_ref())?,
    }

    Ok(())
}

async fn fetch_to_file(url: &str, out: &PathBuf) -> Result<()> {
    let client = reqwest::Client::new();
    let json: Value = client
        .get(url)
        .header("User-Agent", "api_to_sql/0.1.0 (test@example.com)")
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

fn unify_to_file(input: &PathBuf, path: &str, out: &PathBuf) -> Result<()> {
    let root = read_json(input)?;
    let items = resolve_array_path(&root, path)?;
    let unified = unify_objects(items)?;

    fs::write(out, serde_json::to_string_pretty(&unified)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote unified object JSON to {}", out.display());
    Ok(())
}

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

fn read_json(path: &PathBuf) -> Result<Value> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let value = serde_json::from_str::<Value>(&data)
        .with_context(|| format!("failed to parse JSON from {}", path.display()))?;
    Ok(value)
}

fn resolve_array_path<'a>(root: &'a Value, path: &str) -> Result<&'a Vec<Value>> {
    if path.trim().is_empty() {
        bail!("path cannot be empty");
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
