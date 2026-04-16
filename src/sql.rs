use crate::{build_create_table_sql, flatten_object, read_json};
use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

/// Reads a unified JSON object from a file, flattens it into SQL column definitions,
/// generates a CREATE TABLE statement, and outputs it to a file or stdout.
/// The flattening depth can be controlled to limit how deeply nested objects are flattened.
///
/// # Arguments
/// * `input` - Path to the input JSON file containing the unified object
/// * `table` - Target SQL table name for the CREATE TABLE statement
/// * `max_depth` - Optional maximum depth to flatten nested objects. If None, flatten all levels.
/// * `out` - Optional path to the output SQL file. If None, prints SQL to stdout.
///
/// # Returns
/// Returns `Ok(())` on success, or an error if file reading fails, JSON parsing fails,
/// or file writing fails (when output file is specified).
pub fn sql_from_file(input: &PathBuf, table: &str, max_depth: Option<usize>, out: Option<&PathBuf>) -> Result<()> {
    let unified = read_json(input)?;
    let obj = unified
        .as_object()
        .context("input JSON must be an object")?;

    let mut cols = Vec::new();
    flatten_object(obj, "", 0, max_depth, &mut cols);

    let sql = build_create_table_sql(table, &cols);

    match out {
        Some(path) => {
            fs::write(path, &sql)
                .with_context(|| format!("failed to write SQL to {}", path.display()))?;
            println!("Wrote SQL schema to {}", path.display());
        }
        None => {
            println!("{}", sql);
        }
    }

    Ok(())
}