use crate::{build_create_table_sql, build_openjson_insert_sql, flatten_object, flatten_object_with_paths, read_json};
use anyhow::{bail, Context, Result};
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
/// * `out` - Path to the output SQL file.
///
/// # Returns
/// Returns `Ok(())` on success, or an error if file reading fails, JSON parsing fails,
/// or file writing fails.
pub fn sql_from_file(input: &PathBuf, table: &str, max_depth: Option<usize>, out: &PathBuf) -> Result<()> {
    let unified = read_json(input)?;
    let obj = unified
        .as_object()
        .context("input JSON must be an object")?;

    let mut cols = Vec::new();
    flatten_object(obj, "", 0, max_depth, &mut cols);

    let sql = build_create_table_sql(table, &cols);

    fs::write(out, &sql)
        .with_context(|| format!("failed to write SQL to {}", out.display()))?;
    println!("Wrote SQL schema to {}", out.display());

    Ok(())
}

/// Reads a unified JSON object and generates SQL Server OPENJSON INSERT SQL for row parsing.
///
/// # Arguments
/// * `input` - Path to the input JSON file containing the unified object
/// * `table` - Target SQL table name
/// * `max_depth` - Optional maximum depth to flatten nested objects. If None, flatten all levels.
/// * `return_val_var` - SQL variable/expression containing API response JSON (e.g. @returnval)
/// * `data_path_expr` - SQL variable/expression for JSON_QUERY path (e.g. @DataPath)
/// * `out` - Optional output .sql file path. If None, prints SQL to stdout.
pub fn parse_sql_from_file(
    input: &PathBuf,
    table: &str,
    max_depth: Option<usize>,
    return_val_var: &str,
    data_path_expr: &str,
    out: Option<&PathBuf>,
) -> Result<()> {
    let unified = read_json(input)?;
    let obj = unified
        .as_object()
        .context("input JSON must be an object")?;

    let mut cols = Vec::new();
    flatten_object_with_paths(obj, "", "$", 0, max_depth, &mut cols);

    if cols.is_empty() {
        bail!("no columns were derived from unified JSON object");
    }

    let sql = build_openjson_insert_sql(table, &cols, return_val_var, data_path_expr);

    match out {
        Some(path) => {
            fs::write(path, &sql)
                .with_context(|| format!("failed to write parser SQL to {}", path.display()))?;
            println!("Wrote parser SQL to {}", path.display());
        }
        None => {
            println!("{}", sql);
        }
    }

    Ok(())
}