use crate::{read_json, resolve_array_path, unify_objects};
use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

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
pub fn unify_to_file(input: &PathBuf, path: &str, out: &PathBuf) -> Result<()> {
    let root = read_json(input)?;
    let items = resolve_array_path(&root, path)?;
    let unified = unify_objects(items)?;

    fs::write(out, serde_json::to_string_pretty(&unified)?)
        .with_context(|| format!("failed to write {}", out.display()))?;

    println!("Wrote unified object JSON to {}", out.display());
    Ok(())
}