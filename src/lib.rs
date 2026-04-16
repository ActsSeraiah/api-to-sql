pub mod fetch;
pub mod unify;
pub mod sql;

use anyhow::{bail, Context, Result};
use serde_json::{Map, Value};
use std::fs;
use std::path::PathBuf;

/// Reads a JSON file from disk and parses it into a serde_json::Value.
/// This is a utility function used by other file-based operations.
///
/// # Arguments
/// * `path` - Path to the JSON file to read
///
/// # Returns
/// Returns the parsed JSON Value, or an error if the file cannot be read or parsed.
pub fn read_json(path: &PathBuf) -> Result<Value> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&content)
        .with_context(|| format!("failed to parse JSON from {}", path.display()))
}

/// Resolves a JSON path to extract an array of objects from a JSON document.
/// Supports both dot notation (e.g., "data.items") and JSON Pointer notation (e.g., "/data/items").
/// For root-level arrays, an empty path can be provided.
///
/// # Arguments
/// * `root` - The root JSON value to search within
/// * `path` - The path string to resolve
///
/// # Returns
/// Returns a reference to the Vec<Value> at the specified path, or an error if the path
/// is invalid, empty (when root is not an array), or doesn't resolve to an array.
pub fn resolve_array_path<'a>(root: &'a Value, path: &str) -> Result<&'a Vec<Value>> {
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
///
/// # Arguments
/// * `items` - Slice of JSON Values representing the objects to unify
///
/// # Returns
/// Returns a unified JSON object (Value::Object) containing all unique fields from all input objects.
pub fn unify_objects(items: &[Value]) -> Result<Value> {
    let mut unified = Map::new();

    for item in items {
        if let Some(obj) = item.as_object() {
            merge_object_union(&mut unified, obj);
        } else {
            bail!("array element at index {} is not an object", items.iter().position(|x| x == item).unwrap_or(0));
        }
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
/// * `dst` - Mutable reference to the destination object map
/// * `src` - Reference to the source object map to merge in
pub fn merge_object_union(dst: &mut Map<String, Value>, src: &Map<String, Value>) {
    for (key, value) in src {
        if let Some(dst_value) = dst.get_mut(key) {
            // Both are objects, merge recursively
            if let (Some(dst_obj), Some(src_obj)) = (dst_value.as_object_mut(), value.as_object()) {
                merge_object_union(dst_obj, src_obj);
            }
            // Otherwise keep the destination value (no overwriting)
        } else {
            // Field doesn't exist, add it
            dst.insert(key.clone(), value.clone());
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
/// * `prefix` - Current prefix for nested field names (empty string for root)
/// * `depth` - Current recursion depth (should start at 0)
/// * `out` - Mutable vector to collect (column_name, sql_type) tuples
pub fn flatten_object(obj: &Map<String, Value>, prefix: &str, depth: usize, out: &mut Vec<(String, String)>) {
    const MAX_DEPTH: usize = 3;

    for (key, value) in obj {
        let col_name = if prefix.is_empty() {
            sanitize_ident(key)
        } else {
            sanitize_ident(&format!("{}_{}", prefix, key))
        };

        match value {
            Value::Object(nested) if depth < MAX_DEPTH => {
                flatten_object(nested, &col_name, depth + 1, out);
            }
            _ => {
                out.push((col_name, infer_sql_type(value)));
            }
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
/// A valid SQL identifier string
pub fn sanitize_ident(s: &str) -> String {
    let mut result = String::new();

    for ch in s.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push('_');
        }
    }

    // Ensure it doesn't start with a digit
    if result.chars().next().map_or(false, |c| c.is_ascii_digit()) {
        result.insert(0, '_');
    }

    // Fallback for empty strings
    if result.is_empty() {
        result = "_".to_string();
    }

    result
}

/// Infers the appropriate MSSQL data type for a JSON value.
/// Maps JSON types to MSSQL types with sensible defaults for the target schema.
/// Handles integers, floats, strings, booleans, nulls, arrays, and objects.
///
/// # Arguments
/// * `v` - The JSON value to analyze
///
/// # Returns
/// MSSQL data type string
pub fn infer_sql_type(v: &Value) -> String {
    match v {
        Value::Null => "VARCHAR(1000)".to_string(),
        Value::Bool(_) => "BIT".to_string(),
        Value::Number(n) => {
            if n.is_i64() {
                "INT".to_string()
            } else {
                "DECIMAL(18,9)".to_string()
            }
        }
        Value::String(_) => "VARCHAR(1000)".to_string(),
        Value::Array(_) => "NVARCHAR(MAX)".to_string(),
        Value::Object(_) => "NVARCHAR(MAX)".to_string(),
    }
}

/// Builds a complete CREATE TABLE SQL statement for MSSQL.
/// Includes LogKey (auto-incrementing primary key) and LogDate (timestamp) columns.
/// Generates column definitions from the provided column list.
///
/// # Arguments
/// * `table` - The table name
/// * `cols` - Slice of (column_name, sql_type) tuples
///
/// # Returns
/// Complete CREATE TABLE SQL statement
pub fn build_create_table_sql(table: &str, cols: &[(String, String)]) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", sanitize_ident(table));
    sql.push_str("  LogKey INT IDENTITY(1,1) PRIMARY KEY,\n");
    sql.push_str("  LogDate DATETIME DEFAULT GETDATE(),\n");

    for (i, (col_name, col_type)) in cols.iter().enumerate() {
        sql.push_str(&format!("  {} {}", col_name, col_type));
        if i < cols.len() - 1 {
            sql.push_str(",");
        }
        sql.push_str("\n");
    }

    sql.push_str(");");
    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_object_union_from_array() {
        let json = r#"[
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "department": "Engineering", "city": "SF"}
        ]"#;
        let items: Vec<Value> = serde_json::from_str(json).unwrap();
        let unified = unify_objects(&items).unwrap();

        let obj = unified.as_object().unwrap();
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("age"));
        assert!(obj.contains_key("department"));
        assert!(obj.contains_key("city"));
    }

    #[test]
    fn generates_sql_from_unified() {
        let json = r#"{
            "name": "John",
            "age": 25,
            "active": true
        }"#;
        let unified: Value = serde_json::from_str(json).unwrap();
        let obj = unified.as_object().unwrap();

        let mut cols = Vec::new();
        flatten_object(obj, "", 0, &mut cols);

        let sql = build_create_table_sql("test_table", &cols);
        assert!(sql.contains("LogKey INT IDENTITY(1,1) PRIMARY KEY"));
        assert!(sql.contains("LogDate DATETIME DEFAULT GETDATE()"));
        assert!(sql.contains("name VARCHAR(1000)"));
        assert!(sql.contains("age INT"));
        assert!(sql.contains("active BIT"));
    }
}