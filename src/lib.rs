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
    // Read the entire file into a string, attaching a helpful error message if it fails
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    // Parse the string as JSON, attaching a helpful error message if it fails
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
    // An empty path means the caller expects the root JSON value itself to be the array
    if path.trim().is_empty() {
        return root.as_array()
            .with_context(|| "root value is not an array");
    }

    let target = if path.starts_with('/') {
        // RFC 6901 JSON Pointer (e.g. "/data/items") — delegate to serde_json's built-in resolver
        root.pointer(path)
            .with_context(|| format!("JSON pointer path not found: {path}"))?
    } else {
        // Dot-separated path (e.g. "data.items.0") — walk the JSON tree segment by segment
        let mut cur = root;
        for seg in path.split('.') {
            if seg.is_empty() {
                bail!("dot path contains empty segment: {path}");
            }

            if let Ok(index) = seg.parse::<usize>() {
                // Segment is a number, so treat the current node as an array and index into it
                cur = cur
                    .as_array()
                    .and_then(|arr| arr.get(index))
                    .with_context(|| format!("array index segment not found: {seg}"))?;
            } else {
                // Segment is a key name, so treat the current node as an object and look up the field
                cur = cur
                    .as_object()
                    .and_then(|obj| obj.get(seg))
                    .with_context(|| format!("object field segment not found: {seg}"))?;
            }
        }
        cur
    };

    // The final resolved value must be a JSON array; return an error if it isn't
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
    // Start with an empty map that will accumulate every unique field seen across all objects
    let mut unified = Map::new();

    for item in items {
        if let Some(obj) = item.as_object() {
            // Merge each object's fields into the accumulated map (non-destructively)
            merge_object_union(&mut unified, obj);
        } else {
            bail!("array element at index {} is not an object", items.iter().position(|x| x == item).unwrap_or(0));
        }
    }

    // Wrap the accumulated map back into a serde_json Value so callers get a consistent type
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
/// with double-underscore separators. Stops recursing when the specified max_depth is reached.
/// Arrays are always stored as NVARCHAR(MAX) regardless of depth.
/// When max_depth is None, flattening continues until all nested objects are processed.
///
/// # Arguments
/// * `obj` - The JSON object to flatten
/// * `prefix` - Current prefix for nested field names (empty string for root)
/// * `depth` - Current recursion depth (should start at 0)
/// * `max_depth` - Optional maximum depth to flatten. If None, flatten all levels.
/// * `out` - Mutable vector to collect (column_name, sql_type) tuples
pub fn flatten_object(obj: &Map<String, Value>, prefix: &str, depth: usize, max_depth: Option<usize>, out: &mut Vec<(String, String)>) {
    for (key, value) in obj {
        // Build the column name by joining the running prefix and this key with "__"
        let col_name = if prefix.is_empty() {
            sanitize_ident(key)
        } else {
            sanitize_ident(&format!("{}__{}", prefix, key))
        };

        match value {
            Value::Object(nested) => {
                // Always flatten objects if we're below max_depth (or no max_depth set)
                if max_depth.map_or(true, |max| depth < max) {
                    // Recurse one level deeper, carrying the current column name as the new prefix
                    flatten_object(nested, &col_name, depth + 1, max_depth, out);
                } else {
                    // Depth limit reached, store as NVARCHAR(MAX)
                    out.push((col_name, "NVARCHAR(MAX)".to_string()));
                }
            }
            _ => {
                // Scalar or array value — infer its SQL type and record the column directly
                out.push((col_name, infer_sql_type(value)));
            }
        }
    }
}

/// Recursively flattens a JSON object into SQL columns plus JSON path mappings.
/// The generated mapping can be used in OPENJSON WITH clauses for parsing JSON rows.
///
/// # Arguments
/// * `obj` - The JSON object to flatten
/// * `prefix` - Current SQL column prefix (empty string for root)
/// * `json_path_prefix` - Current JSON path prefix (use "$" for root)
/// * `depth` - Current recursion depth (should start at 0)
/// * `max_depth` - Optional maximum depth to flatten. If None, flatten all levels.
/// * `out` - Mutable vector to collect (column_name, sql_type, json_path) tuples
pub fn flatten_object_with_paths(
    obj: &Map<String, Value>,
    prefix: &str,
    json_path_prefix: &str,
    depth: usize,
    max_depth: Option<usize>,
    out: &mut Vec<(String, String, String)>,
) {
    for (key, value) in obj {
        // Build the flattened SQL column name using "__" as the nesting separator
        let col_name = if prefix.is_empty() {
            sanitize_ident(key)
        } else {
            sanitize_ident(&format!("{}__{}", prefix, key))
        };

        // Extend the JSON path so OPENJSON knows where to find this field in the payload
        let json_path = format!("{}.{}", json_path_prefix, json_path_segment(key));

        match value {
            Value::Object(nested) => {
                if max_depth.map_or(true, |max| depth < max) {
                    // Recurse deeper, passing the current column name and JSON path as the new prefixes
                    flatten_object_with_paths(nested, &col_name, &json_path, depth + 1, max_depth, out);
                } else {
                    // Depth limit reached — store the whole nested object as raw JSON text
                    out.push((col_name, "NVARCHAR(MAX)".to_string(), json_path));
                }
            }
            _ => {
                // Scalar or array — record the column name, its inferred SQL type, and its JSON path
                out.push((col_name, infer_sql_type(value), json_path));
            }
        }
    }
}

/// Builds a SQL Server OPENJSON INSERT script that parses array items into a target table.
///
/// # Arguments
/// * `table` - Target table name
/// * `cols` - Slice of (column_name, sql_type, json_path) tuples
/// * `return_val_var` - SQL variable/expression containing the full API payload JSON (e.g. @returnval)
/// * `data_path_expr` - SQL variable/expression that resolves to the array path (e.g. @DataPath)
///
/// # Returns
/// Complete INSERT...SELECT...OPENJSON SQL statement
pub fn build_openjson_insert_sql(
    table: &str,
    cols: &[(String, String, String)],
    return_val_var: &str,
    data_path_expr: &str,
) -> String {
    // Qualify the table name with a schema (defaults to dbo if none is provided)
    let qualified_table = qualify_table_name(table);

    // Build the comma-separated list of bracketed column names for the INSERT clause
    let column_list = cols
        .iter()
        .map(|(name, _, _)| format!("[{}]", name))
        .collect::<Vec<_>>()
        .join(", ");

    // Build the OPENJSON WITH clause lines: each line maps a SQL column to its JSON path
    let with_lines = cols
        .iter()
        .map(|(name, sql_type, json_path)| format!("    [{}] {} '{}'", name, sql_type, json_path))
        .collect::<Vec<_>>()
        .join(",\n");

    // Assemble the full INSERT...SELECT...OPENJSON statement using the parts built above
    format!(
        "INSERT INTO {table}\n    ( {columns} )\nSELECT\n    parsed_row.*\nFROM (\n    SELECT\n        content = JSON_QUERY({return_val_var}, {data_path_expr})\n) as json_data\nCROSS APPLY OPENJSON(content)\nWITH (\n{with_lines}\n) as parsed_row",
        table = qualified_table,
        columns = column_list,
        return_val_var = return_val_var,
        data_path_expr = data_path_expr,
        with_lines = with_lines
    )
}

fn json_path_segment(key: &str) -> String {
    if key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        // Simple alphanumeric key — use it as-is (e.g. "items" → "items")
        key.to_string()
    } else {
        // Key contains special characters — wrap in quotes and escape any existing quotes
        let escaped = key.replace('"', "\\\"");
        format!("\"{}\"", escaped)
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

    // Walk each character and keep only alphanumerics and underscores (lowercased);
    // replace spaces and any other special characters with an underscore
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            result.push(ch.to_ascii_lowercase());
        } else if ch == ' ' {
            result.push('_');
        } else {
            result.push('_');
        }
    }

    // SQL identifiers must not start with a digit; prepend an underscore if needed
    if result.chars().next().map_or(false, |c| c.is_ascii_digit()) {
        result.insert(0, '_');
    }

    // Fallback for empty strings
    if result.is_empty() {
        result = "_".to_string();
    }

    result
}

/// Qualifies a target table name with schema support.
/// If input is `schema.table`, the provided schema is preserved (sanitized).
/// If no schema is provided, `dbo` is assumed.
pub fn qualify_table_name(table: &str) -> String {
    match table.split_once('.') {
        Some((schema, table_name)) => {
            // Caller supplied an explicit schema — sanitize both parts and reassemble
            format!("{}.{}", sanitize_ident(schema), sanitize_ident(table_name))
        }
        // No schema supplied — default to the dbo schema
        None => format!("dbo.{}", sanitize_ident(table)),
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
/// MSSQL data type string
pub fn infer_sql_type(v: &Value) -> String {
    match v {
        // Null values have no type information, so use a generous varchar as a safe default
        Value::Null => "VARCHAR(1000)".to_string(),
        // JSON booleans map directly to the SQL Server BIT type (0 or 1)
        Value::Bool(_) => "BIT".to_string(),
        Value::Number(n) => {
            if n.is_i64() {
                // Whole numbers fit in a standard INT
                "INT".to_string()
            } else {
                // Floating-point numbers use a high-precision DECIMAL to avoid rounding loss
                "DECIMAL(18,9)".to_string()
            }
        }
        // JSON strings → VARCHAR; 1000 chars covers most real-world API string values
        Value::String(_) => "VARCHAR(1000)".to_string(),
        // Arrays and objects are stored as raw JSON text in a MAX column
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
    // Start the statement with the fully-qualified table name
    let mut sql = format!("CREATE TABLE {} (\n", qualify_table_name(table));
    // Every generated table gets an auto-incrementing surrogate primary key
    sql.push_str("  LogKey INT IDENTITY(1,1) PRIMARY KEY,\n");
    // LogDate lets consumers see when each row was inserted without extra tooling
    sql.push_str("  LogDate DATETIME DEFAULT GETDATE(),\n");

    for (i, (col_name, col_type)) in cols.iter().enumerate() {
        sql.push_str(&format!("  {} {}", col_name, col_type));
        // Append a comma after every column except the last one
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
        flatten_object(obj, "", 0, None, &mut cols);

        let sql = build_create_table_sql("test_table", &cols);
        assert!(sql.contains("CREATE TABLE dbo.test_table"));
        assert!(sql.contains("LogKey INT IDENTITY(1,1) PRIMARY KEY"));
        assert!(sql.contains("LogDate DATETIME DEFAULT GETDATE()"));
        assert!(sql.contains("name VARCHAR(1000)"));
        assert!(sql.contains("age INT"));
        assert!(sql.contains("active BIT"));
    }

    #[test]
    fn builds_openjson_insert_sql_from_nested_json() {
        let json = r#"{
            "amount": 12.5,
            "session_id": "abc",
            "coordinates": {
                "Longitude": 10.1,
                "Latitude": 20.2
            },
            "is_Active": true
        }"#;

        let unified: Value = serde_json::from_str(json).unwrap();
        let obj = unified.as_object().unwrap();

        let mut cols = Vec::new();
        flatten_object_with_paths(obj, "", "$", 0, None, &mut cols);

        let sql = build_openjson_insert_sql("Session_Data_Parsed", &cols, "@returnval", "@DataPath");
        assert!(sql.contains("INSERT INTO dbo.session_data_parsed"));
        assert!(sql.contains("[coordinates__longitude] DECIMAL(18,9) '$.coordinates.Longitude'"));
        assert!(sql.contains("[coordinates__latitude] DECIMAL(18,9) '$.coordinates.Latitude'"));
        assert!(sql.contains("[is_active] BIT '$.is_Active'"));
    }

    #[test]
    fn replaces_special_characters_in_identifiers() {
        let input = "driver’s_first_session_on_the_organization?";
        let actual = sanitize_ident(input);
        assert_eq!(actual, "driver_s_first_session_on_the_organization_");
    }

    #[test]
    fn qualifies_table_name_with_default_schema() {
        assert_eq!(qualify_table_name("session_data"), "dbo.session_data");
    }

    #[test]
    fn qualifies_table_name_with_provided_schema() {
        assert_eq!(qualify_table_name("analytics.session_data"), "analytics.session_data");
    }
}