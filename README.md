# api_to_sql

A Rust CLI tool that fetches JSON from an API, unifies arrays of objects, and generates MSSQL SQL scripts for table creation and JSON parsing.

## Features

- Fetch JSON data from REST APIs
- Extract and unify arrays of objects from JSON responses
- Generate MSSQL CREATE TABLE statements with appropriate column types
- Generate OPENJSON INSERT scripts to parse API JSON arrays into the generated table
- Flatten nested objects with optional depth limiting
- Arrays are stored as NVARCHAR(MAX)

## Installation

1. Ensure you have Rust installed: https://rustup.rs/
2. Clone this repository
3. Build with `cargo build --release`

## Usage

The tool has four subcommands that form a pipeline:

### 1. Fetch API Data

```bash
cargo run -- fetch --url <API_URL> [--bearer-token <TOKEN>] [--x-api-key <API_KEY>] [--out <output_file>]
```

- `--url`: The API endpoint URL
- `--bearer-token`: Optional Bearer token for authenticated APIs (do not include "Bearer " prefix - the tool adds it automatically)
- `--x-api-key`: Optional API key value sent as `x-api-key` header
- `--out`: Output file (default: `returnval.json`)

Examples:
```bash
# Public API
cargo run -- fetch --url https://api.weather.gov/gridpoints/OKX/33,37/forecast

# Authenticated API
cargo run -- fetch --url https://api.example.com/data --bearer-token "your_token_here"

# API key authenticated API
cargo run -- fetch --url https://api.example.com/data --x-api-key "your_api_key_here"

# API requiring both Bearer token and API key
cargo run -- fetch --url https://api.example.com/data --bearer-token "your_token_here" --x-api-key "your_api_key_here"
```

**Note**: If you accidentally include "Bearer " at the beginning of your token (e.g., `--bearer-token "Bearer your_token"`), the tool will warn you and suggest removing the prefix. The tool automatically adds the "Bearer " prefix to your token.

### 2. Unify Array Objects

```bash
cargo run -- unify --input <input_file> --path <json_path> [--out <output_file>]
```

- `--input`: Input JSON file (default: `returnval.json`)
- `--path`: JSON path to the array of objects (e.g., `data`, `properties.periods`, `/data/0/items`)
- `--out`: Output file (default: `unified.json`)

Example:
```bash
cargo run -- unify --path properties.periods
```

### 3. Generate SQL Schema

```bash
cargo run -- sql --input <input_file> --table <table_name> [--max-depth <depth>] [--out <output_file>]
```

- `--input`: Input JSON file (default: `unified.json`)
- `--table`: Table name for the CREATE TABLE statement
- `--max-depth`: Maximum depth to flatten nested JSON objects (optional, default: no limit)
- `--out`: Output SQL file (default: `create_table.sql`)

If table is provided as `Schema_name.Table_name`, the provided schema is used.
If no schema is provided, `dbo` is assumed.

Examples:
```bash
# Flatten all nested objects and write to create_table.sql (default behavior)
cargo run -- sql --table weather_periods

# Limit flattening to 2 levels deep
cargo run -- sql --table weather_periods --max-depth 2 --out create_table.sql

# Use a specific schema
cargo run -- sql --table analytics.weather_periods
```

**Note**: Arrays are always stored as `NVARCHAR(MAX)` regardless of the depth setting. When the maximum depth is reached, remaining nested objects are also stored as `NVARCHAR(MAX)`.

### 4. Generate SQL Parser Script (OPENJSON)

```bash
cargo run -- parse-sql --input <input_file> --table <table_name> [--max-depth <depth>] [--return-var <sql_var>] [--data-path <sql_expr>] [--out <output_file>]
```

- `--input`: Input JSON file (default: `unified.json`)
- `--table`: Target table name for the INSERT statement
- `--max-depth`: Maximum depth to flatten nested JSON objects (optional, default: no limit)
- `--return-var`: SQL variable/expression with full payload JSON (default: `@returnval`)
- `--data-path`: SQL variable/expression passed to `JSON_QUERY` for the row array path (default: `@DataPath`)
- `--out`: Output SQL file (default: `parse_rows.sql`)

Generated parser SQL always targets `dbo.<table_name>`.

Example:
```bash
cargo run -- parse-sql --table weather_periods
```

This generates SQL in the same pattern as hand-written OPENJSON parsing scripts:

```sql
INSERT INTO dbo.weather_periods
  ( [name], [details__size] )
SELECT
  parsed_row.*
FROM (
  SELECT
    content = JSON_QUERY(@returnval, @DataPath)
) as json_data
CROSS APPLY OPENJSON(content)
WITH (
  [name] VARCHAR(1000) '$.name',
  [details__size] VARCHAR(1000) '$.details.size'
) as parsed_row
```

## SQL Type Mapping

- Strings: `VARCHAR(1000)`
- Integers: `INT`
- Floats: `DECIMAL(18,9)`
- Booleans: `BIT`
- Null values: `VARCHAR(1000)`
- Arrays: `NVARCHAR(MAX)`
- Objects beyond the configured flatten depth: `NVARCHAR(MAX)`

> By default, if `--max-depth` is omitted, the tool flattens all nested objects as far as possible. If `--max-depth <depth>` is set, any nested object deeper than that level is stored as `NVARCHAR(MAX)`.

## Simple JSON Examples

### Input JSON

```json
{
  "id": 1,
  "name": "Widget",
  "details": {
    "color": "red",
    "size": "large"
  },
  "tags": ["new", "sale"]
}
```

### Unified JSON Result

If this object is already unified, the same structure is used for SQL generation.

### SQL Behavior

- `id` becomes `INT`
- `name` becomes `VARCHAR(1000)`
- `details__color` and `details__size` become `VARCHAR(1000)` if flattened
- `tags` becomes `NVARCHAR(MAX)` because it is an array

Nested object paths use double underscores (`__`) as separators so they are easier to distinguish from source field names that may already contain underscores.

### Column Name Normalization

Generated SQL column names are normalized for SQL safety:

- Letters and numbers are preserved
- Spaces become underscores (`_`)
- Special characters (anything other than letters, numbers, spaces, or underscore) are replaced with underscores (`_`)
- Names are lowercased

Example:

- JSON field: `driver’s_first_session_on_the_organization?`
- SQL column: `driver_s_first_session_on_the_organization_`

### Depth-Limited Example

For the following input:

```json
{
  "user": {
    "name": "Jane",
    "profile": {
      "age": 30,
      "address": {
        "city": "Seattle"
      }
    }
  }
}
```

- With `--max-depth 1`, `user__profile` becomes `NVARCHAR(MAX)`
- With `--max-depth 2`, `user__profile__address__city` can be flattened
- With no `--max-depth`, the object is flattened fully into individual columns

## Additional Columns

All generated tables include two additional columns for logging:

- `LogKey`: INT IDENTITY(1,1) PRIMARY KEY - Auto-incrementing primary key
- `LogDate`: DATETIME DEFAULT GETDATE() - Timestamp when row was inserted

## Example Pipeline

```bash
# Fetch data
cargo run -- fetch --url https://api.weather.gov/gridpoints/OKX/33,37/forecast

# Unify the periods array
cargo run -- unify --path properties.periods

# Generate SQL
cargo run -- sql --table weather_periods --out create_table.sql

# Generate OPENJSON parser SQL
cargo run -- parse-sql --table weather_periods --out parse_rows.sql
```

## Testing

Run the tests with:
```bash
cargo test
```

## License

MIT