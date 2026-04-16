# api_to_sql

A Rust CLI tool that fetches JSON from an API, unifies arrays of objects, and generates MSSQL CREATE TABLE statements.

## Features

- Fetch JSON data from REST APIs
- Extract and unify arrays of objects from JSON responses
- Generate MSSQL CREATE TABLE statements with appropriate column types
- Handle nested objects up to depth 3, storing deeper structures as NVARCHAR(MAX)
- Arrays are stored as NVARCHAR(MAX)

## Installation

1. Ensure you have Rust installed: https://rustup.rs/
2. Clone this repository
3. Build with `cargo build --release`

## Usage

The tool has three subcommands that form a pipeline:

### 1. Fetch API Data

```bash
cargo run -- fetch --url <API_URL> [--bearer-token <TOKEN>] [--out <output_file>]
```

- `--url`: The API endpoint URL
- `--bearer-token`: Optional Bearer token for authenticated APIs (do not include "Bearer " prefix - the tool adds it automatically)
- `--out`: Output file (default: `returnval.json`)

Examples:
```bash
# Public API
cargo run -- fetch --url https://api.weather.gov/gridpoints/OKX/33,37/forecast

# Authenticated API
cargo run -- fetch --url https://api.example.com/data --bearer-token "your_token_here"
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
- `--out`: Output SQL file (default: prints to stdout)

Examples:
```bash
# Flatten all nested objects (default behavior)
cargo run -- sql --table weather_periods --out create_table.sql

# Limit flattening to 2 levels deep
cargo run -- sql --table weather_periods --max-depth 2 --out create_table.sql
```

**Note**: Arrays are always stored as `NVARCHAR(MAX)` regardless of the depth setting. When the maximum depth is reached, remaining nested objects are also stored as `NVARCHAR(MAX)`.

## SQL Type Mapping

- Strings: `VARCHAR(1000)`
- Integers: `INT`
- Floats: `DECIMAL(18,9)`
- Booleans: `BIT`
- Null values: `VARCHAR(1000)`
- Arrays: `NVARCHAR(MAX)`
- Objects (depth > 3): `NVARCHAR(MAX)`

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
```

## Testing

Run the tests with:
```bash
cargo test
```

## License

MIT