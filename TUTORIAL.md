# api_to_sql Full Tutorial (Public .gov API)

This tutorial walks through a complete, practical workflow using a public `.gov` API, from raw API response to SQL scripts you can run in SQL Server.

It is intentionally short and end-to-end. Optional flags are included as notes so you can extend the commands when needed.

## Goal

Use a public API response to generate:

1. A SQL table definition (`create_table.sql`)
2. A SQL parser script (`parse_rows.sql`) that uses `OPENJSON` to insert rows

## Example API Used

This tutorial uses the National Weather Service API:

- API URL: `https://api.weather.gov/gridpoints/OKX/33,37/forecast`
- Data path for array rows: `properties.periods`

## Prerequisites

- Rust installed: https://rustup.rs/
- This repository cloned locally
- Terminal open in the project root

## Step 1: Build the project

```bash
cargo build
```

## Step 2: Fetch JSON from the API

```bash
cargo run -- fetch --url https://api.weather.gov/gridpoints/OKX/33,37/forecast --out returnval.json
```

What this does:

- Calls the API
- Saves the full JSON response to `returnval.json`

Optional fetch flags (examples only):

```bash
# If API uses Bearer token
cargo run -- fetch --url <API_URL> --bearer-token "your_token" --out returnval.json

# If API uses x-api-key
cargo run -- fetch --url <API_URL> --x-api-key "your_api_key" --out returnval.json

# If API uses both
cargo run -- fetch --url <API_URL> --bearer-token "your_token" --x-api-key "your_api_key" --out returnval.json
```

## Step 3: Unify the array objects into one schema object

```bash
cargo run -- unify --input returnval.json --path properties.periods --out unified.json
```

What this does:

- Navigates to `properties.periods`
- Merges all array objects into a single representative object containing all discovered fields
- Saves result to `unified.json`

Notes on `--path`:

- Dot notation: `properties.periods`
- JSON pointer style also works: `/properties/periods`
- If your API returns a root-level array, use an empty path:

```bash
cargo run -- unify --input returnval.json --path "" --out unified.json
```

## Step 4: Generate table DDL SQL

```bash
cargo run -- sql --input unified.json --table weather_periods
```

What this does:

- Generates a `CREATE TABLE` statement from `unified.json`
- Writes to `create_table.sql` by default

Important table behavior:

- If table is `weather_periods`, output uses `dbo.weather_periods`
- If table is `analytics.weather_periods`, output uses `analytics.weather_periods`

Optional schema-depth example:

```bash
cargo run -- sql --input unified.json --table weather_periods --max-depth 2
```

## Step 5: Generate OPENJSON parser SQL

```bash
cargo run -- parse-sql --input unified.json --table weather_periods
```

What this does:

- Generates an `INSERT ... OPENJSON ... WITH (...)` parser script
- Writes to `parse_rows.sql` by default

Optional parser variable examples:

```bash
cargo run -- parse-sql --input unified.json --table weather_periods --return-var "@returnval" --data-path "@DataPath"
```

## Step 6: Review generated files

You should now have:

- `returnval.json` (raw API response)
- `unified.json` (merged schema object)
- `create_table.sql` (DDL)
- `parse_rows.sql` (row parser SQL)

## Step 7: Run SQL in SQL Server

Typical order:

1. Run `create_table.sql` to create the table
2. In your SQL session, define the JSON payload and path variables
3. Run `parse_rows.sql` to insert parsed rows

Example variable setup (adapt as needed):

```sql
DECLARE @returnval NVARCHAR(MAX) = N'{ ... full API JSON response ... }';
DECLARE @DataPath NVARCHAR(4000) = '$.properties.periods';
```

Then execute the generated `parse_rows.sql`.

## How to adapt this tutorial to another API

1. Replace the fetch URL with your API URL
2. Find the array path you want to parse
3. Use that path in `unify`
4. Keep the same `sql` and `parse-sql` flow

If the API shape changes over time, re-run steps 2 through 5 to regenerate scripts.

## Quick command summary

```bash
cargo build
cargo run -- fetch --url https://api.weather.gov/gridpoints/OKX/33,37/forecast --out returnval.json
cargo run -- unify --input returnval.json --path properties.periods --out unified.json
cargo run -- sql --input unified.json --table weather_periods
cargo run -- parse-sql --input unified.json --table weather_periods
```
