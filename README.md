# api_to_sql

`api_to_sql` is a Rust command-line tool that helps you go from API JSON to SQL schema in three steps:

1. Fetch JSON from an API and save it to `returnval.json`.
2. Extract and unify an array of objects into one combined object in `unified.json`.
3. Generate `CREATE TABLE` SQL from the unified object.

## Features

- **Fetch API JSON** with robust HTTP error handling.
- **Path-based extraction** using either:
  - dot path (`properties.periods`), or
  - JSON pointer (`/properties/periods`).
- **Object unification** across array elements, including recursive nested object merges.
- **SQL generation** by flattening nested objects into columns and inferring PostgreSQL-friendly types.

## Installation

### Prerequisites

- Rust toolchain (stable), including `cargo`.

### Build

```bash
cargo build
```

## Usage

The CLI has three subcommands: `fetch`, `unify`, and `sql`.

```bash
cargo run -- <subcommand> [options]
```

---

### 1) Fetch JSON from an API

```bash
cargo run -- fetch --url "https://api.weather.gov/gridpoints/OKX/33,37/forecast"
```

By default this writes to:

- `returnval.json`

You can customize output path:

```bash
cargo run -- fetch \
  --url "https://api.weather.gov/gridpoints/OKX/33,37/forecast" \
  --out data/returnval.json
```

---

### 2) Unify an array of objects into one object

For the weather endpoint, the forecast periods live at `properties.periods`.

```bash
cargo run -- unify --path properties.periods
```

By default:

- reads `returnval.json`
- writes `unified.json`

Custom paths are supported:

```bash
cargo run -- unify \
  --input data/returnval.json \
  --path /properties/periods \
  --out data/unified.json
```

---

### 3) Generate SQL from unified JSON

```bash
cargo run -- sql --table forecast_periods
```

By default:

- reads `unified.json`
- prints SQL to stdout

Write SQL to file:

```bash
cargo run -- sql \
  --input data/unified.json \
  --table forecast_periods \
  --out schema.sql
```

## End-to-end example

```bash
cargo run -- fetch --url "https://api.weather.gov/gridpoints/OKX/33,37/forecast"
cargo run -- unify --path properties.periods
cargo run -- sql --table forecast_periods --out schema.sql
```

## SQL type inference

Current mapping:

- JSON `null` -> `TEXT`
- JSON `bool` -> `BOOLEAN`
- JSON integer (signed) -> `BIGINT`
- JSON integer (unsigned) -> `NUMERIC(20,0)`
- JSON float -> `DOUBLE PRECISION`
- JSON string -> `TEXT`
- JSON array/object -> `JSONB`

## Running tests

```bash
cargo test
```

If your environment blocks crates.io/network, dependency resolution may fail until network access is available.
