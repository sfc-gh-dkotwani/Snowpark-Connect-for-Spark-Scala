# Load JSON Data into VARIANT Column — Client Side (`snowpark-connect-execute-jar`)

This project demonstrates loading **JSON data from a Snowflake Internal Stage** into a table with a **VARIANT column** using **Snowpark Connect for Spark (Scala)**, executed via the `snowpark-connect-execute-jar` CLI.

Unlike the two-terminal server approach, this runs everything in a **single command, single process** — the Python SCOS server and JVM share the same OS process via JPype.

---

## How It Works

```
snowpark-connect-execute-jar
        │
        ├── Starts SCOS server (Python + gRPC on localhost)
        ├── Boots JVM via JPype (in the same process)
        ├── Sets SPARK_REMOTE=sc://127.0.0.1:<port>
        ├── Adds customer JAR to classpath
        ├── Invokes main() on the specified class
        │     └── SparkSession.builder().getOrCreate()
        │           connects to the in-process gRPC server
        ├── Blocks until main() completes
        └── Shuts down gRPC server and JVM
```

The Scala application:
1. Reads JSON data from a Snowflake Internal Stage (`@GCP_BILLING_STAGE`)
2. Validates records against a predefined schema using Spark's PERMISSIVE mode
3. Transforms valid records — structured fields into regular columns, nested `trade_details` kept as a complex struct (auto-converted to VARIANT by Snowpark Connect)
4. Appends data to `JSON_VARIANT_DATA_TABLE_CLIENT_SIDE` via `df.write.mode("append").saveAsTable()`

---

## Project Structure

```
snowpark-connect-execute-jar/
├── VariantDataLoaderClientSide.scala   # Scala application (main entry point)
├── build.sbt                           # SBT build config with sbt-assembly
├── project/
│   └── plugins.sbt                     # sbt-assembly plugin declaration
├── Create Table.sql                    # DDL for target table
├── commands.txt                        # Step-by-step setup and run instructions
└── README.md                           # This file
```

---

## Prerequisites

| Requirement | Version |
| :--- | :--- |
| Java | 11+ |
| sbt | Latest |
| Python | 3.10 – 3.12 |
| pip | Latest |

---

## Quick Start

### 1. Set Up Python Environment

Create the venv in a path with **no spaces** and under **128 characters** (macOS shebang limit):

```bash
python3 -m venv ~/venv_scos
source ~/venv_scos/bin/activate
pip install --upgrade "snowpark-connect[jdk]" snowflake-connector-python
```

Verify the CLI is available:

```bash
which snowpark-connect-execute-jar
```

### 2. Configure Snowflake Connection

Edit `~/.snowflake/connections.toml` with a `[spark-connect]` profile:

```toml
[spark-connect]
account = "<your-account>"
user = "<your-user>"
password = "<your-password-or-token>"
warehouse = "<your-warehouse>"
database = "<your-database>"
schema = "<your-schema>"
role = "<your-role>"
```

> No `host` field is needed. The `snowpark-connect` package resolves the Spark Connect endpoint from the account.

Set file permissions:

```bash
chmod 0600 ~/.snowflake/connections.toml
```

### 3. Create the Target Table

Run `Create Table.sql` in Snowflake to create `JSON_VARIANT_DATA_TABLE_CLIENT_SIDE`:

```sql
CREATE TABLE JSON_VARIANT_DATA_TABLE_CLIENT_SIDE (
    RECORD_ID       VARCHAR(50) NOT NULL,
    TRADE_DATE      DATE,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PORTFOLIO_ID    VARCHAR(50),
    ACCOUNT_ID      VARCHAR(50),
    TICKER_SYMBOL   VARCHAR(20),
    TRADE_TYPE      VARCHAR(10),
    QUANTITY        NUMBER(18,4),
    PRICE           NUMBER(18,6),
    CURRENCY        VARCHAR(3),
    TRADE_DETAILS   VARIANT,
    SOURCE_SYSTEM   VARCHAR(50),
    LOAD_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 4. Build the Fat JAR

```bash
sbt clean assembly
```

Output: `target/scala-2.12/variant-data-loader-client-side.jar`

### 5. Clear Stale Environment Variables

If you've previously used OSS or other Snowpark workflows, clear these to prevent them from overriding `connections.toml`:

```bash
unset SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_WAREHOUSE
unset SNOWPARK_CONNECT_HOST SNOWFLAKE_PAT
```

### 6. Run

```bash
snowpark-connect-execute-jar \
  --jar-file target/scala-2.12/variant-data-loader-client-side.jar \
  --main-class VariantDataLoaderClientSide
```

To specify a different JSON file (passed as a positional argument):

```bash
snowpark-connect-execute-jar \
  --jar-file target/scala-2.12/variant-data-loader-client-side.jar \
  --main-class VariantDataLoaderClientSide \
  -- sample_portfolio_trades.json
```

---

## CLI Reference

| Flag | Description |
| :--- | :--- |
| `--jar-file` | Path to the JAR file (required) |
| `--main-class` | Fully qualified main class name (required) |
| `--jars` | Comma-separated dependency JARs or glob patterns |
| `--port` | gRPC server port (default: 15002) |
| `--jvm-options` | JVM flags, e.g. `--jvm-options="-Xmx4g -Xms1g"` |
| `--verbose` | Enable DEBUG logging |
| `-- <args>` | Arguments passed to `main(String[] args)` |

---

## Key Files

### `VariantDataLoaderClientSide.scala`

The main Scala application. Key design choices:

- **`SparkSession.builder().getOrCreate()`** — no manual connection URL or PAT; `SPARK_REMOTE` is set automatically by the CLI
- **Fully qualified names** — stage and table references use `DATABASE.SCHEMA.OBJECT` to avoid session context issues
- **Single-pass validation** — one `.agg()` call counts total and invalid records in a single stage read instead of 3 separate `.count()` calls
- **Struct-to-VARIANT** — `trade_details` is kept as a complex Spark struct; Snowpark Connect automatically converts it to VARIANT on write
- **PERMISSIVE mode** — malformed JSON records are captured in `_corrupt_record` without failing the job

### `build.sbt`

- Scala 2.12.18, Spark Connect Client 3.5.6
- `sbt-assembly` plugin configured to produce a fat JAR with all dependencies bundled
- JVM options for Java module access (required by Spark/Netty)

### `Create Table.sql`

DDL script to create the target table with 13 columns, including `TRADE_DETAILS VARIANT` for semi-structured JSON data. Run this before the first execution.

---

## Table Schema

| Column | Type | Description |
| :--- | :--- | :--- |
| `RECORD_ID` | `VARCHAR(50)` | Unique trade identifier |
| `TRADE_DATE` | `DATE` | Date of the trade |
| `CREATED_AT` | `TIMESTAMP_NTZ` | Row creation timestamp |
| `PORTFOLIO_ID` | `VARCHAR(50)` | Portfolio identifier |
| `ACCOUNT_ID` | `VARCHAR(50)` | Account identifier |
| `TICKER_SYMBOL` | `VARCHAR(20)` | Stock/instrument ticker |
| `TRADE_TYPE` | `VARCHAR(10)` | BUY, SELL, or HOLD |
| `QUANTITY` | `NUMBER(18,4)` | Trade quantity |
| `PRICE` | `NUMBER(18,6)` | Trade price |
| `CURRENCY` | `VARCHAR(3)` | Currency code (e.g., USD) |
| `TRADE_DETAILS` | `VARIANT` | Nested JSON — order details, market data, risk metrics, fees, etc. |
| `SOURCE_SYSTEM` | `VARCHAR(50)` | Originating system |
| `LOAD_TIMESTAMP` | `TIMESTAMP_NTZ` | Data load timestamp |

### Querying the VARIANT Column

After data is loaded, access nested fields using Snowflake JSON path notation:

```sql
SELECT
    RECORD_ID,
    TICKER_SYMBOL,
    TRADE_DETAILS:order:order_type::STRING AS ORDER_TYPE,
    TRADE_DETAILS:market_data:volume::NUMBER AS VOLUME,
    TRADE_DETAILS:risk_metrics:beta::FLOAT AS BETA
FROM JSON_VARIANT_DATA_TABLE_CLIENT_SIDE;
```

---

## How This Differs from Other Approaches

| Aspect | Python Server (2 terminals) | OSS Client (direct) | `snowpark-connect-execute-jar` (this) |
| :--- | :--- | :--- | :--- |
| Terminals needed | 2 (server + sbt) | 1 | 1 |
| Server management | Manual start/stop | N/A | Automatic |
| Connection config in Scala | `SparkSession.builder().remote(url)` | Manual URL + PAT + host | `SparkSession.builder().getOrCreate()` |
| Authentication | `connections.toml` | PAT env var + host | `connections.toml` (handled by CLI) |
| Build artifact | Thin JAR via `sbt package` | Thin JAR via `sbt package` | Fat JAR via `sbt assembly` |
| Process model | Python server + JVM (separate) | JVM only | Single process (Python + JVM via JPype) |

---

## Troubleshooting

### `bad interpreter` error

The Python venv path is either too long (>128 bytes) or contains spaces. Create the venv in your home directory with a short name:

```bash
python3 -m venv ~/venv_scos
```

### Wrong database/schema/warehouse being used

Stale environment variables (`SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`) override `connections.toml`. Clear them:

```bash
unset SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_WAREHOUSE
unset SNOWPARK_CONNECT_HOST SNOWFLAKE_PAT
```

### `snowpark-connect-execute-jar` not found

Verify the correct package version is installed:

```bash
pip show snowpark-connect | grep Version
which snowpark-connect-execute-jar
```

The CLI name changed between versions — `snowpark-connect-jar` in some earlier releases, `snowpark-connect-execute-jar` in 1.19.0+.

### Dependency resolution errors

```bash
sbt clean
sbt update
sbt assembly
```

---

## References

- [Snowpark Connect for Spark — Snowflake Docs](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter)
- [Snowpark Connect pip package](https://pypi.org/project/snowpark-connect/)
- [sbt-assembly plugin](https://github.com/sbt/sbt-assembly)
