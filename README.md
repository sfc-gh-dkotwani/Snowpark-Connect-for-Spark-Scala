# Snowpark Connect for Spark - Scala Example

**Official Snowflake Documentation:** [Run Scala code from your client](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client)

This project demonstrates running Scala Spark workloads on Snowflake using Snowpark Connect for Spark.

---

## 📋 Quick Start

### Step 1: Set Up Python Environment

# Create working directory
mkdir scala-workload
cd scala-workload

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Snowpark Connect
pip install --upgrade --force-reinstall snowpark-connect


### Step 2: Configure Snowflake Connection

Create or edit `~/.snowflake/connections.toml`;

**Set permissions:**
chmod 0600 ~/.snowflake/connections.toml


### Step 3: Copy Project Files

# Copy all required files
cp /xx/launch-snowpark-connect.py .
cp /xx/SnowparkConnectExample.scala .
cp /xx/build.sbt .
cp /xx/project .

### Step 4: Download Dependencies

# Download Spark Connect client (first time only, ~1-2 min)
sbt update

---

## 🚀 Running the Application

### Terminal 1: Start Snowpark Connect Server

cd scala-workload
source .venv/bin/activate
python launch-snowpark-connect.py

**Expected output:**
============================================================
🚀 Starting Snowpark Connect for Spark Server
============================================================

Server URL: sc://localhost:15002
⚠️  KEEP THIS TERMINAL OPEN!

In Terminal 2, run: sbt "runMain SnowparkConnectExample"
============================================================

✅ Server started on port 15002

**Keep this terminal running!**

---

### Terminal 2: Run Scala Application

cd scala-workload
sbt "runMain SnowparkConnectExample"

**Expected output:**
============================================================
🚀 Snowpark Connect for Spark - Scala Example
============================================================

🔗 Connecting to Snowpark Connect server...
✅ Connected!

📊 Test 1: Creating DataFrame from Scala collection

✅ Original DataFrame:
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|  Alice| 25|
|  2|    Bob| 30|
|  3|Charlie| 35|
+---+-------+---+

📊 Test 2: Filtering (age > 28)
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  2|    Bob| 30|
|  3|Charlie| 35|
+---+-------+---+

📊 Test 3: Aggregation (average age)
+--------+
|avg(age)|
+--------+
|    30.0|
+--------+

📊 Test 4: SQL Query with Spark Functions

✅ SQL Query Executed on Snowflake:
+----------+----------------------+-----------+---------+
|PLATFORM  |QUERY_TIME            |SIMPLE_CALC|ROW_COUNT|
+----------+----------------------+-----------+---------+
|Snowflake |2024-01-15 10:30:45.0 |2          |3        |
+----------+----------------------+-----------+---------+

============================================================
✅ SUCCESS! All tests passed!
============================================================

---

## 📁 Project Structure

scala-workload/
├── .venv/                         # Python virtual environment
├── launch-snowpark-connect.py     # Server launcher script
├── SnowparkConnectExample.scala   # Scala application (official example)
├── build.sbt                      # SBT build file
├── project/
│   ├── build.properties          # SBT version
│   └── plugins.sbt               # SBT plugins
└── target/                       # Compiled output

---

## 🔧 Key Components

### 1. Server Launcher (`launch-snowpark-connect.py`)

Based on official Snowflake docs, this script:
- Starts Snowpark Connect server on port 15002
- Runs in non-daemon mode (stays in foreground)
- Uses connection from `~/.snowflake/connections.toml`

```python
from snowflake import snowpark_connect

snowpark_connect.start_session(
    is_daemon=False, 
    remote_url="sc://localhost:15002"
)
```

### 2. Scala Application (`SnowparkConnectExample.scala`)

Official example from Snowflake documentation demonstrating:
- ✅ DataFrame creation from Scala collections
- ✅ Filtering operations
- ✅ Aggregations
- ✅ SQL queries using Spark SQL (not Snowflake-specific functions)
- ✅ Proper connection and session management

```scala
val spark = SparkSession
  .builder()
  .remote("sc://localhost:15002")
  .getOrCreate()
```

### 3. Build Configuration (`build.sbt`)

Uses official Spark version and settings:
- Spark Connect Client 3.5.3 (as per Snowflake docs)
- Java 9+ compatibility options
- Scala 2.12.18

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.3"
)

javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
```

---

## 🆘 Troubleshooting

### Connection Issues

**Problem:** Server won't start

**Solution:**
```bash
# Check if port is in use
lsof -i :15002

# If occupied, kill the process
pkill -f snowpark-connect

# Restart
python launch-snowpark-connect.py
```

---

### Missing Connection File

**Problem:** `Connection 'spark-connect' not found`

**Solution:**
```bash
# Verify file exists
cat ~/.snowflake/connections.toml

# Should contain [spark-connect] section
# If missing, create it with your Snowflake credentials
```

---

### Compilation Errors

**Problem:** Dependency resolution errors

**Solution:**
```bash
# Clean and rebuild
sbt clean
sbt update
sbt compile
```

---

### Architecture Mismatch

**Problem:** Java/Python architecture conflict

**Solution:**
Ensure both Java and Python are the same architecture:
```bash
# Check Python architecture
python3 -c "import platform; print(platform.machine())"

# Check Java architecture
java -version

# Both should show same (e.g., arm64 or x86_64)
```

---

## 📚 What This Demonstrates

This example shows:

1. **Official Snowflake Approach**: Uses exact code from Snowflake documentation
2. **Spark Connect Client**: Thin client connecting to Snowflake infrastructure
3. **DataFrame Operations**: Creating, filtering, and aggregating data
4. **SQL Execution**: Standard Spark SQL queries executed on Snowflake
5. **Production Pattern**: Proper session management and error handling

---

## 🔗 References

- **Snowflake Docs**: [Run Scala code from your client](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client)
- **Spark Connect**: Apache Spark 3.5.3 Connect Client
- **Snowpark Connect Package**: `pip install snowpark-connect`

---

## ✅ Success Criteria

You'll know it's working when you see:

- ✅ Server starts and shows "Server started on port 15002"
- ✅ Scala application connects successfully
- ✅ All 4 tests pass:
  - DataFrame creation ✅
  - Filtering ✅
  - Aggregation ✅
  - SQL query execution ✅
- ✅ No connection errors
- ✅ Clean session shutdown

**Note:** You may see Java reflection warnings - these are harmless and can be ignored.
