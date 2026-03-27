/*
 * Load JSON Data into VARIANT Column — Client Side (snowpark-connect-execute-jar)
 *
 * This application:
 * 1. Reads JSON data from Snowflake Internal Stage
 * 2. Validates records against a predefined schema (PERMISSIVE mode)
 * 3. Loads structured fields into regular columns
 * 4. Loads complex nested struct directly into VARIANT column
 * 5. APPENDS data to existing Snowflake table using df.write.mode("append").saveAsTable()
 *
 * EXECUTION METHOD:
 * - Launched via snowpark-connect-execute-jar CLI (ships with snowpark-connect pip package)
 * - Single OS process: Python SCOS + JVM share the same process via JPype
 * - SPARK_REMOTE is set automatically by the CLI — no manual connection URL needed
 * - Authentication handled by snowpark-connect via ~/.snowflake/connections.toml
 * - SparkSession.builder().getOrCreate() auto-connects to the in-process gRPC server
 *
 * Stage: GCP_BILLING_STAGE
 * Table: JSON_VARIANT_DATA_TABLE_CLIENT_SIDE
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.{Duration, Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

object VariantDataLoaderClientSide {

  // ============================================================
  // VERSION INFORMATION (fetched dynamically)
  // ============================================================
  def getScalaVersion: String = util.Properties.versionNumberString
  def getJavaVersion: String = System.getProperty("java.version")
  def getJavaVendor: String = System.getProperty("java.vendor")
  def getOsName: String = System.getProperty("os.name")
  def getOsArch: String = System.getProperty("os.arch")

  // ============================================================
  // CONFIGURATION
  // ============================================================
  val DATABASE = "USECASE_DB"
  val SCHEMA = "USECASE_SCHEMA"
  val WAREHOUSE = "DKWHXS"
  val STAGE_NAME = "GCP_BILLING_STAGE"
  val DEFAULT_FILE_NAME = "portfolio_trades_10gb.json"
  val TARGET_TABLE = "JSON_VARIANT_DATA_TABLE_CLIENT_SIDE"

  // Fully qualified names (avoids dependency on session context)
  val FQ_STAGE = s"$DATABASE.$SCHEMA.$STAGE_NAME"
  val FQ_TABLE = s"$DATABASE.$SCHEMA.$TARGET_TABLE"

  // ============================================================
  // PREDEFINED SCHEMA FOR PORTFOLIO TRADING DATA
  // ============================================================
  def getPortfolioTradeSchema: StructType = {
    StructType(Seq(
      StructField("record_id", StringType, nullable = false),
      StructField("trade_date", StringType, nullable = true),
      StructField("portfolio_id", StringType, nullable = true),
      StructField("account_id", StringType, nullable = true),
      StructField("ticker_symbol", StringType, nullable = true),
      StructField("trade_type", StringType, nullable = true),
      StructField("quantity", DoubleType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("currency", StringType, nullable = true),
      StructField("trade_details", StructType(Seq(
        StructField("order", StructType(Seq(
          StructField("order_id", StringType, nullable = true),
          StructField("order_type", StringType, nullable = true),
          StructField("time_in_force", StringType, nullable = true),
          StructField("limit_price", DoubleType, nullable = true),
          StructField("stop_price", DoubleType, nullable = true),
          StructField("filled_quantity", DoubleType, nullable = true),
          StructField("average_fill_price", DoubleType, nullable = true),
          StructField("execution_venue", StringType, nullable = true),
          StructField("execution_time", StringType, nullable = true)
        )), nullable = true),
        StructField("market_data", StructType(Seq(
          StructField("bid", DoubleType, nullable = true),
          StructField("ask", DoubleType, nullable = true),
          StructField("last_trade", DoubleType, nullable = true),
          StructField("volume", LongType, nullable = true),
          StructField("vwap", DoubleType, nullable = true),
          StructField("high_52w", DoubleType, nullable = true),
          StructField("low_52w", DoubleType, nullable = true)
        )), nullable = true),
        StructField("risk_metrics", StructType(Seq(
          StructField("beta", DoubleType, nullable = true),
          StructField("volatility_30d", DoubleType, nullable = true),
          StructField("var_95", DoubleType, nullable = true),
          StructField("portfolio_weight", DoubleType, nullable = true)
        )), nullable = true),
        StructField("fees", StructType(Seq(
          StructField("commission", DoubleType, nullable = true),
          StructField("sec_fee", DoubleType, nullable = true),
          StructField("exchange_fee", DoubleType, nullable = true)
        )), nullable = true),
        StructField("tags", ArrayType(StringType), nullable = true),
        StructField("dividend_info", StructType(Seq(
          StructField("yield", DoubleType, nullable = true),
          StructField("ex_date", StringType, nullable = true),
          StructField("pay_date", StringType, nullable = true),
          StructField("amount_per_share", DoubleType, nullable = true),
          StructField("frequency", StringType, nullable = true)
        )), nullable = true),
        StructField("analyst_ratings", StructType(Seq(
          StructField("buy", IntegerType, nullable = true),
          StructField("hold", IntegerType, nullable = true),
          StructField("sell", IntegerType, nullable = true),
          StructField("target_price", DoubleType, nullable = true)
        )), nullable = true),
        StructField("bond_info", StructType(Seq(
          StructField("duration", DoubleType, nullable = true),
          StructField("yield_to_maturity", DoubleType, nullable = true),
          StructField("credit_quality", StringType, nullable = true)
        )), nullable = true),
        StructField("etf_info", StructType(Seq(
          StructField("expense_ratio", DoubleType, nullable = true),
          StructField("aum_billions", DoubleType, nullable = true),
          StructField("holdings_count", IntegerType, nullable = true),
          StructField("tracking_index", StringType, nullable = true)
        )), nullable = true),
        StructField("realized_pnl", StructType(Seq(
          StructField("cost_basis", DoubleType, nullable = true),
          StructField("proceeds", DoubleType, nullable = true),
          StructField("gain_loss", DoubleType, nullable = true),
          StructField("gain_loss_pct", DoubleType, nullable = true),
          StructField("holding_period_days", IntegerType, nullable = true),
          StructField("tax_treatment", StringType, nullable = true)
        )), nullable = true)
      )), nullable = true),
      StructField("source_system", StringType, nullable = true),
      StructField("_corrupt_record", StringType, nullable = true)
    ))
  }

  // ============================================================
  // RESULTS CLASS
  // ============================================================
  case class LoadResults(
    totalRecords: Long = 0,
    validRecords: Long = 0,
    invalidRecords: Long = 0,
    insertedRecords: Long = 0,
    loadTimestamp: String = "",
    filePath: String = "",
    stageName: String = "",
    targetTable: String = ""
  )

  // ============================================================
  // TIMING INFRASTRUCTURE
  // ============================================================
  case class StepTiming(
    stepName: String,
    startTime: Instant,
    endTime: Instant,
    durationMs: Long
  ) {
    def durationFormatted: String = {
      val totalSeconds = durationMs / 1000
      val hours = totalSeconds / 3600
      val minutes = (totalSeconds % 3600) / 60
      val seconds = totalSeconds % 60
      val millis = durationMs % 1000
      if (hours > 0) f"${hours}h ${minutes}%02dm ${seconds}%02ds"
      else if (minutes > 0) f"${minutes}m ${seconds}%02ds"
      else f"${seconds}.${millis}%03ds"
    }
  }

  val stepTimings: ArrayBuffer[StepTiming] = ArrayBuffer.empty[StepTiming]

  def timeStep[T](stepName: String)(block: => T): T = {
    val start = Instant.now()
    val startFmt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
    println(s"\n⏱️  [$stepName] Started at: $startFmt")
    val result = block
    val end = Instant.now()
    val endFmt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
    val durationMs = Duration.between(start, end).toMillis()
    val timing = StepTiming(stepName, start, end, durationMs)
    stepTimings += timing
    println(s"⏱️  [$stepName] Ended at:   $endFmt | Duration: ${timing.durationFormatted}")
    result
  }

  // ============================================================
  // MAIN ENTRY POINT
  // ============================================================
  def main(args: Array[String]): Unit = {
    println("\n" + "="*70)
    println("📊 LOAD JSON DATA INTO VARIANT COLUMN (CLIENT SIDE)")
    println("📂 SOURCE: SNOWFLAKE INTERNAL STAGE")
    println("🔗 MODE: snowpark-connect-execute-jar (single process)")
    println("="*70)

    printVersionInfo()

    val jsonFileName = if (args.length > 0) args(0) else DEFAULT_FILE_NAME
    val stagePath = s"@$FQ_STAGE/$jsonFileName"

    println(s"\n📁 Configuration:")
    println(s"   Database: $DATABASE")
    println(s"   Schema: $SCHEMA")
    println(s"   Warehouse: $WAREHOUSE")
    println(s"   Snowflake Stage: @$FQ_STAGE")
    println(s"   JSON File: $jsonFileName")
    println(s"   Full Stage Path: $stagePath")
    println(s"   Target Table: $FQ_TABLE")
    println(s"   Mode: APPEND (preserves existing data)")
    println("   Method: df.write.mode(\"append\").saveAsTable()")

    val jobStart = Instant.now()
    stepTimings.clear()

    // SPARK_REMOTE is set automatically by snowpark-connect-execute-jar.
    // SparkSession.builder().getOrCreate() reads it and connects to the
    // in-process gRPC server. No manual URL or PAT needed.
    println("\n" + "="*70)
    println("🔗 CONNECTING TO SNOWPARK CONNECT (in-process via SPARK_REMOTE)")
    println("="*70)

    val spark = timeStep("Spark Connection") {
      val sparkRemote = sys.env.getOrElse("SPARK_REMOTE", "not set (will use default)")
      println(s"   SPARK_REMOTE: $sparkRemote")

      val s = SparkSession.builder().getOrCreate()

      println(s"✅ Connected (Spark version: ${s.version})")
      s
    }

    try {
      // Step 1: Read and validate JSON data from stage (single-pass aggregation)
      val (dfValid, dfInvalid, results) = timeStep("Read & Validate (1-pass)") {
        readAndValidateFromStage(spark, stagePath, jsonFileName)
      }

      // Step 2: Show sample data
      timeStep("Show Sample Data") {
        showSampleData(dfValid, dfInvalid)
      }

      // Step 3: Transform data for VARIANT column loading (keep struct as-is)
      val dfTransformed = timeStep("Transform for VARIANT") {
        transformForVariantColumn(spark, dfValid)
      }

      // Step 4: Load to Snowflake table using df.write.mode("append").saveAsTable()
      println("\n" + "="*70)
      println("💾 APPENDING DATA TO SNOWFLAKE TABLE WITH VARIANT COLUMN")
      println("="*70)

      val insertedCount = timeStep("Append to Snowflake") {
        appendToSnowflake(spark, dfTransformed, FQ_TABLE, results)
      }

      val finalResults = results.copy(insertedRecords = insertedCount, targetTable = FQ_TABLE)

      // Print final summary (includes timing breakdown)
      val jobEnd = Instant.now()
      val jobDurationMs = Duration.between(jobStart, jobEnd).toMillis()
      printFinalSummary(finalResults, jobStart, jobEnd, jobDurationMs)

    } catch {
      case e: Exception =>
        println(s"\n❌ Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      println("\n✅ Spark session closed")
    }
  }

  // ============================================================
  // PRINT VERSION INFORMATION
  // ============================================================
  def printVersionInfo(): Unit = {
    println("\n📋 CLIENT ENVIRONMENT (fetched dynamically):")
    println("-"*70)
    println(f"   Scala Version:        $getScalaVersion")
    println(f"   Java Version:         $getJavaVersion")
    println(f"   Java Vendor:          $getJavaVendor")
    println(f"   OS:                   $getOsName ($getOsArch)")
    println(f"   Execution Mode:       snowpark-connect-execute-jar (single process)")
    println("-"*70)
  }

  // ============================================================
  // READ AND VALIDATE JSON FROM SNOWFLAKE STAGE
  //
  // Uses single-pass aggregation: one .agg() to get total + invalid counts
  // instead of 3 separate .count() calls (each re-reads from stage).
  // ============================================================
  def readAndValidateFromStage(spark: SparkSession, stagePath: String, fileName: String): (DataFrame, DataFrame, LoadResults) = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    println("\n" + "="*70)
    println("📋 READING & VALIDATING JSON FROM SNOWFLAKE STAGE")
    println("="*70)
    println(s"   Stage Path: $stagePath")
    println(s"   File Name: $fileName")
    println(s"   Started: $timestamp")

    println("\n🔍 Step 1: Reading JSON with predefined schema (PERMISSIVE mode)...")
    println(s"   - Reading from: $stagePath")
    println("   - Valid records: All fields populated as per schema")
    println("   - Invalid records: Captured in _corrupt_record column")

    val dfAll = spark.read
      .schema(getPortfolioTradeSchema)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("multiline", "false")
      .option("JsonFileParallelLoading", "true")
      .option("compression", "none")
      .json(stagePath)

    println("   🚀 Single-pass aggregation: counting total + invalid in one stage read...")

    val counts = dfAll.agg(
      count(lit(1)).as("total"),
      sum(when(col("_corrupt_record").isNotNull, 1).otherwise(0)).as("invalid")
    ).collect()(0)

    val totalRecords = counts.getLong(0)
    val invalidCount = counts.getLong(1)
    val validCount = totalRecords - invalidCount

    println(s"✅ Total records read from stage: $totalRecords")

    println("\n🔍 Step 2: Validation results (from single-pass aggregation)...")

    val validPct = if (totalRecords > 0) validCount.toDouble / totalRecords * 100 else 0.0
    val invalidPct = if (totalRecords > 0) invalidCount.toDouble / totalRecords * 100 else 0.0

    println(f"✅ Valid records: $validCount ($validPct%.2f%%)")
    println(f"⚠️  Invalid records: $invalidCount ($invalidPct%.2f%%)")

    println("\n🔍 Step 3: Preparing valid and invalid dataframes...")
    val dfInvalid = dfAll.filter(col("_corrupt_record").isNotNull)
    val dfValidClean = dfAll.filter(col("_corrupt_record").isNull).drop("_corrupt_record")
    println(s"✅ Valid dataframe ready with ${dfValidClean.columns.length} columns")

    if (invalidCount > 0) {
      println("\n⚠️  Sample invalid records:")
      dfInvalid.select("_corrupt_record").show(3, truncate = 100)
    }

    val results = LoadResults(
      totalRecords = totalRecords,
      validRecords = validCount,
      invalidRecords = invalidCount,
      loadTimestamp = timestamp,
      filePath = stagePath,
      stageName = FQ_STAGE,
      targetTable = FQ_TABLE
    )

    (dfValidClean, dfInvalid, results)
  }

  // ============================================================
  // SHOW SAMPLE DATA
  // ============================================================
  def showSampleData(dfValid: DataFrame, dfInvalid: DataFrame): Unit = {
    println("\n" + "="*70)
    println("📋 SAMPLE DATA PREVIEW")
    println("="*70)

    println("\n✅ Valid Records Sample (first 5):")
    dfValid.select(
      "record_id", "trade_date", "ticker_symbol", "trade_type", "quantity", "price"
    ).show(5, truncate = false)

    println("\n📊 Trade Details Sample (nested structure):")
    dfValid.select(
      "record_id", "ticker_symbol",
      "trade_details.order.order_type",
      "trade_details.market_data.volume",
      "trade_details.risk_metrics.beta"
    ).show(5, truncate = false)

    println("\n📋 Valid DataFrame Schema:")
    dfValid.printSchema()
  }

  // ============================================================
  // TRANSFORM DATA FOR VARIANT COLUMN
  // trade_details is kept as a complex struct — Snowpark Connect
  // automatically converts struct -> VARIANT in Snowflake.
  // ============================================================
  def transformForVariantColumn(spark: SparkSession, df: DataFrame): DataFrame = {
    println("\n" + "="*70)
    println("🔄 TRANSFORMING DATA FOR VARIANT COLUMN")
    println("="*70)

    println("   🔧 Keeping trade_details as complex struct (NOT converting to JSON string)")
    println("   📝 Snowpark Connect will automatically convert struct -> VARIANT")
    println("   📝 Adding CREATED_AT and LOAD_TIMESTAMP columns")
    println("   📝 Casting QUANTITY to decimal(18,4) and PRICE to decimal(18,6)")

    val dfTransformed = df
      .withColumn("RECORD_ID", col("record_id"))
      .withColumn("TRADE_DATE", to_date(col("trade_date"), "yyyy-MM-dd"))
      .withColumn("CREATED_AT", current_timestamp())
      .withColumn("PORTFOLIO_ID", col("portfolio_id"))
      .withColumn("ACCOUNT_ID", col("account_id"))
      .withColumn("TICKER_SYMBOL", col("ticker_symbol"))
      .withColumn("TRADE_TYPE", col("trade_type"))
      .withColumn("QUANTITY", col("quantity").cast("decimal(18,4)"))
      .withColumn("PRICE", col("price").cast("decimal(18,6)"))
      .withColumn("CURRENCY", col("currency"))
      .withColumn("TRADE_DETAILS", col("trade_details"))
      .withColumn("SOURCE_SYSTEM", col("source_system"))
      .withColumn("LOAD_TIMESTAMP", current_timestamp())
      .select(
        "RECORD_ID", "TRADE_DATE", "CREATED_AT",
        "PORTFOLIO_ID", "ACCOUNT_ID", "TICKER_SYMBOL",
        "TRADE_TYPE", "QUANTITY", "PRICE", "CURRENCY",
        "TRADE_DETAILS", "SOURCE_SYSTEM", "LOAD_TIMESTAMP"
      )

    println("   ✅ Transformation complete")
    println("\n📋 Transformed DataFrame Schema (13 columns to match table):")
    dfTransformed.printSchema()

    println("\n📋 TRADE_DETAILS column type:")
    val tradeDetailsField = dfTransformed.schema.fields.find(_.name == "TRADE_DETAILS")
    tradeDetailsField.foreach { field =>
      println(s"   Type: ${field.dataType.typeName}")
      println(s"   Full Type: ${field.dataType}")
    }

    dfTransformed
  }

  // ============================================================
  // APPEND TO SNOWFLAKE TABLE
  // ============================================================
  def appendToSnowflake(spark: SparkSession, df: DataFrame, tableName: String, results: LoadResults): Long = {
    val recordCount = results.validRecords
    println(s"\n💾 Appending $recordCount records to $tableName...")
    println("   - Mode: APPEND")
    println("   - Method: df.write.mode(\"append\").saveAsTable()")
    println("   - TRADE_DETAILS: Complex struct -> VARIANT (automatic conversion)")

    val countBefore = try {
      spark.sql(s"SELECT COUNT(*) as cnt FROM $tableName").collect()(0).getLong(0)
    } catch {
      case _: Exception => 0L
    }
    println(s"   - Records before insert: $countBefore")

    try {
      println(s"\n   🔄 Executing df.write.mode('append').saveAsTable('$tableName')...")

      df.write.mode("append").saveAsTable(tableName)

      val countAfter = spark.sql(s"SELECT COUNT(*) as cnt FROM $tableName").collect()(0).getLong(0)
      val insertedCount = countAfter - countBefore

      println(s"\n✅ APPEND completed successfully!")
      println(s"   - Records before: $countBefore")
      println(s"   - Records after: $countAfter")
      println(s"   - Records inserted: $insertedCount")

      insertedCount
    } catch {
      case e: Exception =>
        println(s"❌ Error during APPEND: ${e.getMessage}")
        throw e
    }
  }

  // ============================================================
  // VERIFY AND QUERY DATA
  // ============================================================
  def verifyAndQueryData(spark: SparkSession, tableName: String): Unit = {
    println("\n" + "="*70)
    println("🔍 VERIFYING AND QUERYING LOADED DATA")
    println("="*70)

    println("\n📊 Query 1: Table row count")
    spark.sql(s"SELECT COUNT(*) as total_records FROM $tableName").show()

    println("\n📊 Query 2: Sample records with basic columns")
    spark.sql(s"""
      SELECT RECORD_ID, TRADE_DATE, TICKER_SYMBOL, TRADE_TYPE, QUANTITY, PRICE
      FROM $tableName
      LIMIT 5
    """).show(truncate = false)

    println("\n✅ Data verification complete!")
    println(s"   📝 To query the VARIANT column, use Snowflake SQL directly:")
    println(s"      SELECT TRADE_DETAILS:order:order_type FROM $tableName")
  }

  // ============================================================
  // FINAL SUMMARY
  // ============================================================
  def printFinalSummary(results: LoadResults, jobStart: Instant, jobEnd: Instant, jobDurationMs: Long): Unit = {
    println("\n" + "="*70)
    println("✅ JSON DATA APPENDED TO VARIANT COLUMN SUCCESSFULLY!")
    println("📂 SOURCE: SNOWFLAKE INTERNAL STAGE")
    println("🔗 MODE: snowpark-connect-execute-jar (single process)")
    println("="*70)

    println("\n📊 VERSION SUMMARY:")
    println("-"*70)
    println(f"   Scala:                $getScalaVersion")
    println(f"   Java:                 $getJavaVersion")
    println(f"   Platform:             $getOsName ($getOsArch)")
    println(f"   Execution Mode:       snowpark-connect-execute-jar")
    println("-"*70)

    println("\n📊 LOAD SUMMARY:")
    println("-"*70)
    println(f"   Stage Name:           ${results.stageName}")
    println(f"   Stage Path:           ${results.filePath}")
    println(f"   Target Table:         ${results.targetTable}")
    println(f"   Mode:                 APPEND")
    println("   Method:               df.write.mode(\"append\").saveAsTable()")
    println(f"   Total Records Read:   ${results.totalRecords}")
    println(f"   Valid Records:        ${results.validRecords}")
    println(f"   Invalid Records:      ${results.invalidRecords}")
    println(f"   Records Inserted:     ${results.insertedRecords}")
    println(f"   Timestamp:            ${results.loadTimestamp}")
    println("-"*70)

    printTimingSummary(jobStart, jobEnd, jobDurationMs)

    println("\n📋 KEY APPROACH (CLIENT SIDE):")
    println("   ✅ Single process via snowpark-connect-execute-jar (Python SCOS + JVM via JPype)")
    println("   ✅ No manual connection URL, PAT, or host configuration in Scala code")
    println("   ✅ Authentication via ~/.snowflake/connections.toml (handled by snowpark-connect)")
    println("   ✅ Single-pass aggregation (1 stage read instead of 3 separate counts)")
    println("   ✅ trade_details kept as complex struct (auto-converted to VARIANT)")
    println("   ✅ Uses df.write.mode(\"append\").saveAsTable()")
    println("   ✅ All records validated (PERMISSIVE mode with corrupt record detection)")

    println("\n📋 NEXT STEPS:")
    println(s"   1. Query data using Snowflake SQL")
    println(s"   2. Access VARIANT column with JSON path notation in Snowflake:")
    println(s"      SELECT TRADE_DETAILS:order:order_type FROM ${results.targetTable}")
  }

  // ============================================================
  // TIMING SUMMARY
  // ============================================================
  def printTimingSummary(jobStart: Instant, jobEnd: Instant, jobDurationMs: Long): Unit = {
    val timeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val jobStartFmt = LocalDateTime.ofInstant(jobStart, java.time.ZoneId.systemDefault()).format(timeFmt)
    val jobEndFmt = LocalDateTime.ofInstant(jobEnd, java.time.ZoneId.systemDefault()).format(timeFmt)

    println("\n" + "="*70)
    println("⏱️  PROCESSING TIME SUMMARY")
    println("="*70)
    println(f"   Job Started:  $jobStartFmt")
    println(f"   Job Ended:    $jobEndFmt")

    val totalSec = jobDurationMs / 1000
    val hrs = totalSec / 3600
    val mins = (totalSec % 3600) / 60
    val secs = totalSec % 60
    val ms = jobDurationMs % 1000
    val totalFormatted = if (hrs > 0) f"${hrs}h ${mins}%02dm ${secs}%02ds"
                         else if (mins > 0) f"${mins}m ${secs}%02ds ${ms}%03dms"
                         else f"${secs}s ${ms}%03dms"
    println(f"   Total Time:   $totalFormatted ($jobDurationMs%,d ms)")

    println("\n   " + "-"*66)
    println(f"   ${"#"}%-4s ${"Step"}%-28s ${"Duration"}%-16s ${"% of Total"}%-10s")
    println("   " + "-"*66)

    stepTimings.zipWithIndex.foreach { case (t, idx) =>
      val pct = if (jobDurationMs > 0) (t.durationMs.toDouble / jobDurationMs * 100) else 0.0
      val bar = "█" * Math.max(1, (pct / 5).toInt)
      println(f"   ${idx + 1}%-4d ${t.stepName}%-28s ${t.durationFormatted}%-16s ${pct}%5.1f%%  $bar")
    }

    println("   " + "-"*66)
    println(f"   ${""}%-4s ${"TOTAL"}%-28s ${totalFormatted}%-16s ${"100.0"}%5s%%")
    println("   " + "-"*66)
  }
}
