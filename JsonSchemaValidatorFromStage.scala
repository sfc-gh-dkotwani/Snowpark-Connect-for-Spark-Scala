/*
 * JSON Schema Validation using Spark Read with PERMISSIVE mode
 * Leverages Spark's built-in schema enforcement to separate valid and invalid records
 * Uses Snowpark Connect for Snowflake integration
 * 
 * **UPDATED**: Reads JSON file from Snowflake Internal Stage instead of local file
 * Stage: GCP_BILLING_STAGE
 * 
 * Based on: https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object JsonSchemaValidatorFromStage {
  
  // ============================================================
  // VERSION INFORMATION (fetched dynamically)
  // ============================================================
  def getScalaVersion: String = util.Properties.versionNumberString
  def getJavaVersion: String = System.getProperty("java.version")
  def getJavaVendor: String = System.getProperty("java.vendor")
  def getOsName: String = System.getProperty("os.name")
  def getOsArch: String = System.getProperty("os.arch")
  
  // Spark client version - will be set from spark.version after connection
  var sparkClientVersion: String = "3.5.7"  // Default, updated after connection
  
  // ============================================================
  // SNOWFLAKE STAGE CONFIGURATION
  // ============================================================
  val STAGE_NAME = "GCP_BILLING_STAGE"
  val DEFAULT_FILE_NAME = "01E091-D83A6B-1F55DD-gcp-billing-v1_cleansed-1.json"
  
  // ============================================================
  // PREDEFINED SCHEMA FOR GCP BILLING DATA
  // Note: Using simple types compatible with Snowpark Connect
  // DecimalType is not fully supported, using DoubleType instead
  // ============================================================
  def getPredefinedBillingSchema: StructType = {
    StructType(Seq(
      StructField("billing_account_id", StringType, nullable = true),
      StructField("cost", StringType, nullable = true),
      StructField("currency", StringType, nullable = true),
      StructField("currency_conversion_rate", DoubleType, nullable = true),  // Changed from DecimalType
      StructField("export_time", StringType, nullable = true),
      StructField("invoice", StructType(Seq(
        //StructField("month", LongType, nullable = true),
        StructField("month", StringType, nullable = true),
        StructField("publisher_type", StringType, nullable = true)
      )), nullable = true),
      StructField("labels", ArrayType(StructType(Seq(
        StructField("key", StringType, nullable = true),
        StructField("value", StringType, nullable = true)
      ))), nullable = true),
      StructField("location", StringType, nullable = true),
      StructField("project", StringType, nullable = true),
      StructField("service", StructType(Seq(
        StructField("description", StringType, nullable = true),
        StructField("id", StringType, nullable = true)
      )), nullable = true),
      StructField("sku", StructType(Seq(
        StructField("description", StringType, nullable = true),
        StructField("id", StringType, nullable = true)
      )), nullable = true),
      StructField("usage", StructType(Seq(
        StructField("amount", StringType, nullable = true),
        StructField("amount_in_pricing_units", DoubleType, nullable = true),
        StructField("pricing_unit", StringType, nullable = true),
        StructField("unit", StringType, nullable = true)
      )), nullable = true),
      StructField("usage_end_time", StringType, nullable = true),
      StructField("usage_start_time", StringType, nullable = true),
      // Corrupt record column for PERMISSIVE mode
      StructField("_corrupt_record", StringType, nullable = true)
    ))
  }
  
  // ============================================================
  // VALIDATION RESULTS CLASS
  // ============================================================
  case class ValidationResults(
    totalRecords: Long = 0,
    validRecords: Long = 0,
    invalidRecords: Long = 0,
    validationTimestamp: String = "",
    filePath: String = "",
    stageName: String = "",
    nullValueStats: Map[String, NullStats] = Map.empty,
    businessRuleViolations: List[String] = List.empty
  )
  
  case class NullStats(nullCount: Long, nullPercentage: Double)
  
  // ============================================================
  // MAIN ENTRY POINT
  // ============================================================
  def main(args: Array[String]): Unit = {
    println("\n" + "="*70)
    println("🔍 JSON SCHEMA VALIDATION USING SPARK (SCALA)")
    println("📂 SOURCE: SNOWFLAKE INTERNAL STAGE")
    println("="*70)
    
    // Print version information
    printVersionInfo()
    
    // Configuration - File name from args or default
    val jsonFileName = if (args.length > 0) args(0) else DEFAULT_FILE_NAME
    val stagePath = s"@$STAGE_NAME/$jsonFileName"
    val validTable = "GCP_BILLING_VALID_SCALA_SCHEMA_0105_STAGE"
    val invalidTable = "GCP_BILLING_INVALID_SCALA_SCHEMA_0105_STAGE"
    
    println(s"\n📁 Snowflake Stage: @$STAGE_NAME")
    println(s"📁 JSON File: $jsonFileName")
    println(s"📁 Full Stage Path: $stagePath")
    println(s"📊 Valid Table: $validTable")
    println(s"⚠️  Invalid Table: $invalidTable")
    
    // Create Spark session
    println("\n" + "="*70)
    println("🔗 CONNECTING TO SNOWPARK CONNECT SERVER")
    println("="*70)
    
    val spark = SparkSession
      .builder()
      .remote("sc://localhost:15002")
      .getOrCreate()
    
    // Get Spark version from the connected session
    val serverSparkVersion = spark.version
    sparkClientVersion = serverSparkVersion  // Update client version from server
    println(s"✅ Connected to Spark version: $serverSparkVersion")
    
    try {
      // Run validation - reading from Snowflake stage
      val (dfValid, dfInvalid, results) = readAndValidateFromStage(spark, stagePath, jsonFileName)
      
      // Print summary
      printValidationSummary(results)
      
      // Show sample data
      showSampleData(dfValid, dfInvalid)
      
      // Load to Snowflake tables
      println("\n" + "="*70)
      println("💾 LOADING DATA TO SNOWFLAKE TABLES")
      println("="*70)
      
      if (results.validRecords > 0) {
        loadToSnowflake(spark, dfValid, validTable, "valid")
      }
      
      if (results.invalidRecords > 0) {
        loadToSnowflake(spark, dfInvalid, invalidTable, "invalid")
      }
      
      // Final version summary
      printFinalSummary(serverSparkVersion, results)
      
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
    println(f"   Spark Connect:        3.5.7 (build dependency)")
    println("-"*70)
  }
  
  // ============================================================
  // READ AND VALIDATE JSON FILE FROM SNOWFLAKE STAGE
  // Key change: Uses @STAGE_NAME/filename syntax for Snowflake stage
  // ============================================================
  def readAndValidateFromStage(spark: SparkSession, stagePath: String, fileName: String): (DataFrame, DataFrame, ValidationResults) = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    
    println("\n" + "="*70)
    println("📋 SPARK SCHEMA VALIDATION (PERMISSIVE MODE)")
    println("📂 READING FROM SNOWFLAKE INTERNAL STAGE")
    println("="*70)
    println(s"Stage Path: $stagePath")
    println(s"File Name: $fileName")
    println(s"Started: $timestamp")
    
    // Step 1: Read JSON from Snowflake stage with predefined schema using PERMISSIVE mode
    // The stagePath uses the @STAGE_NAME/filename format for Snowflake internal stages
    println("\n🔍 Step 1: Reading JSON from Snowflake stage with predefined schema (PERMISSIVE mode)...")
    println(s"   - Reading from: $stagePath")
    println("   - Valid records: All fields populated as per schema")
    println("   - Invalid records: Captured in _corrupt_record column")
    
    val dfAll = spark.read
      .schema(getPredefinedBillingSchema)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("multiline", "true")
      .json(stagePath)  // Reading from Snowflake stage using @STAGE_NAME/filename path
    
    val totalRecords = dfAll.count()
    println(s"✅ Total records read from stage: $totalRecords")
    
    // Step 2: Separate valid and invalid records
    println("\n🔍 Step 2: Separating valid and invalid records...")
    
    // Invalid records have non-null _corrupt_record
    val dfInvalid = dfAll.filter(col("_corrupt_record").isNotNull)
    val invalidCount = dfInvalid.count()
    
    // Valid records have null _corrupt_record
    val dfValid = dfAll.filter(col("_corrupt_record").isNull)
    val validCount = dfValid.count()
    
    val validPct = if (totalRecords > 0) validCount.toDouble / totalRecords * 100 else 0.0
    val invalidPct = if (totalRecords > 0) invalidCount.toDouble / totalRecords * 100 else 0.0
    
    println(f"✅ Valid records: $validCount ($validPct%.2f%%)")
    println(f"⚠️  Invalid records: $invalidCount ($invalidPct%.2f%%)")
    
    // Step 3: Clean valid dataframe
    println("\n🔍 Step 3: Cleaning valid dataframe...")
    val dfValidClean = dfValid.drop("_corrupt_record")
    println(s"✅ Valid dataframe ready with ${dfValidClean.columns.length} columns")
    
    // Step 4: Analyze invalid records
    if (invalidCount > 0) {
      println("\n🔍 Step 4: Analyzing invalid records...")
      analyzeInvalidRecords(dfInvalid)
    }
    
    // Step 5: Analyze data quality
    val (nullStats, violations) = if (validCount > 0) {
      println("\n🔍 Step 5: Analyzing data quality on valid records...")
      analyzeDataQuality(dfValidClean)
    } else {
      (Map.empty[String, NullStats], List.empty[String])
    }
    
    // Create results - includes stage information
    val results = ValidationResults(
      totalRecords = totalRecords,
      validRecords = validCount,
      invalidRecords = invalidCount,
      validationTimestamp = timestamp,
      filePath = stagePath,
      stageName = STAGE_NAME,
      nullValueStats = nullStats,
      businessRuleViolations = violations
    )
    
    (dfValidClean, dfInvalid, results)
  }
  
  // ============================================================
  // ANALYZE INVALID RECORDS
  // ============================================================
  def analyzeInvalidRecords(dfInvalid: DataFrame): Unit = {
    println("\n📊 Invalid Records Analysis:")
    
    val invalidCount = dfInvalid.count()
    println(s"   Total invalid records: $invalidCount")
    
    // Show sample of corrupt records
    println("\n   Sample corrupt records (first 5):")
    val samples = dfInvalid.select("_corrupt_record").limit(5).collect()
    samples.zipWithIndex.foreach { case (row, idx) =>
      val corruptRecord = row.getString(0)
      val truncated = if (corruptRecord.length > 200) corruptRecord.take(200) + "..." else corruptRecord
      println(s"   ${idx + 1}. $truncated")
    }
  }
  
  // ============================================================
  // ANALYZE DATA QUALITY
  // ============================================================
  def analyzeDataQuality(dfValid: DataFrame): (Map[String, NullStats], List[String]) = {
    println("\n📊 Data Quality Analysis (Valid Records):")
    
    val totalValid = dfValid.count()
    var violations = List.empty[String]
    
    // Null value analysis
    println("\n   Null value statistics:")
    val keyFields = Seq("billing_account_id", "cost", "currency", "project", "service", "sku")
    
    val nullStats = keyFields.map { field =>
      val nullCount = dfValid.filter(col(field).isNull).count()
      val nullPercentage = if (totalValid > 0) nullCount.toDouble / totalValid * 100 else 0.0
      
      if (nullCount > 0) {
        println(f"   - $field%-25s $nullCount%8d nulls ($nullPercentage%6.2f%%)")
      } else {
        println(f"   - $field%-25s ${"✓"}%8s (no nulls)")
      }
      
      field -> NullStats(nullCount, nullPercentage)
    }.toMap
    
    // Business rule validation
    println("\n   Business rule validation:")
    
    // Rule 1: billing_account_id should not be null
    val nullBilling = dfValid.filter(col("billing_account_id").isNull).count()
    if (nullBilling > 0) {
      val msg = s"$nullBilling valid records with null billing_account_id"
      println(s"   ⚠️  $msg")
      violations = violations :+ msg
    } else {
      println("   ✅ All records have billing_account_id")
    }
    
    // Rule 2: cost should be numeric
    try {
      val nonNumericCost = dfValid.filter(
        col("cost").isNotNull && !col("cost").rlike("^-?[0-9]+(\\.[0-9]+)?$")
      ).count()
      if (nonNumericCost > 0) {
        val msg = s"$nonNumericCost records with non-numeric cost"
        println(s"   ⚠️  $msg")
        violations = violations :+ msg
      } else {
        println("   ✅ All cost values are numeric")
      }
    } catch {
      case e: Exception => println(s"   ⚠️  Could not validate cost: ${e.getMessage}")
    }
    
    (nullStats, violations)
  }
  
  // ============================================================
  // PRINT VALIDATION SUMMARY
  // ============================================================
  def printValidationSummary(results: ValidationResults): Unit = {
    println("\n" + "="*70)
    println("📋 VALIDATION SUMMARY REPORT")
    println("="*70)
    
    val validPct = if (results.totalRecords > 0) results.validRecords.toDouble / results.totalRecords * 100 else 0.0
    val invalidPct = if (results.totalRecords > 0) results.invalidRecords.toDouble / results.totalRecords * 100 else 0.0
    
    println("\n📂 Source Information:")
    println(f"   Stage Name:       ${results.stageName}")
    println(f"   Stage Path:       ${results.filePath}")
    
    println("\n📊 Record Statistics:")
    println(f"   Total records:    ${results.totalRecords}%10d")
    println(f"   Valid records:    ${results.validRecords}%10d ($validPct%.2f%%)")
    println(f"   Invalid records:  ${results.invalidRecords}%10d ($invalidPct%.2f%%)")
    
    println("\n📊 Validation Status:")
    if (results.invalidRecords == 0 && results.businessRuleViolations.isEmpty) {
      println("   ✅ VALIDATION PASSED - All records valid, no business rule violations")
    } else if (results.invalidRecords == 0 && results.businessRuleViolations.nonEmpty) {
      println("   ⚠️  VALIDATION PASSED WITH WARNINGS - All records schema-valid but with data quality issues")
    } else if (results.invalidRecords > 0 && results.businessRuleViolations.isEmpty) {
      println("   ⚠️  VALIDATION PARTIAL - Some records failed schema validation")
    } else {
      println("   ⚠️  VALIDATION ISSUES - Schema failures and data quality issues found")
    }
    
    if (results.businessRuleViolations.nonEmpty) {
      println(s"\n⚠️  Business Rule Violations (${results.businessRuleViolations.size}):")
      results.businessRuleViolations.foreach(v => println(s"   - $v"))
    }
  }
  
  // ============================================================
  // SHOW SAMPLE DATA
  // ============================================================
  def showSampleData(dfValid: DataFrame, dfInvalid: DataFrame): Unit = {
    println("\n" + "="*70)
    println("📋 SAMPLE DATA")
    println("="*70)
    
    println("\n✅ Valid Records Sample (first 3):")
    dfValid.select(
      "billing_account_id", "cost", "currency", "project"
    ).show(3, truncate = false)
    
    if (dfInvalid.count() > 0) {
      println("\n⚠️  Invalid Records Sample (first 3):")
      dfInvalid.select("_corrupt_record").show(3, truncate = 100)
    }
    
    println("\n📋 Valid DataFrame Schema:")
    dfValid.printSchema()
  }
  
  // ============================================================
  // LOAD TO SNOWFLAKE
  // ============================================================
  def loadToSnowflake(spark: SparkSession, df: DataFrame, tableName: String, recordType: String): Unit = {
    try {
      val recordCount = df.count()
      println(s"\n💾 Loading $recordCount $recordType records to $tableName...")
      
      // Write to Snowflake table
      df.write.mode("overwrite").saveAsTable(tableName)
      
      // Verify
      val verifyCount = spark.sql(s"SELECT COUNT(*) as cnt FROM $tableName").collect()(0).getLong(0)
      println(s"✅ Table $tableName created with $verifyCount records")
      
    } catch {
      case e: Exception =>
        println(s"❌ Error loading to $tableName: ${e.getMessage}")
    }
  }
  
  // ============================================================
  // FINAL SUMMARY
  // ============================================================
  def printFinalSummary(serverSparkVersion: String, results: ValidationResults): Unit = {
    println("\n" + "="*70)
    println("✅ JSON SCHEMA VALIDATION COMPLETE!")
    println("📂 SOURCE: SNOWFLAKE INTERNAL STAGE")
    println("="*70)
    
    println("\n📊 VERSION SUMMARY (all fetched dynamically):")
    println("-"*70)
    println(f"   Scala:                $getScalaVersion")
    println(f"   Java:                 $getJavaVersion")
    println(f"   Spark Version:        $serverSparkVersion")
    println(f"   Platform:             $getOsName ($getOsArch)")
    println("-"*70)
    
    println("\n📊 PROCESSING SUMMARY:")
    println("-"*70)
    println(f"   Stage Name:           ${results.stageName}")
    println(f"   Stage Path:           ${results.filePath}")
    println(f"   Total Records:        ${results.totalRecords}")
    println(f"   Valid Records:        ${results.validRecords}")
    println(f"   Invalid Records:      ${results.invalidRecords}")
    println(f"   Timestamp:            ${results.validationTimestamp}")
    println("-"*70)
  }
}

