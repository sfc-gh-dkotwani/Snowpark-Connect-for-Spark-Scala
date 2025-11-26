/*
 * Snowpark Connect for Spark - Scala Example
 * Based on: https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client
 */

import org.apache.spark.sql.SparkSession

object SnowparkConnectExample {
  def main(args: Array[String]): Unit = {
    println("\n" + "="*60)
    println("🚀 Snowpark Connect for Spark - Scala Example")
    println("="*60)
    
    // Create Spark session with Snowpark Connect
    println("\n🔗 Connecting to Snowpark Connect server...")
    val spark = SparkSession
      .builder()
      .remote("sc://localhost:15002")
      .getOrCreate()
    
    println("✅ Connected!")
    
    try {
      // Simple DataFrame operations (as per official docs)
      import spark.implicits._
      
      println("\n📊 Test 1: Creating DataFrame from Scala collection")
      val data = Seq(
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35)
      )
      val df = spark.createDataFrame(data).toDF("id", "name", "age")
      
      println("\n✅ Original DataFrame:")
      df.show()
      
      println("\n📊 Test 2: Filtering (age > 28)")
      df.filter($"age" > 28).show()
      
      println("\n📊 Test 3: Aggregation (average age)")
      df.groupBy().avg("age").show()
      
      println("\n📊 Test 4: SQL Query with Spark Functions")
      // Note: Snowflake-specific functions like CURRENT_VERSION() are not supported
      // Use standard Spark SQL functions instead
      val sqlTest = spark.sql("""
        SELECT 
          'Snowflake' as PLATFORM,
          current_timestamp() as QUERY_TIME,
          1 + 1 as SIMPLE_CALC,
          count(*) as ROW_COUNT
        FROM (
          SELECT 1 as id UNION ALL
          SELECT 2 UNION ALL
          SELECT 3
        )
      """)
      
      println("\n✅ SQL Query Executed on Snowflake:")
      sqlTest.show(false)
      
      println("\n" + "="*60)
      println("✅ SUCCESS! All tests passed!")
      println("="*60)
      
    } catch {
      case e: Exception =>
        println("\n❌ Error: " + e.getMessage)
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
      println("\n✅ Spark session closed")
    }
  }
}

