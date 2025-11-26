name := "snowpark-connect-scala"
version := "1.0.0"
scalaVersion := "2.12.18"

// Spark Connect Client - Based on official Snowflake documentation
// https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-connect-client-jvm" % "3.5.3"
)

// Add JVM options for Java 9+ module system compatibility
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// Assembly settings
assembly / assemblyJarName := "snowpark-connect-scala.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

