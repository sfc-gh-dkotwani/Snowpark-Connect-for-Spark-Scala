// ============================================================
// VARIANT Data Loader - Client Side (snowpark-connect-execute-jar)
// ============================================================
// Builds a fat JAR to be executed via snowpark-connect-execute-jar CLI.
// Single process: Python SCOS + JVM share the same process via JPype.
// No manual Spark Connect URL or PAT needed in Scala code.
// ============================================================

name := "variant-data-loader-client-side"
version := "1.0.0"

// ============================================================
// VERSION CONFIGURATION
// ============================================================
val scalaVer = "2.12.18"
val sparkConnectVersion = "3.5.6"

scalaVersion := scalaVer

// Spark Connect Client dependency (standard OSS package)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-connect-client-jvm" % sparkConnectVersion
)

// ============================================================
// RESOURCE DIRECTORIES (for log4j2.properties)
// ============================================================
Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "resources"

// ============================================================
// JVM OPTIONS
// ============================================================
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dio.netty.handler.ssl.noOpenSsl=true",
  "-Dio.grpc.netty.shaded.io.netty.handler.ssl.noOpenSsl=true",
  "-Dorg.sparkproject.io.netty.handler.ssl.noOpenSsl=true",
  "-Dorg.sparkproject.io.grpc.netty.shaded.io.netty.handler.ssl.noOpenSsl=true"
)

fork := true

run / javaOptions += "-Dlog4j2.disable.jmx=true"

// ============================================================
// ASSEMBLY SETTINGS (fat JAR for snowpark-connect-execute-jar)
// ============================================================
assembly / assemblyJarName := "variant-data-loader-client-side.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "log4j2.properties" => MergeStrategy.first
  case "log4j2-defaults.properties" => MergeStrategy.first
  case _ => MergeStrategy.first
}
