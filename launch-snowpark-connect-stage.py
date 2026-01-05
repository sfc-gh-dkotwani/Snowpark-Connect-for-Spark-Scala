#!/usr/bin/env python3
"""
Launch Snowpark Connect for Spark server for JSON Schema Validation

**UPDATED**: Configured for reading JSON from Snowflake Internal Stage
Stage: GCP_BILLING_STAGE

Based on: https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client
"""

import sys
import platform


# ============================================================
# SNOWFLAKE STAGE CONFIGURATION
# ============================================================
STAGE_NAME = "GCP_BILLING_STAGE"
DEFAULT_FILE_NAME = "01E091-D83A6B-1F55DD-gcp-billing-v1_cleansed-1.json"


def get_package_version(package_name):
    """Get package version dynamically"""
    try:
        import importlib.metadata
        return importlib.metadata.version(package_name)
    except Exception:
        return "unknown"


def get_pyspark_version():
    """Get PySpark version if available"""
    try:
        import pyspark
        return pyspark.__version__
    except ImportError:
        return "not installed"


def print_versions():
    """Display all environment versions"""
    print("="*70)
    print("🚀 SNOWPARK CONNECT SERVER - JSON SCHEMA VALIDATOR")
    print("📂 SOURCE: SNOWFLAKE INTERNAL STAGE")
    print("="*70)
    print()
    print("📋 SERVER ENVIRONMENT:")
    print("-"*70)
    print(f"   Python Version:       {sys.version.split()[0]}")
    print(f"   Python Path:          {sys.executable}")
    print(f"   Platform:             {platform.system()} ({platform.machine()})")
    print(f"   Snowpark Connect:     {get_package_version('snowpark-connect')}")
    print(f"   PySpark:              {get_pyspark_version()}")
    print(f"   Snowflake Connector:  {get_package_version('snowflake-connector-python')}")
    print("-"*70)
    print()


def print_stage_info():
    """Display Snowflake stage configuration"""
    print("📂 SNOWFLAKE STAGE CONFIGURATION:")
    print("-"*70)
    print(f"   Stage Name:           @{STAGE_NAME}")
    print(f"   Default File:         {DEFAULT_FILE_NAME}")
    print(f"   Full Stage Path:      @{STAGE_NAME}/{DEFAULT_FILE_NAME}")
    print("-"*70)
    print()


def main():
    from snowflake import snowpark_connect
    
    print_versions()
    print_stage_info()
    
    print("📡 SERVER CONFIGURATION:")
    print("-"*70)
    print("   Server URL:           sc://localhost:15002")
    print("   Mode:                 Non-daemon (foreground)")
    print("-"*70)
    print()
    print("⚠️  KEEP THIS TERMINAL OPEN!")
    print()
    print("="*70)
    print("📋 INSTRUCTIONS FOR READING FROM SNOWFLAKE STAGE:")
    print("="*70)
    print()
    print("1. Ensure the stage exists in Snowflake:")
    print(f"   SHOW STAGES LIKE '{STAGE_NAME}';")
    print()
    print("2. Verify the file is in the stage:")
    print(f"   LIST @{STAGE_NAME};")
    print()
    print("3. In Terminal 2, run the Scala validator:")
    print("   sbt \"runMain JsonSchemaValidatorFromStage\"")
    print()
    print("   Or with a custom filename:")
    print("   sbt \"runMain JsonSchemaValidatorFromStage your-file.json\"")
    print()
    print("="*70)
    print()
    
    # Start the server
    snowpark_connect.start_session(is_daemon=False, remote_url="sc://localhost:15002")
    print("✅ Server started on port 15002")


if __name__ == "__main__":
    main()

