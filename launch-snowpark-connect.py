#!/usr/bin/env python3
"""
Launch Snowpark Connect for Spark server
Based on: https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-workloads-jupyter#run-scala-code-from-your-client
"""

from snowflake import snowpark_connect

def main():
    print("="*60)
    print("🚀 Starting Snowpark Connect for Spark Server")
    print("="*60)
    print()
    print("Server URL: sc://localhost:15002")
    print("⚠️  KEEP THIS TERMINAL OPEN!")
    print()
    print("In Terminal 2, run: sbt \"runMain SnowparkConnectExample\"")
    print("="*60)
    print()
    
    # Start the server (is_daemon=False keeps it running)
    snowpark_connect.start_session(is_daemon=False, remote_url="sc://localhost:15002")
    print("✅ Server started on port 15002")

if __name__ == "__main__":
    main()

