from snowflake.snowpark import Session

connection_params = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "<your_role>",
    "warehouse": "<your_warehouse>",
    "database": "<your_database>",
    "schema": "<your_schema>",
}

session = Session.builder.configs(connection_params).create()

# Upload a single file
put_result = session.file.put(
    "file:///path/to/local/file.csv",
    "@my_stage/optional/path",
    auto_compress=False,
    overwrite=True,
)

for r in put_result:
    print(f"{r.source} -> {r.target}: {r.status}")

session.close()
