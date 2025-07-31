import snowflake.connector

SF_ACCOUNT = "EG48488"  # Update with correct account identifier
SF_USER = "your_snowflake_user"
SF_PASSWORD = "your_snowflake_password"
SF_DATABASE = "poc_db"
SF_SCHEMA = "poc_schema"
SF_WAREHOUSE = "poc_warehouse"

try:
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    print("Snowflake connection successful:", conn.cursor().execute("SELECT CURRENT_VERSION()").fetchone())
    conn.close()
except snowflake.connector.errors.Error as e:
    print(f"Snowflake connection error: {e}")   