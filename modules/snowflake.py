import snowflake.connector
import pandas as pd
from datetime import datetime
import os

class SnowflakeLoader:
    def __init__(self, config):
        self.config = config
        self.snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
        if not self.snowflake_password:
            raise ValueError("SNOWFLAKE_PASSWORD not found in .env file")
        self.conn = self.connect_raw()  # Establish connection once in init for reuse

    def connect_raw(self):
        """Create a raw Snowflake connection (no DictCursor for simplicity)."""
        conn = snowflake.connector.connect(
            user=self.config["snowflake"]["user"],
            password=self.snowflake_password,
            account=self.config["snowflake"]["account"],
            warehouse=self.config["snowflake"]["warehouse"],
            database=self.config["snowflake"]["database"],
            schema=self.config["snowflake"]["schema"]
        )
        print("Snowflake raw connection established")
        return conn

    def get_latest_created_on(self):
        """Get the latest sys_created_on timestamp from Snowflake table."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{self.config['snowflake']['table']}' IN {self.config['snowflake']['database']}.{self.config['snowflake']['schema']}")
            if not cursor.fetchone():
                print("Table does not exist, no latest created_on timestamp")
                return None

            cursor.execute(f'SELECT MAX("sys_created_on") AS max_created_on FROM {self.config["snowflake"]["database"]}.{self.config["snowflake"]["schema"]}.{self.config["snowflake"]["table"]}')
            result = cursor.fetchone()
            print(f"Raw result from fetchone: {result}")  # For debugging
            latest_timestamp = result[0] if result else None  # Positional access
            if latest_timestamp:
                print(f"Latest created_on in Snowflake: {latest_timestamp}")
                return latest_timestamp
            else:
                print("No data in table, fetching all tickets")
                return None
        except Exception as e:
            print(f"Error fetching latest created_on: {e}")
            return None

    def create_table(self, sample_df=None):
        """Create table dynamically based on sample DataFrame columns or inferred structure, escaping reserved keywords."""
        # Note: Since we're loading from Parquet now, we pass a sample_df if available for schema inference.
        conn = self.conn
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW WAREHOUSES LIKE '{self.config['snowflake']['warehouse']}'")
            if not cursor.fetchone():
                raise Exception(f"Warehouse {self.config['snowflake']['warehouse']} does not exist.")

            # If sample_df is provided, use it for dynamic column creation; otherwise, use a default schema
            if sample_df is not None and not sample_df.empty:
                snowflake_columns = []
                for col in sample_df.columns:
                    dtype = str(sample_df[col].dtype)
                    if "datetime" in dtype or col in ["sys_created_on", "sys_updated_on", "opened_at", "resolved_at", "closed_at"]:
                        col_type = "TIMESTAMP"
                    else:
                        col_type = "STRING"
                    if col == "number":
                        col_type += " PRIMARY KEY UNIQUE"
                    snowflake_columns.append(f'"{col}" {col_type}')
            else:
                # Default schema if no sample_df (adjust based on your expected columns)
                snowflake_columns = [
                    '"number" STRING PRIMARY KEY UNIQUE',
                    '"short_description" STRING',
                    '"sys_created_on" TIMESTAMP',
                    '"sys_updated_on" TIMESTAMP',
                    '"opened_at" TIMESTAMP',
                    '"resolved_at" TIMESTAMP',
                    '"closed_at" TIMESTAMP',
                    # Add more default columns as needed
                ]

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.config['snowflake']['database']}.{self.config['snowflake']['schema']}.{self.config['snowflake']['table']} (
                {', '.join(snowflake_columns)}
            )
            """
            cursor.execute(create_table_sql)
            print(f"Table {self.config['snowflake']['database']}.{self.config['snowflake']['schema']}.{self.config['snowflake']['table']} created or verified")

            cursor.execute(f"USE WAREHOUSE {self.config['snowflake']['warehouse']}")
            print(f"Warehouse {self.config['snowflake']['warehouse']} activated")
        except Exception as e:
            print(f"Error creating table: {e}")
            raise

    def create_s3_stage(self):
        """Create an external stage pointing to S3."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
            CREATE STAGE IF NOT EXISTS s3_stage
            URL = 's3://{self.config['s3']['bucket']}/{self.config['s3']['prefix']}'
            CREDENTIALS = (AWS_KEY_ID = '{os.getenv("AWS_ACCESS_KEY_ID")}' AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}')
            FILE_FORMAT = (TYPE = 'PARQUET');
            """)
            print("S3 stage created")
        except Exception as e:
            print(f"Error creating S3 stage: {e}")
            raise

    def copy_from_s3(self, s3_key, sample_df):
        """Copy data from S3 Parquet to Snowflake using a temp table and dynamic MERGE for upserts."""
        try:
            cursor = self.conn.cursor()
            temp_table = "temp_incident_load"

            # Compute relative path from s3_key (strip the prefix)
            relative = s3_key.replace(self.config['s3']['prefix'], '', 1)

            # Step 1: Create temp table with same schema as target (using sample_df for columns)
            snowflake_columns = []
            for col in sample_df.columns:
                dtype = str(sample_df[col].dtype)
                if "datetime" in dtype or col in ["sys_created_on", "sys_updated_on", "opened_at", "resolved_at", "closed_at"]:
                    col_type = "TIMESTAMP"
                else:
                    col_type = "STRING"
                snowflake_columns.append(f'"{col}" {col_type}')
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                {', '.join(snowflake_columns)}
            )
            """
            cursor.execute(create_temp_sql)
            print(f"Temporary table {temp_table} created")

            # Step 2: COPY INTO temp table with explicit transformations for datetimes
            # Generate dynamic SELECT clause with casts (e.g., TO_TIMESTAMP for datetime cols)
            select_clauses = []
            for col in sample_df.columns:
                quoted_col = f'"{col}"'
                if col in ["sys_created_on", "sys_updated_on", "opened_at", "resolved_at", "closed_at"]:
                    select_clauses.append(f"TO_TIMESTAMP($1:{quoted_col}::STRING) AS {quoted_col}")
                else:
                    select_clauses.append(f"$1:{quoted_col}::STRING AS {quoted_col}")
            select_sql = ', '.join(select_clauses)

            copy_sql = f"""
            COPY INTO {temp_table}
            FROM (SELECT {select_sql} FROM @s3_stage/{relative})
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE;  -- Optional: Remove the file from stage after loading
            """
            cursor.execute(copy_sql)
            print(f"Copied data into temp table from S3 Parquet: {relative}")

            # Diagnostic: Check row count in temp table after copy
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            temp_count = cursor.fetchone()[0]
            print(f"Rows loaded into temp table: {temp_count}")
            if temp_count == 0:
                # Further diagnostic: Query load history for errors
                load_history_sql = f"""
                SELECT TABLE_NAME, FILE_NAME, STATUS, ERROR_COUNT, FIRST_ERROR_MESSAGE, ROW_COUNT
                FROM INFORMATION_SCHEMA.LOAD_HISTORY
                WHERE FILE_NAME LIKE '%{relative}%'
                ORDER BY LAST_LOAD_TIME DESC LIMIT 1
                """
                cursor.execute(load_history_sql)
                load_result = cursor.fetchone()
                if load_result:
                    table_name, file_name, status, error_count, first_error, row_count = load_result
                    inferred_parsed = row_count + error_count if row_count is not None and error_count is not None else 'Unknown'
                    print(f"Load History: Table={table_name}, File={file_name}, Status={status}, Errors={error_count}, First Error Message={first_error}, Rows Loaded={row_count}, Inferred Parsed Rows={inferred_parsed}")
                else:
                    print("No load history found for this file.")
                print("Warning: No rows were loaded into the temp table. Check the load history above for errors (e.g., type mismatches). Consider verifying Parquet schema locally.")

            # Step 3: Generate dynamic MERGE SQL based on all columns in sample_df
            all_columns = [f'"{col}"' for col in sample_df.columns]
            update_sets = ', '.join([f'target.{col} = source.{col}' for col in all_columns if col != '"number"'])  # Update all except PK
            insert_columns = ', '.join(all_columns)
            insert_values = ', '.join([f'source.{col}' for col in all_columns])

            merge_sql = f"""
            MERGE INTO {self.config['snowflake']['database']}.{self.config['snowflake']['schema']}.{self.config['snowflake']['table']} AS target
            USING {temp_table} AS source
            ON target."number" = source."number"
            WHEN MATCHED THEN
                UPDATE SET {update_sets}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
            """
            cursor.execute(merge_sql)
            merge_rows = cursor.rowcount  # Get affected rows directly from cursor
            self.conn.commit()
            print(f"Merged data from temp table into target (affected rows: {merge_rows})")

            # Step 4: Clean up temp table
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            print(f"Temporary table {temp_table} dropped")

            # Verify total rows
            cursor.execute(f"SELECT COUNT(*) FROM {self.config['snowflake']['database']}.{self.config['snowflake']['schema']}.{self.config['snowflake']['table']}")
            count = cursor.fetchone()[0]
            print(f"Total rows in Snowflake table: {count}")
        except Exception as e:
            print(f"Error merging data from S3: {e}")
            self.conn.rollback()
            raise

    def run(self, s3_key, sample_df=None):
        """Run the Snowflake loading process from S3."""
        self.create_table(sample_df)  # Pass sample_df if available for schema
        self.create_s3_stage()
        self.copy_from_s3(s3_key, sample_df)

    def __del__(self):
        """Close connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            print("Snowflake raw connection closed")

if __name__ == "__main__":
    config = {
        "snowflake": {
            "account": "FDAPBEA-MA48001",
            "user": "BASAVARAJSM",
            "database": "poc_db",
            "schema": "poc_schema",
            "table": "incident_test",
            "warehouse": "poc_warehouse"
        },
        "s3": {
            "bucket": "poc-bucket-2102",
            "prefix": "tickets/",
            "region": "us-east-1"
        }
    }
    df = pd.DataFrame({
        "number": ["INC001"],
        "short_description": ["Test"],
        "sys_created_on": [datetime.now()],
        "order": ["123"],
        "parent": ["INC000"]
    })
    loader = SnowflakeLoader(config)
    loader.run("tickets_test.parquet", sample_df=df)  # Test with sample
