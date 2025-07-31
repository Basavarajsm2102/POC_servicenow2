import requests
import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
import boto3
import os
from datetime import datetime

# ServiceNow Configuration
SNOW_INSTANCE = "dev293786"
SNOW_URL = f"https://{SNOW_INSTANCE}.service-now.com/api/now/table/incident"
SNOW_USER = "admin"
SNOW_PASS = "Bk*tZW+Ydp82"

# Snowflake Configuration
SF_ACCOUNT = "FDAPBEA-MA48001"
SF_USER = "BASAVARAJSM"
SF_PASSWORD = "2102@Basavaraj" # Replace with your Snowflake password
SF_DATABASE = "poc_db"
SF_SCHEMA = "poc_schema"
SF_WAREHOUSE = "poc_warehouse"
SF_TABLE = "incident"

# AWS S3 Configuration
S3_BUCKET = "poc-bucket-2102"  # Updated with your provided bucket
S3_PREFIX = "tickets/"  # Folder path in S3
S3_REGION = "us-east-1"  # Replace with your AWS region if different

def get_servicenow_tickets():
    """Extract tickets from ServiceNow using Table API with basic authentication."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    auth = (SNOW_USER, SNOW_PASS)
    params = {
        "sysparm_limit": 10,
        "sysparm_fields": "number,short_description,priority,category,sys_created_on,state,caller_id",
        "sysparm_query": "ORDERBYsys_created_on"
    }
    
    try:
        response = requests.get(SNOW_URL, auth=auth, headers=headers, params=params)
        response.raise_for_status()
        tickets = response.json().get("result", [])
        print(f"Retrieved {len(tickets)} tickets from ServiceNow")
        return tickets
    except requests.RequestException as e:
        print(f"Error fetching ServiceNow data: {e}")
        if response:
            print(f"Response details: {response.text}")
        return []

def process_tickets(tickets):
    """Process ticket data into a structured format."""
    processed_data = []
    for ticket in tickets:
        caller_id = ticket.get("caller_id", {}).get("value", "") if isinstance(ticket.get("caller_id"), dict) else ticket.get("caller_id", "")
        processed_data.append({
            "ticket_number": ticket.get("number", ""),
            "short_description": ticket.get("short_description", ""),
            "priority": ticket.get("priority", ""),
            "category": ticket.get("category", ""),
            "created_on": ticket.get("sys_created_on", ""),
            "state": ticket.get("state", ""),
            "caller_id": caller_id
        })
    return processed_data

def save_to_parquet(data, local_file):
    """Save ticket data to a Parquet file."""
    try:
        df = pd.DataFrame(data)
        df.to_parquet(local_file, engine="pyarrow", index=False)
        print(f"Saved {len(data)} tickets to Parquet file: {local_file}")
        return df
    except Exception as e:
        print(f"Error saving to Parquet: {e}")
        raise

def upload_to_s3(local_file, s3_key):
    """Upload Parquet file to S3."""
    try:
        s3_client = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        s3_client.upload_file(local_file, S3_BUCKET, s3_key)
        print(f"Uploaded Parquet file to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise

def load_to_snowflake(df):
    """Load ticket data from local Parquet file into Snowflake using SQLAlchemy."""
    try:
        # Create a raw Snowflake connection for DDL and warehouse selection
        conn_raw = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        print("Snowflake raw connection established")
        
        cursor = conn_raw.cursor()
        
        # Check if warehouse exists
        cursor.execute(f"SHOW WAREHOUSES LIKE '{SF_WAREHOUSE}'")
        if not cursor.fetchone():
            raise Exception(f"Warehouse {SF_WAREHOUSE} does not exist. Please create it or check permissions.")
        
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
            ticket_number STRING,
            short_description STRING,
            priority STRING,
            category STRING,
            created_on TIMESTAMP,
            state STRING,
            caller_id STRING
        )
        """
        cursor.execute(create_table_sql)
        print(f"Table {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} created or verified")
        
        # Activate warehouse
        cursor.execute(f"USE WAREHOUSE {SF_WAREHOUSE}")
        print(f"Warehouse {SF_WAREHOUSE} activated")
        conn_raw.close()
        
        # Create SQLAlchemy engine for data loading
        engine = create_engine(
            URL(
                account=SF_ACCOUNT,
                user=SF_USER,
                password=SF_PASSWORD,
                database=SF_DATABASE,
                schema=SF_SCHEMA,
                warehouse=SF_WAREHOUSE
            )
        )
        print("Snowflake SQLAlchemy connection established")
        
        # Write data to Snowflake
        df.to_sql(
            name=SF_TABLE.lower(),
            con=engine,
            schema=SF_SCHEMA,
            index=False,
            if_exists="append",
            method="multi"
        )
        print(f"Loaded {len(df)} tickets into Snowflake table {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
        
        # Verify data
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}"))
            count = result.fetchone()[0]
            print(f"Total rows in Snowflake table: {count}")
        
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake connection error: {e}")
        print(f"Account: {SF_ACCOUNT}, User: {SF_USER}, Database: {SF_DATABASE}, Schema: {SF_SCHEMA}, Warehouse: {SF_WAREHOUSE}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Account: {SF_ACCOUNT}, User: {SF_USER}, Database: {SF_DATABASE}, Schema: {SF_SCHEMA}, Warehouse: {SF_WAREHOUSE}")
        raise
    finally:
        if 'conn_raw' in locals():
            conn_raw.close()

def main():
    # Step 1: Extract tickets from ServiceNow
    tickets = get_servicenow_tickets()
    
    # Step 2: Process tickets
    processed_data = process_tickets(tickets)
    
    # Step 3: Save to Parquet, upload to S3, and load to Snowflake
    if processed_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_file = f"tickets_{timestamp}.parquet"
        s3_key = f"{S3_PREFIX}tickets_{timestamp}.parquet"
        
        # Save to Parquet
        df = save_to_parquet(processed_data, local_file)
        
        # Upload to S3
        upload_to_s3(local_file, s3_key)
        
        # Load to Snowflake from local Parquet data
        load_to_snowflake(df)
        
        # Clean up local file
        os.remove(local_file)
        print(f"Cleaned up local file: {local_file}")
    else:
        print("No data to process")

if __name__ == "__main__":
    main()