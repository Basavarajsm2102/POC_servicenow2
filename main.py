import yaml
import os
from dotenv import load_dotenv
from modules.servicenow import ServiceNowClient
from modules.snowflake import SnowflakeLoader
import pandas as pd
import boto3
from datetime import datetime

def load_config():
    """Load configuration from config.yaml."""
    with open("config/config.yaml", "r") as file:
        return yaml.safe_load(file)

def run_servicenow(config, password):
    """Fetch tickets from ServiceNow."""
    client = ServiceNowClient(config, password)
    loader = SnowflakeLoader(config)
    latest_created_on = loader.get_latest_created_on()
    latest_updated_on = loader.get_latest_updated_on()
    df = client.fetch_tickets(latest_created_on, latest_updated_on)
    return df

def main():
    """Main function to orchestrate the data pipeline."""
    load_dotenv()
    config = load_config()
    password = os.getenv("SERVICENOW_PASSWORD")
    if not password:
        raise ValueError("SERVICENOW_PASSWORD not found in .env file")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=config["s3"]["region"]
    )

    df = run_servicenow(config, password)
    if not df.empty:
        print(f"Processed {len(df)} tickets into DataFrame with {len(df.columns)} columns")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file = f"tickets_{timestamp}.parquet"
        df.to_parquet(parquet_file)
        print(f"Saved {len(df)} tickets to Parquet file: {parquet_file}")

        s3_key = f"{config['s3']['prefix']}{parquet_file}"
        s3_client.upload_file(parquet_file, config["s3"]["bucket"], s3_key)
        print(f"Uploaded Parquet file to s3://{config['s3']['bucket']}/{s3_key}")

        # Load from S3 to Snowflake (pass s3_key and optional sample_df for schema)
        loader = SnowflakeLoader(config)
        loader.run(s3_key, sample_df=df.head(1))  # Use head(1) as sample for table creation

        os.remove(parquet_file)
        print(f"Cleaned up local file: {parquet_file}")
    else:
        print("No new tickets to process")

if __name__ == "__main__":
    main()
