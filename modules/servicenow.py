import requests
import pandas as pd
from datetime import datetime

class ServiceNowClient:
    def __init__(self, config, password):
        self.config = config
        self.password = password
        self.base_url = config["servicenow"]["url"].format(instance=config["servicenow"]["instance"])
        self.session = requests.Session()
        self.session.auth = (config["servicenow"]["username"], password)
        self.session.headers.update({"Accept": "application/json"})

    def fetch_tickets(self, latest_created_on=None):
        """Fetch tickets from ServiceNow, optionally after latest_created_on."""
        try:
            params = {
                "sysparm_limit": 2000,  # Adjust if you want more; add pagination for large sets
                "sysparm_query": "ORDERBYDESCsys_created_on"
            }

            if latest_created_on:
                timestamp_str = latest_created_on.strftime("%Y-%m-%d %H:%M:%S")
                params["sysparm_query"] += f"^sys_created_on>{timestamp_str}"  # Strict > to avoid duplicates

            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json().get("result", [])
            if not data:
                print("No new tickets found from ServiceNow")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Dynamically preprocess any column containing dictionaries (extract sys_id)
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, dict)).any():
                    print(f"Warning: Found dict in column '{col}', will convert to sys_id string")
                    df[col] = df[col].apply(lambda x: x.get("sys_id", "") if isinstance(x, dict) else (x if isinstance(x, str) else ""))

                # Debug: Check types after conversion
                types = df[col].apply(type).value_counts()
                print(f"Column '{col}' types after preprocessing: {types}")

            # Parse datetime fields
            datetime_cols = ["sys_created_on", "sys_updated_on", "opened_at", "resolved_at", "closed_at"]
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Final check for any remaining dictionaries
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, dict)).any():
                    print(f"Error: Column '{col}' still contains dictionaries after preprocessing")
                    raise ValueError(f"Column '{col}' contains unhandled dictionary values")

            print(f"Retrieved {len(df)} tickets from ServiceNow")
            return df

        except Exception as e:
            print(f"Error fetching tickets from ServiceNow: {e}")
            raise

        finally:
            self.session.close()

    def __del__(self):
        """Ensure session is closed."""
        if hasattr(self, "session"):
            self.session.close()
