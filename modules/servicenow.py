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

    def fetch_tickets(self, latest_created_on=None, latest_updated_on=None):
        """Fetch tickets from ServiceNow, including new and updated ones, with pagination."""
        try:
            all_data = []
            offset = 0
            limit = 1000  # Batch size for pagination; adjust as needed
            while True:
                params = {
                    "sysparm_query": "ORDERBYDESCsys_updated_on",  # Order by update time
                    "sysparm_limit": limit,
                    "sysparm_offset": offset
                }

                query_parts = []
                if latest_created_on:
                    timestamp_str = latest_created_on.strftime("%Y-%m-%d %H:%M:%S")
                    query_parts.append(f"sys_created_on>{timestamp_str}")  # New tickets
                if latest_updated_on:
                    timestamp_str = latest_updated_on.strftime("%Y-%m-%d %H:%M:%S")
                    query_parts.append(f"sys_updated_on>{timestamp_str}")  # Updated tickets

                if query_parts:
                    params["sysparm_query"] += "^" + "^OR".join(query_parts)  # Combine with OR

                response = self.session.get(self.base_url, params=params)
                response.raise_for_status()
                batch_data = response.json().get("result", [])
                if not batch_data:
                    break  # No more records

                all_data.extend(batch_data)
                offset += limit
                print(f"Fetched batch of {len(batch_data)} tickets (total so far: {len(all_data)})")

            if not all_data:
                print("No new or updated tickets found from ServiceNow")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(all_data)

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

            print(f"Retrieved {len(df)} tickets from ServiceNow (new/updated)")
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
