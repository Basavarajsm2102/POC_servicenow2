import pandas as pd

class ParquetHandler:
    def __init__(self):
        pass

    def save_to_parquet(self, df, local_file):
        """Save DataFrame to a Parquet file."""
        try:
            df.to_parquet(local_file, engine="pyarrow", index=False)
            print(f"Saved {len(df)} tickets to Parquet file: {local_file}")
            return df
        except Exception as e:
            print(f"Error saving to Parquet: {e}")
            raise

    def run(self, df, local_file):
        """Run the Parquet saving process."""
        return self.save_to_parquet(df, local_file)

if __name__ == "__main__":
    # Test the class
    df = pd.DataFrame({"ticket_number": ["INC001"], "short_description": ["Test"]})
    handler = ParquetHandler()
    handler.run(df, "test.parquet")