import pandas as pd
import os
from sqlalchemy import create_engine, text

# ... (keep your existing create_database_if_not_exists code here) ...

# Paths
raw_data_path = 'data/raw/'
parquet_output_path = 'data/parquet/'

# Create parquet folder if it doesn't exist
os.makedirs(parquet_output_path, exist_ok=True)

def ingest_to_postgres_and_parquet():
    engine = create_engine("postgresql://admin:admin@localhost:5433/olist_db")
    
    for file in os.listdir(raw_data_path):
        if file.endswith(".csv"):
            table_name = file.replace("_dataset.csv", "").replace(".csv", "")
            print(f"📦 Processing {table_name}...")
            
            # Read CSV
            df = pd.read_csv(os.path.join(raw_data_path, file))
            
            # 1. Push to Postgres (Bronze Local)
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            # 2. Save as Parquet for Databricks
            df.to_parquet(os.path.join(parquet_output_path, f"{table_name}.parquet"), index=False)
            
    print(f"\n✅ Done! Files saved to {parquet_output_path}")

if __name__ == "__main__":
    # Ensure you have 'pyarrow' installed: pip install pyarrow
    ingest_to_postgres_and_parquet()