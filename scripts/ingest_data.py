import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# Connection to your Docker Postgres on port 5433
DB_URL = "postgresql://admin:admin@localhost:5433/olist_db"
raw_data_path = 'data/raw/'

def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    # Connect to the default postgres database with autocommit
    default_url = "postgresql://admin:admin@localhost:5433/postgres"
    temp_engine = create_engine(default_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    
    try:
        with temp_engine.connect() as connection:
            # Check if database exists
            result = connection.execute(
                text("SELECT 1 FROM pg_database WHERE datname = 'olist_db'")
            )
            if not result.fetchone():
                # Create database if it doesn't exist
                connection.execute(text("CREATE DATABASE olist_db"))
                print("✅ Database 'olist_db' created successfully!")
            else:
                print("ℹ️  Database 'olist_db' already exists.")
    except Exception as e:
        print(f"Note: Could not check/create database: {e}")
    finally:
        temp_engine.dispose()

# Create database and then set up the main engine
create_database_if_not_exists()
engine = create_engine(DB_URL)

def ingest():
    if not os.path.exists(raw_data_path):
        print(f"Error: Folder {raw_data_path} not found!")
        return

    for file in os.listdir(raw_data_path):
        if file.endswith(".csv"):
            table_name = file.replace("_dataset.csv", "").replace(".csv", "")
            print(f"🚀 Importing {file}...")
            
            df = pd.read_csv(os.path.join(raw_data_path, file))
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"✅ Table '{table_name}' created.")

if __name__ == "__main__":
    ingest()