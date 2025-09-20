import os
import pandas as pd
from sqlalchemy import create_engine, text
import re

def load_csvs_to_postgres(data_folder, db_user, db_pass, db_host, db_port, db_name, schema="brimich_logistics", if_exists="replace"):
    """
    Load all CSV files from a folder into a PostgreSQL schema.
    Each CSV becomes a table (table name = file name).
    
    Parameters:
        data_folder (str): Path to folder containing CSV files
        db_user (str): Database username
        db_pass (str): Database password
        db_host (str): Database host
        db_port (str): Database port
        db_name (str): Database name
        schema (str): Schema to load tables into (default: dgp_logistics)
        if_exists (str): "replace", "append", or "fail" (default: replace)
    """
    
    # Create connection engine
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")

    # Ensure schema exists
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    # Iterate through CSV files in folder
    for file in os.listdir(data_folder):
        if file.endswith(".csv"):
            table_name = os.path.splitext(file)[0].lower()
            file_path = os.path.join(data_folder, file)

            print(f"Processing {file} -> table {schema}.{table_name}")

            # Read CSV into pandas DataFrame
            df = pd.read_csv(file_path)

            # Clean column names
            df.columns = [re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", col.strip())).lower()
                for col in df.columns]

            # Load into PostgreSQL
            df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)

            print(f"✅ Loaded {len(df)} rows into {schema}.{table_name}")

    print("🎉 ETL completed successfully!")

if __name__ == "__main__":
    DATA_FOLDER = r'C:\Users\dpack\OneDrive\Desktop\DGPAnalytics\Brimich Logistics\Portfolio Showcase\Data'
    DB_CREDENTIALS = {
        "db_user": "postgres",
        "db_pass": "dGp1998rJs1989!",
        "db_host": "localhost",
        "db_port": "5432",
        "db_name": "postgres"
    }
    load_csvs_to_postgres(DATA_FOLDER, **DB_CREDENTIALS)