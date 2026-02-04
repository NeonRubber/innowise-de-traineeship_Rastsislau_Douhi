import os
import psycopg2
import sys

def run_etl(file_path):
    conn = None

    db_host = os.getenv('DB_HOST', 'localhost')
    
    conn_params = {
        "host": db_host,
        "database": "superstore_bi",
        "user": "postgres",
        "password": "postgres_superstore_bi",
        "port": "5432"
    }

    try:
        # DB connection
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()

        print(f"Starting ETL for {file_path}...")

        # 1. Load Stage (Raw data)
        print("Executing sp_load_stage...")
        # File is passed as is for SQL execution
        cur.execute("CALL layer_stage.sp_load_stage(%s)", (file_path,))

        # 2. Load Dimensions
        print("Executing sp_load_core_dimensions...")
        cur.execute("CALL layer_core.sp_load_core_dimensions()")

        # 3. Load Facts
        print("Executing sp_load_core_facts...")
        cur.execute("CALL layer_core.sp_load_core_facts()")

        # 4. Refresh Mart
        print("Executing sp_refresh_mart...")
        cur.execute("CALL layer_mart.sp_refresh_mart()")

        print("ETL Pipeline completed successfully!")

    except Exception as e:
        print(f"Error during ETL: {e}")
        # Avoid sys.exit(1) here to allow finally block to run
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python orchestrator.py <container_internal_file_path>")
        print("Example: python etl/orchestrator.py /import_data/initial_load.csv")
        sys.exit(1)
    
    run_etl(sys.argv[1])