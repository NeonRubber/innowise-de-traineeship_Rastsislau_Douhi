import argparse
import logging
import os
import sys
from dotenv import load_dotenv
from database_json_importer import load_rooms_info, load_students_info
from database_query_executor import DatabaseQueryExecutor
from database_json_xml_exporter import DataExporter
from database_connection_manager import close_db_pool
from utils import load_sql_from_file

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

def optimize_database(executor: DatabaseQueryExecutor):
    logger.info("Applying database optimizations (Indexes)...")
    ddl_statements = [
        "CREATE INDEX IF NOT EXISTS idx_students_room ON students(room);",
        "CREATE INDEX IF NOT EXISTS idx_students_birthday ON students(birthday);",
        "CREATE INDEX IF NOT EXISTS idx_students_sex ON students(sex);"
    ]
    for sql in ddl_statements:
        try:
            executor.update_query_execution(sql)
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Data Engineering Pipeline")
    parser.add_argument("--students", default=os.getenv("STUDENTS_FILE_PATH"))
    parser.add_argument("--rooms", default=os.getenv("ROOMS_FILE_PATH"))
    parser.add_argument("--format", default=os.getenv("EXPORT_DATA_FORMAT", "json"))
    parser.add_argument("--output", default=os.getenv("EXPORT_PATH", "output_data/"))
    parser.add_argument("--queries", default="sql/queries.sql")
    parser.add_argument("--ingest-sql", default="sql/ingestion.sql")
    args = parser.parse_args()

    if not args.students or not args.rooms:
        logger.error("Paths to files must be provided.")
        sys.exit(1)

    query_executor = DatabaseQueryExecutor()

    try:
        # 0. Load SQL Queries for Ingestion
        ingestion_queries = load_sql_from_file(args.ingest_sql)
        if 'insert_room' not in ingestion_queries or 'insert_student' not in ingestion_queries:
             logger.critical("Ingestion SQL file is missing required queries (@insert_room, @insert_student)")
             sys.exit(1)

        # 1. Ingestion
        logger.info("Starting pipeline execution...")
        
        logger.info("--- Step 1: Loading Rooms ---")
        load_rooms_info(args.rooms, ingestion_queries['insert_room'])
        
        logger.info("--- Step 2: Loading Students ---")
        load_students_info(args.students, ingestion_queries['insert_student'])

        # 2. Optimization
        logger.info("--- Step 3: Optimizing Database ---")
        optimize_database(query_executor)

        # 3. Execution & Export
        logger.info("--- Step 4: Analysis ---")
        analysis_queries = load_sql_from_file(args.queries)

        if not analysis_queries:
            logger.warning("No analysis queries found.")
        else:
            for query_name, sql_content in analysis_queries.items():
                logger.info(f"Running query: {query_name}")
                try:
                    query_result = query_executor.select_query_execution(sql_content)
                    data_exporter = DataExporter(query_result)
                    output_file = os.path.join(args.output, f"{query_name}.{args.format}")
                    data_exporter.export(args.format, output_file)
                except Exception as e:
                    logger.error(f"Error processing {query_name}: {e}")

        logger.info("Pipeline finished successfully.")

    except Exception as e:
        logger.critical(f"Critical pipeline failure: {e}")
    finally:
        close_db_pool()
        logger.info("Database connections closed.")

if __name__ == "__main__":
    main()