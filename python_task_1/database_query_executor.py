from database_connection_manager import establish_connection
from psycopg2.extras import execute_values, RealDictCursor

class DatabaseQueryExecutor:
    # Executes standard and bulk SQL queries

    def select_query_execution(self, query, params=None):
        # Executes SELECT and returns results as dict list using RealDictCursor
        with establish_connection() as connection:
            # RealDictCursor handles dict creation at the driver level for speed
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def update_query_execution(self, query, params=None):
        # Executes a single INSERT/UPDATE/DELETE query
        with establish_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)

    def bulk_insert(self, query, data):
        # Executes bulk INSERT to reduce network round-trips
        with establish_connection() as connection:
            with connection.cursor() as cursor:
                # execute_values expands data into a single INSERT statement
                execute_values(cursor, query, data)