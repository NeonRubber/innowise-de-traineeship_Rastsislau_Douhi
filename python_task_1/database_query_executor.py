from database_connection_manager import establish_connection

# Создание класса-исполнителя SQL-запросов
class DatabaseQueryExecutor:

    # Метод-выборщик данных из БД
    def select_query_execution(self, query, params=None):
        with establish_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                column_names = [desc[0] for desc in cursor.description]
                select_results = [dict(zip(column_names, rows)) for rows in cursor.fetchall()]
        return select_results

    # Метод-апдейтер данных в БД
    def update_query_execution(self, query, params=None):
        with establish_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)