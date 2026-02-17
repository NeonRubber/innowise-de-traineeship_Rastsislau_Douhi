from database_query_executor import DatabaseQueryExecutor

executor = DatabaseQueryExecutor()
results = executor.select_query_execution("SELECT * FROM rooms LIMIT 5;")
print(results)
