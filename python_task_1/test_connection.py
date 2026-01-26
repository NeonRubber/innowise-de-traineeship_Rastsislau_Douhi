from database_connection_manager import establish_connection  # Импорт созданной функции для соединения

with establish_connection() as connection:
    cur = connection.cursor()
    cur.execute("SELECT version();")
    print("PostgreSQL version:", cur.fetchone())