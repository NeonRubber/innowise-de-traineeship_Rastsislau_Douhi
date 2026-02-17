from database_connection_manager import establish_connection  # Import the connection manager function

with establish_connection() as connection:
    cur = connection.cursor()
    cur.execute("SELECT version();")
    print("PostgreSQL version:", cur.fetchone())