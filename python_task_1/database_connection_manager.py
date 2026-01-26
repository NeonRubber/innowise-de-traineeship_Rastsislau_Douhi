# Импорт PostgreSQL-адаптера для Python
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
import os

load_dotenv()

# Словарь с параметрами для подключения к БД, берутся ранее загруженные переменные из .env
database_cfg = {
    "dbname": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "host": os.getenv("DATABASE_HOST"),
    "port": os.getenv("DATABASE_PORT"),
}

# Контекстный менеджер, содержащий устанавливающую соединение и вносящую изменения в БД функцию establish_connection
@contextmanager
def establish_connection():
    conn = psycopg2.connect(**database_cfg)
    try:
        yield conn
        print("Database connection established.")
        conn.commit()
        print("Record changes are committed!")
    except Exception as exc:
        conn.rollback()
        print("Error:", exc)
        raise
    finally:
        conn.close()