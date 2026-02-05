import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseConfig:
    # DB connection configuration
    DB_NAME = os.getenv("DATABASE_NAME")
    USER = os.getenv("DATABASE_USER")
    PASSWORD = os.getenv("DATABASE_PASSWORD")
    HOST = os.getenv("DATABASE_HOST")
    PORT = os.getenv("DATABASE_PORT")
    MIN_CONN = int(os.getenv("DB_MIN_CONN", 1))
    MAX_CONN = int(os.getenv("DB_MAX_CONN", 10))

class PostgresConnectionPool:
    _pool = None

    @classmethod
    def get_pool(cls):
        # Thread-safe lazy initialization of the connection pool."""
        if cls._pool is None:
            try:
                cls._pool = pool.ThreadedConnectionPool(
                    minconn=DatabaseConfig.MIN_CONN,
                    maxconn=DatabaseConfig.MAX_CONN,
                    dbname=DatabaseConfig.DB_NAME,
                    user=DatabaseConfig.USER,
                    password=DatabaseConfig.PASSWORD,
                    host=DatabaseConfig.HOST,
                    port=DatabaseConfig.PORT
                )
                logger.info("Connection pool created.")
            except Exception as e:
                logger.error(f"Error creating connection pool: {e}")
                raise
        return cls._pool

    @classmethod
    def close_pool(cls):
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logger.info("Connection pool closed.")

# Compatibility wrapper for legacy code
@contextmanager
def establish_connection():
    # Context manager for DB transactions
    connection_pool = PostgresConnectionPool.get_pool()
    conn = connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error(f"Database transaction error: {exc}")
        raise
    finally:
        connection_pool.putconn(conn)

def close_db_pool():
    # Closes the global connection pool
    PostgresConnectionPool.close_pool()
