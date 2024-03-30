import mysql.connector
from mysql.connector import pooling
from resources.dev import config
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger


class DatabaseConnectionPool:
    _instance = None
    _pool = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            try:
                cls._instance = super(DatabaseConnectionPool, cls).__new__(cls, *args, **kwargs)
                cls._pool = pooling.MySQLConnectionPool(pool_name="mypool",
                                                        pool_size=10,
                                                        host=config.host,
                                                        user=config.user,
                                                        password=decrypt(config.password),
                                                        database=config.database)
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
        return cls._instance

    @classmethod
    def get_connection(cls):
        # print("connected to mysql")
        return cls._pool.get_connection()

    @classmethod
    def close_connection(cls, conn):
        # print("disconnected from mysql and connection is returned to the pool")
        conn.close()

# # Get a connection from the pool
# conn = DatabaseConnectionPool()
# connection = conn.get_connection()
#
# # Use the connection
# cursor = connection.cursor()
# cursor.execute(f"SELECT * FROM {config.product_staging_table}")
# results = cursor.fetchall()
# print(results)
#
# # Close the connection
# DatabaseConnectionPool.close_connection(connection)


def get_mysql_connection():
    connection = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=decrypt(config.password),
        database=config.database
    )
    return connection
