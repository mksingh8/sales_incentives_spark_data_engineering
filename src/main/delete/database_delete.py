import mysql.connector

from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import DatabaseConnectionPool


def delete_mysql_database(connection, database_name):
    try:
        # Establish a connection to the MySQL server
        # conn = DatabaseConnectionPool()
        # connection = conn.get_connection()
        cursor = connection.cursor()

        # SQL query to delete the database
        delete_database_query = f"DROP DATABASE {database_name}"

        # Execute the query
        cursor.execute(delete_database_query)
        connection.commit()

        logger.info(f"Database {database_name} deleted successfully.")

    except mysql.connector.Error as error:
        logger.error(f"Failed to delete database: {error}")

    finally:
        if connection:
            # cursor.close()
            # connection.close()
            DatabaseConnectionPool.close_connection(connection)
            logger.info("MySQL connection is closed.")


