import mysql.connector
from mysql.connector import Error

def connect_to_mysql():
    try:
        # Establish connection
        connection = mysql.connector.connect(
            host="10.20.0.5",         # MySQL server host
            user="HOL",              # MySQL username
            password="Welcome@11",   # MySQL password
            database="Holistique",   # MySQL database name
            port=1433                # MySQL port
        )

        if connection.is_connected():
            print("Connected to MySQL database successfully!")
            # Fetch server details
            server_info = connection.get_server_info()
            print("MySQL Server version:", server_info)

    except Error as e:
        print(f"Error connecting to the database: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

# Call the function
connect_to_mysql()
