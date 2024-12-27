import pyodbc
 
def connect_to_sql_server():
    try:
        # Define the connection string
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=10.20.0.5;'
            'DATABASE=Holistique;'
            'UID=HOL;'
            'PWD=Welcome@11;'
            'PORT=1433;'  # Default port for SQL Server
        )
        print("Connected to SQL Server database successfully!")
 
        # Create a cursor and execute a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION;")
        row = cursor.fetchone()
        print("SQL Server version:", row[0])
 
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
 
    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("SQL Server connection closed.")
 
# Call the function
connect_to_sql_server()