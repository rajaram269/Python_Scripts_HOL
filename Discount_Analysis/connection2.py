import pyodbc
import pandas as pd

def fetch_sales_data():
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

        # Define the query
        query = "SELECT TOP 100* FROM [base] WHERE [PLATFORM] = 'Amazon';"

        # Use Pandas to execute the query and fetch data
        df = pd.read_sql_query(query, connection)

        # Print a preview of the DataFrame
        print(df)
        return df

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("SQL Server connection closed.")

# Call the function
sales_data_df = fetch_sales_data()

# Optional: Save the data to a CSV file
if sales_data_df is not None:
    sales_data_df.to_csv('d:/amazon_sales1.csv', index=False)
    print("Data saved to amazon_sales.csv")
