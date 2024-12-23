import pandas as pd
from sqlalchemy import create_engine
import pymysql
from urllib.parse import quote_plus

def list_all_tables(host, user, password, database, port=3306):
    """
    Connect to MySQL database and list all tables
    """
    try:
        # Handle special characters in password
        password = quote_plus(password)
        
        # Create connection string
        conn_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        print(conn_string)
        
        # Create engine
        engine = create_engine(conn_string)
        
        # Query to show all tables
        query = """
        SELECT 
            table_name,
            table_rows,
            create_time,
            update_time
        FROM 
            information_schema.tables
        WHERE 
            table_schema = %s
        ORDER BY 
            table_name;
        """
        
        # Execute query
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection, params=[database])
            
        print(f"\nFound {len(df)} tables in database '{database}':")
        print("\nTable Details:")
        print(df)
        
        return df
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

# Usage
if __name__ == "__main__":
    # Replace with your credentials
    config = {
        'host': '10.20.0.5',
        'user': 'HOL',
        'password': 'Welcome@11',
        'database': 'Holistique',
        'port':3306
    }
    
    # List all tables
    tables_df = list_all_tables(**config)