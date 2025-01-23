import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, Integer
import os

# Configuration
CSV_PATH = "D:/test.csv"  # Update with your CSV path
DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '10.20.0.5',
    'database': 'Holistique',
    'uid': 'HOL',
    'pwd': 'Welcome@11',
    'port': '1433'
}

def post_metric_to_db(df: pd.DataFrame, channel: str) -> bool:
    """Process CSV data and insert into SQL Server using SQLAlchemy."""
    try:
        # ======================================================================
        # STEP 1: Data Cleaning
        # ======================================================================
        # Drop unnecessary columns
        df = df.drop(columns=['peak_day'], errors='ignore')

        # Convert numeric columns
        numeric_cols = [
            'total_sales', 'total_sales_value', 'total_sales_days',
            'weeks_of_data', 'total_weeks', 'sales_std', 'avg_weekly_sales',
            'avg_weekly_revenue', 'sale_frequency_in_weeks', 'current_stock',
            'weeks_coverage', 'sales_velocity', 'avg_sales_90day', 'avg_sales_30day',
            'revenue_rank', 'safety_stock', 'refill_level', 'potential_revenue_loss',
            'MRP'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                # Handle Inf values
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                df[col] = df[col].replace([np.inf, -np.inf], 0.0)  # Replace Inf values
        
        # Clean string columns
        string_cols = [
            'store_id', 'sku_id', 'sku_segment', 'performance_bucket',
            'mdq', 'weeks_until_stockout', 'brand_line', 'sku_name',
            'store_name', 'channel'
        ]
        
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # ======================================================================
        # STEP 2: Database Connection
        # ======================================================================
        connection_string = (
            f"mssql+pyodbc://{DB_CONFIG['uid']}:{DB_CONFIG['pwd']}@{DB_CONFIG['server']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?"
            f"driver={DB_CONFIG['driver'].replace(' ', '+')}&Encrypt=yes&TrustServerCertificate=yes&ConnectTimeout=30"
            )

        engine = create_engine(connection_string)
        print('Connected to the database successfully')

        # ======================================================================
        # STEP 3: Table Definition
        # ======================================================================
        metadata = MetaData()
        
        retail_ars = Table('retail_ars', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('store_id', String(50)),
            Column('sku_id', String(50)),
            Column('total_sales', Float),
            Column('total_sales_value', Float),
            Column('total_sales_days', Float),
            Column('weeks_of_data', Float),
            Column('total_weeks', Float),
            Column('sales_std', Float),
            Column('avg_weekly_sales', Float),
            Column('avg_weekly_revenue', Float),
            Column('sale_frequency_in_weeks', Float),
            Column('current_stock', Float),
            Column('weeks_coverage', Float),
            Column('sales_velocity', Float),
            Column('avg_sales_90day', Float),
            Column('avg_sales_30day', Float),
            Column('revenue_rank', Float),
            Column('sku_segment', String(50)),
            Column('performance_bucket', String(50)),
            Column('safety_stock', Float),
            Column('refill_level', Float),
            Column('mdq', String(50)),
            Column('weeks_until_stockout', String(50)),
            Column('potential_revenue_loss', Float),
            Column('brand_line', String(100)),
            Column('sku_name', String(255)),
            Column('MRP', Float),
            Column('store_name', String(100)),
            Column('channel', String(50))
        )

        # ======================================================================
        # STEP 4: Database Operations
        # ======================================================================
        with engine.connect() as conn:
            # Delete existing records for the channel
            delete_stmt = retai_ars.delete().where(retai_ars.c.channel == channel)
            conn.execute(delete_stmt)

            # Insert new data into the database
            df.to_sql(
                name='retai_ars',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi',
                dtype={col.name: col.type for col in retai_ars.columns}
            )
            
            print(f"Successfully inserted {len(df)} rows for channel: {channel}")
            return True

    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    # Read CSV file from D drive
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"Successfully read CSV file: {os.path.basename(CSV_PATH)}")
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Specify channel name (update with your value)
        channel_name = "Nykaa"  # Change this to your channel name
        
        # Process and insert data
        if post_metric_to_db(df, channel_name):
            print("Data pipeline completed successfully")
        else:
            print("Data pipeline failed")
            
    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_PATH}")
    except Exception as e:
        print(f"Critical error: {str(e)}")
