import pandas as pd
import pyodbc
import numpy as np
import os
from datetime import datetime, timedelta
from scipy import stats
from sklearn.cluster import KMeans
import json


# Database Configuration
DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '10.20.0.5',
    'database': 'Holistique',
    'uid': 'HOL',
    'pwd': 'Welcome@11',
    'port': '1433'
}

#CHANNELS = ['Nykaa FSN', 'Enrich', 'Purplle Retail']
CHANNELS = ['Nykaa FSN']
#OUTPUT_DIR = 'channel_analytics'

def fetch_channel_sales_data(channel):
    """Fetch sales data for a specific channel"""
    try:
        connection = pyodbc.connect(**DB_CONFIG)
        query = f"""
            SELECT 
                [Date] AS date_str,
                [Store code] AS store_id,
                [Mat code] AS sku_id,
                [MRP Sales] AS sales_value,
                [Qty] AS sales_units
            FROM Base
            WHERE Platform = ?
        """
        sales_df = pd.read_sql(query, connection, params=[channel])
        connection.close()
        print(f"Fetched {len(sales_df)} sales records for {channel}")
                # Convert dates in Python with error handling
        
        sales_df['date'] = pd.to_datetime(
            sales_df['date_str'],
            format='%d-%m-%Y',
            errors='coerce',
            dayfirst=True
        )
        
        # Filter valid dates and apply 9-month window
        nine_months_ago = datetime.now() - timedelta(days=270)
        sales_df = sales_df[
            (sales_df['date'].notna()) &
            (sales_df['date'] >= nine_months_ago)
        ].copy()
        return sales_df
    except Exception as e:
        print(f"Error fetching sales data for {channel}: {str(e)}")
        raise

def fetch_channel_inventory_data(channel):
    """Fetch inventory data for a specific channel"""
    try:
        connection = pyodbc.connect(**DB_CONFIG)
        query = """
            SELECT 
                [Store code] AS store_id,
                [Mat code] AS sku_id,
                [Qty] AS current_stock
            FROM [Retail Inventory]
            WHERE Platform = ?
        """
        inventory_df = pd.read_sql(query, connection, params=[channel])
        connection.close()
        print(f"Fetched {len(inventory_df)} inventory records for {channel}")
        return inventory_df
    except Exception as e:
        print(f"Error fetching inventory data for {channel}: {str(e)}")
        raise

def post_metric_to_db(df: pd.DataFrame, channel: str) -> bool:
    """Safely inserts data into SQL Server with strict type alignment."""
    try:
        # ======================================================================
        # STEP 1: Clean and Validate Data
        # ======================================================================
        # Drop columns not in the database schema
        df = df.drop(columns=['peak_day', 'id'], errors='ignore')

        # Define explicit column order (MUST match database schema EXACTLY)
        column_order = [
            'store_id', 'sku_id', 'total_sales', 'total_sales_value', 'total_sales_days',
            'weeks_of_data', 'total_weeks', 'sales_std', 'avg_weekly_sales', 'avg_weekly_revenue',
            'sale_frequency_in_weeks', 'current_stock', 'weeks_coverage', 'sales_velocity',
            'avg_sales_90day', 'avg_sales_30day', 'revenue_rank', 'sku_segment',
            'performance_bucket', 'safety_stock', 'refill_level', 'mdq', 'weeks_until_stockout',
            'potential_revenue_loss', 'brand_line', 'sku_name', 'MRP', 'store_name', 'channel'
        ]

        # Reorder DataFrame columns
        df = df[column_order]

        # Convert columns to match the database schema
        df['store_id'] = df['store_id'].astype(str).str.strip()
        df['sku_id'] = df['sku_id'].astype(str).str.strip()
        df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce').fillna(0.0).round(2)
        df['total_sales_value'] = pd.to_numeric(df['total_sales_value'], errors='coerce').fillna(0.0).round(2)
        df['total_sales_days'] = df['total_sales_days'].astype(float).round(2)
        df['weeks_of_data'] = df['weeks_of_data'].astype(float).round(2)
        df['total_weeks'] = df['total_weeks'].astype(float).round(2)
        df['sales_std'] = pd.to_numeric(df['sales_std'], errors='coerce').fillna(0.0).round(2)
        df['avg_weekly_sales'] = pd.to_numeric(df['avg_weekly_sales'], errors='coerce').fillna(0.0).round(2)
        df['avg_weekly_revenue'] = pd.to_numeric(df['avg_weekly_revenue'], errors='coerce').fillna(0.0).round(2)
        df['sale_frequency_in_weeks'] = pd.to_numeric(df['sale_frequency_in_weeks'], errors='coerce').fillna(0.0).round(2)
        df['current_stock'] = pd.to_numeric(df['current_stock'], errors='coerce').fillna(0.0).round(2)
        df['weeks_coverage'] = pd.to_numeric(df['weeks_coverage'], errors='coerce').fillna(0.0).round(2)
        df['sales_velocity'] = pd.to_numeric(df['sales_velocity'], errors='coerce').fillna(0.0).round(2)
        df['avg_sales_90day'] = pd.to_numeric(df['avg_sales_90day'], errors='coerce').fillna(0.0).round(2)
        df['avg_sales_30day'] = pd.to_numeric(df['avg_sales_30day'], errors='coerce').fillna(0.0).round(2)
        df['revenue_rank'] = pd.to_numeric(df['revenue_rank'], errors='coerce').fillna(0.0).round(2)
        df['sku_segment'] = df['sku_segment'].astype(str).str.strip()
        df['performance_bucket'] = df['performance_bucket'].astype(str).str.strip()
        df['safety_stock'] = pd.to_numeric(df['safety_stock'], errors='coerce').fillna(0.0).round(2)
        df['refill_level'] = pd.to_numeric(df['refill_level'], errors='coerce').fillna(0.0).round(2)
        df['mdq'] = df['mdq'].astype(float).round(2)  # Convert to float
        df['weeks_until_stockout'] = df['weeks_until_stockout'].astype(str).str.strip()
        df['potential_revenue_loss'] = df['potential_revenue_loss'].astype(float).round(2)
        df['brand_line'] = df['brand_line'].astype(str).str.strip()
        df['sku_name'] = df['sku_name'].astype(str).str.strip()
        df['MRP'] = df['MRP'].astype(str).str.strip()  # Ensure this matches the database schema
        df['store_name'] = df['store_name'].astype(str).str.strip()
        df['channel'] = df['channel'].astype(str).str.strip()

        df = df.drop(columns=['mdq'], errors='ignore')

        # ======================================================================
        # STEP 2: Database Connection
        # ======================================================================
        conn_str = (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']};"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )

        with pyodbc.connect(conn_str, autocommit=False) as conn:
            cursor = conn.cursor()
            
            try:
                # Delete existing data
                cursor.execute("DELETE FROM retail_ars_1 WHERE channel = ?", (channel,))

                # Prepare insert query
                #insert_query = """
                #INSERT INTO retail_ars_1 (
                    #store_id, sku_id, total_sales, total_sales_value, total_sales_days,
                    #weeks_of_data, total_weeks, sales_std, avg_weekly_sales, avg_weekly_revenue,
                    #sale_frequency_in_weeks, current_stock, weeks_coverage, sales_velocity,
                    #avg_sales_90day, avg_sales_30day, revenue_rank, sku_segment,
                    #performance_bucket, safety_stock, refill_level, weeks_until_stockout,
                    #potential_revenue_loss, brand_line, sku_name, MRP, store_name, channel
                #) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                #"""
                df = df[['store_id','sku_id','total_sales','total_sales_value', 'total_sales_days', 'weeks_of_data', 'total_weeks', 'sales_std', 'avg_weekly_sales', 'avg_weekly_revenue',
                    'sale_frequency_in_weeks', 'current_stock', 'weeks_coverage', 'sales_velocity',
                    'avg_sales_90day', 'avg_sales_30day', 'revenue_rank', 'sku_segment', 'performance_bucket', 
                    'safety_stock', 'refill_level', 'potential_revenue_loss', 'MRP']]
                insert_query = """
                INSERT INTO retail_ars_1 (
                    store_id, sku_id, total_sales, total_sales_value, total_sales_days,
                    weeks_of_data, total_weeks, sales_std, avg_weekly_sales, avg_weekly_revenue,
                    sale_frequency_in_weeks, current_stock, weeks_coverage, sales_velocity, 
                    avg_sales_90day, avg_sales_30day, revenue_rank, sku_segment, performance_bucket, 
                    safety_stock, refill_level, potential_revenue_loss,  MRP
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """


                # Convert DataFrame to list of tuples
                data = [tuple(row) for row in df.itertuples(index=False, name=None)]

                # Set explicit parameter types based on the database schema
                cursor.setinputsizes([
                    (pyodbc.SQL_VARCHAR, 255),   # store_id
                    (pyodbc.SQL_VARCHAR, 255),   # sku_id
                    (pyodbc.SQL_FLOAT,),        # total_sales
                    (pyodbc.SQL_FLOAT,),        # total_sales_value
                    (pyodbc.SQL_FLOAT,),      # total_sales_days
                    (pyodbc.SQL_FLOAT,),      # weeks_of_data
                    (pyodbc.SQL_FLOAT,),      # total_weeks
                    (pyodbc.SQL_FLOAT,),        # sales_std
                    (pyodbc.SQL_FLOAT,),        # avg_weekly_sales
                    (pyodbc.SQL_FLOAT,),        # avg_weekly_revenue
                    (pyodbc.SQL_FLOAT,),        # sale_frequency_in_weeks
                    (pyodbc.SQL_FLOAT,),        # current_stock
                    (pyodbc.SQL_FLOAT,),        # weeks_coverage
                    (pyodbc.SQL_FLOAT,),        # sales_velocity
                    (pyodbc.SQL_FLOAT,),        # avg_sales_90day
                    (pyodbc.SQL_FLOAT,),        # avg_sales_30day
                    (pyodbc.SQL_FLOAT,),        # revenue_rank
                    (pyodbc.SQL_VARCHAR, 255),   # sku_segment
                    (pyodbc.SQL_VARCHAR, 255),   # performance_bucket
                    (pyodbc.SQL_FLOAT,),        # safety_stock
                    (pyodbc.SQL_FLOAT,),        # refill_level
                    (pyodbc.SQL_FLOAT,),        # mdq
                    (pyodbc.SQL_VARCHAR, 255),        # weeks_until_stockout
                    (pyodbc.SQL_FLOAT,),      # potential_revenue_loss
                    (pyodbc.SQL_VARCHAR, 255),  # brand_line
                    (pyodbc.SQL_VARCHAR, 255),  # sku_name
                    (pyodbc.SQL_VARCHAR, 255),      # MRP
                    (pyodbc.SQL_VARCHAR, 255),  # store_name
                    (pyodbc.SQL_VARCHAR, 255)    # channel
                ])

                # Batch insert
                cursor.fast_executemany = True
                cursor.executemany(insert_query, data)
                conn.commit()
                
                print(f"Successfully inserted {len(data)} rows for {channel}")
                return True

            except pyodbc.Error as e:
                conn.rollback()
                print(f"Database Error: {str(e)}")
                if data:
                    print("Sample Row Data Types:", [type(x).__name__ for x in data[0]])
                    print("Sample Row Data:", data[0])  # Print the first row of data for debugging
                return False

    except Exception as e:
        print(f"Critical Error: {str(e)}")
        return False


def preprocess_data(sales_data, stock_data):
    """
    Ensure consistent data types between sales and stock data
    """
    # Make copies to avoid modifying original data
    sales_df = sales_data.copy()
    stock_df = stock_data.copy()
    
    try:
        # Convert store_id and sku_id to string type in both DataFrames
        sales_df['store_id'] = sales_df['store_id'].astype(str)
        sales_df['sku_id'] = sales_df['sku_id'].astype(str)
        stock_df['store_id'] = stock_df['store_id'].astype(str)
        stock_df['sku_id'] = stock_df['sku_id'].astype(str)
        
        # Ensure numeric types for quantitative columns
        sales_df['sales_units'] = pd.to_numeric(sales_df['sales_units'], errors='coerce')
        sales_df['sales_value'] = pd.to_numeric(sales_df['sales_value'], errors='coerce')
        stock_df['current_stock'] = pd.to_numeric(stock_df['current_stock'], errors='coerce')
        
        # Convert date column with explicit format (DD-MM-YYYY)
        sales_df['date'] = pd.to_datetime(sales_df['date'], format='%d-%m-%Y', dayfirst=True)
        
        # Remove any rows with NaN values after conversion
        sales_df = sales_df.dropna(subset=['sales_units', 'sales_value'])
        stock_df = stock_df.dropna(subset=['current_stock'])
        
        print("Data preprocessing completed successfully")
        return sales_df, stock_df
        
    except Exception as e:
        print(f"Error in data preprocessing: {str(e)}")
        raise

def planogram_mapper(planogram_layout,store_map):
    planogram_layout = pd.read_csv(planogram_layout)
    store_map=pd.read_csv(store_map)
    merged_data = pd.merge(
        store_map,
        planogram_layout,
        on='format',
        how='inner',
        #validate='1:1'
    )
    merged_data['store_id'] = merged_data['store_id'].astype(str)
    merged_data['sku_id'] = merged_data['sku_id'].astype(str)
    merged_data['store_name'] = merged_data['store_name'].astype(str)
    merged_data['format'] = merged_data['format'].astype(str)
    merged_data['brand_line'] = merged_data['brand_line'].astype(str)
    merged_data['sku_name'] = merged_data['sku_name'].astype(str)
    merged_data['channel']=merged_data['channel'].astype(str)
        
    # Ensure numeric types for quantitative columns
    merged_data['mdq'] = pd.to_numeric(merged_data['mdq'], errors='coerce')
    merged_data.to_csv('E:/Nykaa_Analysis/merged_plano.csv',index=False)
    return merged_data

def analyze_store_sku_performance(sales_data, stock_data, plano_data):
    """
    Analyze sales and stock data at store-SKU level
    """
    sales_data['week'] = sales_data['date'].dt.isocalendar().week
    unique_weeks = sales_data['week'].nunique()
    sales_data['total_weeks'] = unique_weeks
    all_weeks = pd.DataFrame({
    'week': sales_data['week'].unique()
    })
    # Fill missing weeks with zero sales
    #sales_data = sales_data.set_index(['store_id', 'sku_id', 'week']).unstack(fill_value=0).stack().reset_index()
    #print(unique_weeks)
    try:
        insights = {}

        # 1. Basic Store-SKU Performance Metrics
        weekly_stats = sales_data.groupby(['store_id', 'sku_id']).agg(
            total_sales = ('sales_units','sum'),
            total_sales_value = ('sales_value','sum'),
            total_sales_days = ('date','nunique'),
            weeks_of_data =('week','nunique'),
            total_weeks = ('total_weeks','first'),
            #non_zero_sales_weeks=('sales_units', lambda x: (x > 0).sum()),
            sales_std=('sales_units', 'std')           # Standard deviation of weekly sales
        ).reset_index()
        
        #total_weeks = sales_data['week'].nunique()
        #print(weekly_stats['sales_std'])
        weekly_stats['avg_weekly_sales']=weekly_stats['total_sales']/weekly_stats['total_weeks']   # Average weekly sales across all weeks
        weekly_stats['avg_weekly_revenue']=weekly_stats['total_sales_value']/weekly_stats['total_weeks']     # Average weekly revenue across all weeks
        
        #print(weekly_std_stats)
        weekly_stats=weekly_stats.round(3)
        
        weekly_stats['sales_std'] = weekly_stats['sales_std'].fillna(0)
        weekly_stats['sale_frequency_in_weeks']= (weekly_stats['weeks_of_data'] / weekly_stats['total_weeks']).round(2)
        #weekly_stats.to_csv('E:/Nykaa_Analysis/filled_sales.csv')
        
        # Merge with current stock
        store_sku_metrics = pd.merge(
            weekly_stats,
            stock_data,
            on=['store_id', 'sku_id'],
            how='left',
            validate='1:1'
        )

        no_sale_inv =  pd.merge(
            stock_data,
            weekly_stats,
            on=['store_id', 'sku_id'],
            how='left',
            #validate='1:1'
        )
        store_master = pd.read_csv('E:/Nykaa_Analysis/store_master.csv')
        
        no_sale_inv=pd.merge(
            no_sale_inv,
            store_master,
            on=['store_id'],
            how = 'left'
        )
        
        no_sale_inv=no_sale_inv[no_sale_inv['total_sales'].isnull()].drop(columns=['total_sales'])
        #no_sale_inv = no_sale_inv[ (no_sale_inv['total_sales'] == 0)]
        no_sale_inv=no_sale_inv.dropna(subset=['store_name'])
        #print(no_sale_inv)
        plano_data_sku = plano_data.drop_duplicates(subset=['store_id','sku_id'])
        no_sale_inv=pd.merge(
            no_sale_inv,
            plano_data[['store_id','sku_id','mdq']],
            on=['store_id','sku_id'],
            how = 'left'
        )
        no_sale_inv=no_sale_inv[['store_id','sku_id','current_stock','store_name','is_new','mdq']]
        #print(no_sale_inv)
        store_sku_metrics['current_stock'] = store_sku_metrics['current_stock'].replace([np.inf, -np.inf, 0,''], np.nan)
        store_sku_metrics['current_stock'] = store_sku_metrics['current_stock'].fillna(0)
        # 2. Calculate Stock Coverage
        store_sku_metrics['weeks_coverage'] = (
            store_sku_metrics['current_stock'] / 
            store_sku_metrics['avg_weekly_sales']
        ).replace([np.inf, -np.inf,''], np.nan).round(2)
        store_sku_metrics['weeks_coverage'] = store_sku_metrics['weeks_coverage'].fillna(0)
        # 3. Sales Velocity Calculation
        #recent_sales = sales_data.sort_values('date').groupby(['store_id', 'sku_id']).tail(30)
        last_date = sales_data['date'].max()
        #print(last_date - timedelta(days=90))
        # Filter for sales data in the last 30 days
        recent_sales = sales_data[sales_data['date'] > last_date - timedelta(days=90)]
        recent_sales_30 = sales_data[sales_data['date'] > last_date - timedelta(days=30)]
        print(recent_sales)
        sales_velocity = recent_sales.groupby(['store_id', 'sku_id']).agg(
            total_last_90_days_sales=('sales_units', 'sum'),
            total_last_90_days_sales_value=('sales_value','sum')
            ).reset_index()
        recent_sales_30 = recent_sales_30.groupby(['store_id', 'sku_id']).agg(
            #total_last_90_days_sales=('sales_units', 'sum'),
            total_last_30_days_sales_value=('sales_value','sum')
            ).reset_index()
        # Add sales velocity column weekly
        sales_velocity['sales_velocity'] = (sales_velocity['total_last_90_days_sales'] / 12.85).round(2)
        sales_velocity['recent_avg_sales'] = (sales_velocity['total_last_90_days_sales_value'] / 12.85).round(2)
        sales_velocity['sales_velocity'] = sales_velocity['sales_velocity'].replace([np.inf, -np.inf, 0,''], np.nan)
        sales_velocity['sales_velocity'] = sales_velocity['sales_velocity'].fillna(0)
        sales_velocity['avg_sales_90day'] = sales_velocity['recent_avg_sales'].replace([np.inf, -np.inf, 0,''], np.nan)
        sales_velocity['avg_sales_90day'] = sales_velocity['avg_sales_90day'].fillna(0)
        recent_sales_30['avg_sales_30day']=(recent_sales_30['total_last_30_days_sales_value']/4.2).round(2)
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            sales_velocity[['store_id', 'sku_id', 'sales_velocity','avg_sales_90day']],
            on=['store_id', 'sku_id'],
            how='left'
        )
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            recent_sales_30[['store_id', 'sku_id', 'avg_sales_30day']],
            on=['store_id', 'sku_id'],
            how='left'
        )
        store_sku_metrics['sales_velocity'] = store_sku_metrics['sales_velocity'].replace([np.inf, -np.inf, 0,'',' '], np.nan)
        store_sku_metrics['sales_velocity'] = store_sku_metrics['sales_velocity'].fillna(0)
        store_sku_metrics['avg_sales_90day'] = store_sku_metrics['avg_sales_90day'].replace([np.inf, -np.inf, 0,'',' '], np.nan)
        store_sku_metrics['avg_sales_90day'] = store_sku_metrics['avg_sales_90day'].fillna(0)
        store_sku_metrics['avg_sales_30day'] = store_sku_metrics['avg_sales_30day'].replace([np.inf, -np.inf, 0,'',' '], np.nan)
        store_sku_metrics['avg_sales_30day'] = store_sku_metrics['avg_sales_30day'].fillna(0)
        # 4. SKU Segmentation
        store_sku_metrics['revenue_rank'] = store_sku_metrics.groupby('store_id')['avg_weekly_revenue'].rank(ascending=False).round()
        def assign_sku_segment(row):
            if row['revenue_rank'] < 10:
                return 'A - High Value'
            elif row['sale_frequency_in_weeks'] > 0.99:
                return 'B - Regular'
            elif row['sale_frequency_in_weeks'] > 0.7:
                return 'C - Moderate'
            else:
                return 'D - Slow Moving'
        
        store_sku_metrics['sku_segment'] = store_sku_metrics.apply(assign_sku_segment, axis=1)
        
        # Calculate total revenue per store
        store_revenue = store_sku_metrics.groupby('store_id')['avg_weekly_revenue'].sum().reset_index()
        store_revenue.columns = ['store_id', 'total_revenue']

        # Calculate quantiles for bucket thresholds
        top_20_threshold = store_revenue['total_revenue'].quantile(0.8)
        average_threshold = store_revenue['total_revenue'].quantile(0.5)

        # Assign performance buckets
        def assign_bucket(revenue):
            if revenue >= top_20_threshold:
                return 'Star_Store'
            elif revenue >= average_threshold:
                return 'Average_Store'
            else:
                return 'Laggard_Store'

        store_revenue['performance_bucket'] = store_revenue['total_revenue'].apply(assign_bucket)

        # Merge the performance bucket back to store_sku_metrics
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            store_revenue[['store_id', 'performance_bucket']],
            on='store_id',
            how='left'
            )

        #store_sku_metrics.to_csv('E:/test123.csv',index=False)
        
        # 5. Safety Stock and Reorder Points
        lead_time_weeks = 3
        service_level_z = {
            'A - High Value': 2.326,
            'B - Regular': 1.96,
            'C - Moderate': 1.645,
            'D - Slow Moving': 1.28
        }
        
        store_sku_metrics['safety_stock'] = store_sku_metrics.apply(
            lambda row: service_level_z[row['sku_segment']] * row['sales_std'] * np.sqrt(lead_time_weeks), 
            axis=1
        ).round(2)
        print(store_sku_metrics['safety_stock'])
        
        store_sku_metrics['refill_level'] = (
            store_sku_metrics['avg_weekly_sales'] * 8 + 
            store_sku_metrics['safety_stock']
        ).round()
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            plano_data[['store_id','sku_id','mdq']],
            on=['store_id','sku_id'],
            how='left'
        )
        # Adjust refill_level to be at least equal to mdq
        store_sku_metrics['refill_level'] = store_sku_metrics[['refill_level', 'mdq']].max(axis=1)
        # 6. Lost Sales Risk Analysis
        store_sku_metrics['weeks_until_stockout'] = np.where(
            store_sku_metrics['sales_velocity'] > 0,
            store_sku_metrics['current_stock'] / (store_sku_metrics['sales_velocity'] ),
            float('inf')
        ).round(1)
        
        store_sku_metrics['potential_revenue_loss'] = np.where(
            store_sku_metrics['weeks_coverage'] < lead_time_weeks,
            store_sku_metrics['avg_weekly_revenue'] * (lead_time_weeks - store_sku_metrics['weeks_coverage']),
            0
        ).astype(int)
        
        # 7. Day of Week Patterns
        sales_data['day_of_week'] = pd.to_datetime(sales_data['date']).dt.day_name()
        dow_patterns = sales_data.groupby(['store_id', 'sku_id', 'day_of_week'])['sales_units'].mean().unstack()
        peak_days = dow_patterns.idxmax(axis=1).reset_index(name='peak_day')
        
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            peak_days,
            on=['store_id', 'sku_id'],
            how='left'
        )
        plano_data_sku = plano_data.drop_duplicates(subset=['sku_id'])
        sku_master=pd.read_csv('E:/Nykaa_Analysis/sku_master.csv')
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            sku_master[['sku_id','brand_line','sku_name','MRP']],
            on=['sku_id'],
            how='left'
        )
        plano_data_store = plano_data.drop_duplicates(subset=['store_id'])
        store_sku_metrics = pd.merge(
            store_sku_metrics,
            plano_data_store[['store_id','store_name','channel']],
            on=['store_id'],
            how='left'
        )

        no_sale_inv = pd.merge(
            no_sale_inv,
            sku_master[['sku_id','brand_line','sku_name','MRP']],
            on=['sku_id'],
            how='left'
        )
        no_sale_inv.to_csv('E:/Nykaa_Analysis/no_sale_inv.csv',index=False)
        #print(store_sku_metrics)
        store_metrics_df = pd.DataFrame(store_sku_metrics)
        store_metrics_df.to_csv('E:/Nykaa_Analysis/store_metric.csv',index=False)
        channel_name = store_sku_metrics['channel'].iloc[0]
        post_metric_to_db(store_sku_metrics,channel_name)
        excel_output = 'E:/Nykaa_Analysis/retail_ars.xlsx'
        # Write dataframes to Excel file with separate sheets
        with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
            store_metrics_df.to_excel(writer, sheet_name='store_analysis', index=False)
            no_sale_inv.to_excel(writer, sheet_name='no_sales', index=False)
        # Store results
        insights['store_sku_metrics'] = store_sku_metrics
        #insights['dow_patterns'] = dow_patterns
        
        # Critical SKUs requiring attention
        insights['critical_skus'] = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < lead_time_weeks) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular', 'C - Moderate', 'D - Slow Moving'])) &
            (store_sku_metrics['sales_velocity'] > 0)
        ].sort_values(['sku_segment', 'potential_revenue_loss'], ascending=[True, False])
        #insights=pd.DataFrame(insights)
        insights['critical_skus'].to_csv('E:/Nykaa_Analysis/insights2.csv',index=False)
        
        #print(insights)
        return insights
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        raise

def generate_sku_recommendations(insights):
    """
    Generate detailed recommendations at store-SKU level
    """
    lead_time = 3 #weeks
    try:
        recommendations = []
        store_sku_metrics = insights['store_sku_metrics']
        
        # 1. Immediate Replenishment Needs
        critical_skus = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < lead_time) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular']))
        ]
        
        for _, item in critical_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'store_name':item['store_name'],
                'sku_id': item['sku_id'],
                'brand_line':item['brand_line'],
                'sku_name':item['sku_name'],
                'mdq':item['mdq'],
                'avg_sale_unit_weekly':item['avg_weekly_sales'],
                'avg_mrp_sales_weekly':item['avg_weekly_revenue'],
                'current_stock':item['current_stock'],
                #'safety_stock':item['safety_stock'],
                'store_type':item['performance_bucket'],
                'priority': 'CRITICAL',
                'category': 'Stock Alert',
                'inventory_weeks':item['weeks_until_stockout'],
                'refill_level':item['refill_level'],
                'reorder_qty':item['refill_level']-item['current_stock'],
                'potential_revenue_loss':item['potential_revenue_loss'],
                'action': f"SKU {item['sku_id']} ({item['sku_segment']}) will stockout in {item['weeks_until_stockout']:.1f} weeks. "
                         f"Order {max(item['refill_level'] - item['current_stock'], 0):.0f} units. "
                         f"Potential weekly revenue loss: INR {item['potential_revenue_loss']:.2f}"
            })
        # 1b. Non-Critical Replenishment Needs
        critical_skus = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < lead_time) & 
            (store_sku_metrics['sku_segment'].isin(['C - Moderate', 'D - Slow Moving']))
        ]
        
        for _, item in critical_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'store_name':item['store_name'],
                'sku_id': item['sku_id'],
                'brand_line':item['brand_line'],
                'sku_name':item['sku_name'],
                'mdq':item['mdq'],
                'avg_sale_unit_weekly':item['avg_weekly_sales'],
                'avg_mrp_sales_weekly':item['avg_weekly_revenue'],
                'current_stock':item['current_stock'],
                #'safety_stock':item['safety_stock'],
                'store_type':item['performance_bucket'],
                'priority': 'Medium',
                'category': 'Stock Alert',
                'inventory_weeks':item['weeks_until_stockout'],
                'refill_level':item['refill_level'],
                'reorder_qty':item['refill_level']-item['current_stock'],
                'potential_revenue_loss':item['potential_revenue_loss'],
                'action': f"SKU {item['sku_id']} ({item['sku_segment']}) will stockout in {item['weeks_until_stockout']:.1f} weeks. "
                         f"Order {max(item['refill_level'] - item['current_stock'], 0):.0f} units. "
                         f"Potential weekly revenue loss: INR {item['potential_revenue_loss']:.2f}"
            })
        
        # 2. Overstock Situations
        overstock_threshold = lead_time+1
        overstock_skus = store_sku_metrics[
            (store_sku_metrics['weeks_coverage'] > overstock_threshold) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular']))
        ]
        
        for _, item in overstock_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'store_name':item['store_name'],
                'sku_id': item['sku_id'],
                'brand_line':item['brand_line'],
                'sku_name':item['sku_name'],
                'mdq':item['mdq'],
                'avg_sale_unit_weekly':item['avg_weekly_sales'],
                'avg_mrp_sales_weekly':item['avg_weekly_revenue'],
                'current_stock':item['current_stock'],
                #'safety_stock':item['safety_stock'],
                'store_type':item['performance_bucket'],
                'priority': 'MEDIUM',
                'category': 'Inventory Optimization',
                'inventory_weeks':item['weeks_until_stockout'],
                'refill_level':item['refill_level'],
                'reorder_qty':item['current_stock'] - (item['refill_level']),
                'potential_revenue_loss':item['potential_revenue_loss'],
                'action': f"Excess inventory of SKU {item['sku_id']}. Consider redistributing "
                         f"{(item['current_stock'] - (item['refill_level'])):.0f} units. "
                         f"Current coverage: {item['weeks_coverage']:.1f} weeks"
            })

        # 2b. Overstock Situations
        overstock_threshold = lead_time+1
        overstock_skus = store_sku_metrics[
            (store_sku_metrics['weeks_coverage'] > overstock_threshold) & 
            (store_sku_metrics['sku_segment'].isin(['C - Moderate', 'D - Slow Moving']))
        ]
        
        for _, item in overstock_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'store_name':item['store_name'],
                'sku_id': item['sku_id'],
                'brand_line':item['brand_line'],
                'sku_name':item['sku_name'],
                'mdq':item['mdq'],
                'avg_sale_unit_weekly':item['avg_weekly_sales'],
                'avg_mrp_sales_weekly':item['avg_weekly_revenue'],
                'current_stock':item['current_stock'],
                #'safety_stock':item['safety_stock'],
                'store_type':item['performance_bucket'],
                'priority': 'Low',
                'category': 'Inventory Optimization',
                'inventory_weeks':item['weeks_until_stockout'],
                'refill_level':item['refill_level'],
                'reorder_qty':item['current_stock'] - (item['refill_level']),
                'potential_revenue_loss':item['potential_revenue_loss'],
                'action': f"Excess inventory of SKU {item['sku_id']}. Consider redistributing "
                         f"{(item['current_stock'] - (item['refill_level'])):.0f} units. "
                         f"Current coverage: {item['weeks_coverage']:.1f} weeks"
            })

        pd.DataFrame(recommendations).to_csv('E:/Nykaa_Analysis/sku_recommenations.csv',index=False)
        return pd.DataFrame(recommendations)
        
    except Exception as e:
        print(f"Error generating recommendations: {str(e)}")
        raise

def generate_summary_report(insights):
    """
    Generate a comprehensive summary report
    """
    try:
        metrics = insights['store_sku_metrics']
        
        summary = {
            'total_skus': metrics['sku_id'].nunique(),
            'total_stores': metrics['store_id'].nunique(),
            'total_store_sku_combinations': len(metrics),
            #'sku_segments': metrics['sku_segment'].value_counts().to_dict(),
            'total_inventory_value': (
                metrics['current_stock'] * metrics['avg_weekly_revenue'] / 
                metrics['avg_weekly_sales']
            ).sum().round(),
            #'total_weekly_revenue_at_risk': metrics['potential_revenue_loss'].sum()
        }

        with open('E:/Nykaa_Analysis/analysis_summary.txt', 'w') as f:
            f.write(json.dumps(summary))
        print (summary)
        return summary
        
    except Exception as e:
        print(f"Error generating summary report: {str(e)}")
        raise



def process_channel(channel, planogram_layout, store_map):
    """Process data for a single channel"""
    print(f"\n{'='*40}\nProcessing {channel}\n{'='*40}")
    
    try:
        
        # Fetch data
        sales_data = fetch_channel_sales_data(channel)
        stock_data = fetch_channel_inventory_data(channel)
        stock_data = stock_data.groupby(['store_id','sku_id']).agg({
            'current_stock' : ['sum']
            }).round(2)        
        stock_data.columns=['current_stock']
        stock_data=stock_data.reset_index()
        # Load planogram data
        plano_data = planogram_mapper(planogram_layout, store_map)
        
        # Preprocess and analyze
        sales_df, stock_df = preprocess_data(sales_data, stock_data)
        insights = analyze_store_sku_performance(sales_df,stock_df,plano_data)
        recommendations = generate_sku_recommendations(insights)
        summary = generate_summary_report(insights)     
        # Save channel-specific results 
        print(f"Completed processing for {channel}")
        return True
        
    except Exception as e:
        print(f"Failed processing {channel}: {str(e)}")
        return False

def main():
    """Main function to process all channels"""
    # Configuration for local files (example paths)
    PLANO_CONFIG = {
        'Nykaa FSN': {
            'planogram': 'E:/Nykaa_Analysis/panogram_layout.csv',
            'store_map': 'E:/Nykaa_Analysis/store_shelf_ref.csv'
        },
        'Enrich': {
            'planogram': 'E:/Nykaa_Analysis/enrich_planogram.csv',
            'store_map': 'E:/Nykaa_Analysis/enrich_store_map.csv'
        },
        'Purplle Retail': {
            'planogram': 'E:/Nykaa_Analysis/purplle_planogram.csv',
            'store_map': 'E:/Nykaa_Analysis/purplle_store_map.csv'
        }
    }
    
    # Create output directory
    #os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Process each channel
    results = {}
    for channel in CHANNELS:
        success = process_channel(
            channel=channel,
            planogram_layout=PLANO_CONFIG[channel]['planogram'],
            store_map=PLANO_CONFIG[channel]['store_map']
        )
        results[channel] = 'Success' if success else 'Failed'
    
    # Generate final report
    
    print("\nProcessing Complete!")
    print("Results:", json.dumps(results, indent=2))

if __name__ == "__main__":
    main()