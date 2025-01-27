import pandas as pd
from datetime import datetime
import pyodbc


DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': '10.20.0.5',
    'database': 'Holistique',
    'uid': 'HOL',
    'pwd': 'Welcome@11',
    'port': '1433'
}

# Load data with proper escaping
file_path = r"D:/merged_reports.csv"  # Raw string for Windows paths

df = pd.read_csv(file_path)
df = df[~df['Site Location '].str.contains(
    'Amazon|Myntra|Flipkart|BOM T2|Head Office', 
    case=False, 
    na=False  # Treat NaN/blank values as non-matching
)]
# -------------------------------------------
# Step 4: Drop rows where Available Qty = 0
# -------------------------------------------
df = df[df['Available Qty'] != 0] 

try:
        connection = pyodbc.connect(**DB_CONFIG)
        query = f"""
            SELECT 
                [Mat Code] AS SKU,
                [SKU Name] AS SKU_Name,
                [Brand] AS Brand1,
                [L3 Category] AS Hirearchy
            FROM [SKU Master]
        """
        sku_df = pd.read_sql(query, connection)
        print(sku_df)
        connection.close()

except Exception as e:
        print(f"Error fetching sales master data: {str(e)}")
        raise

df = pd.merge(
        df,
        sku_df,
        on='SKU',
        how='inner',
        #validate='1:1'
    )
# If Zone = "ige 2" and Inv Bucket = "in process", set Inv Bucket to "good"
mask = (df['Zone'] == 'IGE-2') & (df['Inv Bucket'] == 'In Process')
df.loc[mask, 'Inv Bucket'] = 'good'

# Clean dates
df['ExpiryDate'] = df['ExpiryDate'].str.strip()
df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'], format='%d/%m/%Y %I:%M %p', errors='coerce')
print(df['ExpiryDate'])


# Calculate days to expiry
today = datetime.today()
df['Days to Expiry'] = (df['ExpiryDate'] - today).dt.days
print(df['Days to Expiry'])

# Create expiry buckets
bins = [-float('inf'), 90, 180, 270, 360, 450, 540, 720, 900, 901, float('inf')]
labels = ['Expired', '<3 months','3-6 months', '6-9 months', '9-12 months', 
          '12-15 months', '15-18 months', '18-24 months', '24-30 months', '30+ months']
df['Expiry Bucket'] = pd.cut(df['Days to Expiry'], bins=bins, labels=labels)
#print(df['Expiry Bucket'])

# Calculate value
df['Lottable01'] = pd.to_numeric(df['Lottable01'], errors='coerce')
df['Value of Goods'] = df['Lottable01'] * df['Available Qty']

# Optimize memory
#df['Zone'] = df['Zone'].astype('category')
#df['Inv Bucket'] = df['Inv Bucket'].astype('category')
#df['Site Location '] = df['Site Location '].astype('category')

#print(df)

# Aggregate (simplified grouping)
report = df.groupby([
    'SKU',
    'SKU_Name',
    'Hirearchy',
    'Brand1',
    'Site Location ',
    'Zone', 
    'Inv Bucket', 
    'Expiry Bucket',
    'ExpiryDate',
    'MFG Date',
    'Lottable01',
], observed=True).agg({
    'Available Qty': 'sum',
    #'Lottable01':'nunique',
    'Value of Goods': 'sum'
}).reset_index()
print(report)
# Save
# Database Configuration


#report=report.drop(columns=['SKU Desc', 'Hierarchy Code','Brand'])
report.to_csv("D:/inventory_aging_report.csv", index=False)
print("Report generated!")