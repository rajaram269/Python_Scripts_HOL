import pandas as pd
import numpy as np

# Preprocessing function
def preprocess_data(sales_data, stock_data):

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

# Function to calculate sales velocity
def calculate_velocity(group):
    # Example: calculating average weekly sales and sales standard deviation
    group['avg_weekly_sales'] = group['quantity'].sum() / group['date'].nunique()
    group['sales_std'] = group['quantity'].std() if len(group) > 1 else 0
    return group

# Analysis function
def analyze_store_sku_performance(sales, stock):
    # Filter recent sales data
    recent_sales = sales[sales['date'] > pd.Timestamp.now() - pd.Timedelta(days=30)]

    # Calculate sales velocity
    sales_velocity = recent_sales.groupby(['store_id', 'sku_id']).apply(calculate_velocity)

    # Merge sales velocity with stock data
    stock_summary = stock.groupby(['store_id', 'sku_id'])['stock_on_hand'].sum().reset_index()
    store_sku_metrics = pd.merge(sales_velocity, stock_summary, on=['store_id', 'sku_id'], how='left')

    # Add Safety Stock and Reorder Points
    lead_time_weeks = 1
    service_level_z = {
        'A - High Value': 2.326,
        'B - Regular': 1.96,
        'C - Moderate': 1.645,
        'D - Slow Moving': 1.28
    }

    store_sku_metrics['safety_stock'] = store_sku_metrics.apply(
        lambda row: service_level_z.get(row.get('sku_segment', 'D - Slow Moving'), 1.28) *
                    row['sales_std'] * np.sqrt(lead_time_weeks),
        axis=1
    )

    store_sku_metrics['reorder_point'] = (
        store_sku_metrics['avg_weekly_sales'] * lead_time_weeks +
        store_sku_metrics['safety_stock']
    )

    # Debugging: Check if columns are created
    print(f"Columns in store_sku_metrics: {store_sku_metrics.columns}")

    return store_sku_metrics

# Function to generate SKU recommendations
def generate_sku_recommendations(store_sku_metrics):
    recommendations = []
    try:
        for _, row in store_sku_metrics.iterrows():
            if row['current_stock'] < row['reorder_point']:
                recommendations.append({
                    'store_id': row['store_id'],
                    'sku_id': row['sku_id'],
                    'action': f"Order {max(row['reorder_point'] - row['current_stock'], 0):.0f} units."
                })
    except KeyError as e:
        raise ValueError(f"Error generating recommendations: {str(e)}")

    return pd.DataFrame(recommendations)

# Main function
def main(sales_data_file, stock_data_file):
    sales, stock = preprocess_data(sales_data_file, stock_data_file)
    store_sku_metrics = analyze_store_sku_performance(sales, stock)
    recommendations = generate_sku_recommendations(store_sku_metrics)

    # Save results
    store_sku_metrics.to_csv("store_sku_metrics.csv", index=False)
    recommendations.to_csv("recommendations.csv", index=False)

    print("Analysis completed. Results saved.")
    return store_sku_metrics, recommendations

if __name__ == "__main__":
    sales_data_file = "E:/Nykaa_Analysis/sales_df.csv"  # Replace with actual file path
    stock_data_file = "E:/Nykaa_Analysis/stock_df.csv"  # Replace with actual file path

    try:
        store_sku_metrics, recommendations = main(sales_data_file, stock_data_file)
    except Exception as e:
        print(f"Error during execution: {str(e)}")
