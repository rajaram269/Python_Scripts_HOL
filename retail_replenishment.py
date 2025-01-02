import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
from scipy import stats
from sklearn.cluster import KMeans

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

def analyze_store_sku_performance(sales_data, stock_data):
    """
    Analyze sales and stock data at store-SKU level
    """
    sales_data['week'] = sales_data['date'].dt.isocalendar().week
    try:
        insights = {}
        
        # 1. Basic Store-SKU Performance Metrics
        weekly_stats = sales_data.groupby(['store_id', 'sku_id','week']).agg(
            #'sales_units': ['mean', 'std', 'count']
            avg_weekly_sales = ('sales_units','sum'),
            #sales_std = ('sales_units','std'),
            #weeks_of_data = ('sales_units','count'),
            sales_value = ('sales_value','sum'),
            #total_sales = ('sales_units','sum'),
            sales_days = ('date','nunique'),
            #weekly_sale_frequency = ('sales_units','count')
        ).reset_index()
        total_weeks = sales_data['week'].nunique()
        #weekly_stats.columns = ['avg_weekly_sales', 'sales_std', 'weeks_of_data', 'avg_weekly_revenue','total_sales','sales_days','weekly_sale_frequency']
        #print(weekly_stats['sales_std'])
        weekly_std_stats =weekly_stats.groupby(['store_id','sku_id']).agg(
            avg_weekly_sales=('avg_weekly_sales', 'mean'),   # Average weekly sales across all weeks
            #total_sales=('avg_weekly_sales', 'sum'),         # Total sales across all weeks
            weeks_of_data=('week', 'count'),                # Number of weeks with data
            avg_weekly_revenue=('sales_value', 'mean'),     # Average weekly revenue across all weeks
            sales_std=('avg_weekly_sales', 'std')           # Standard deviation of weekly sales
        ).reset_index()
        #print(weekly_std_stats)
        
        weekly_std_stats['sales_std'] = weekly_std_stats['sales_std'].fillna(0)
        weekly_std_stats['sale_frequency_in_weeks']= weekly_std_stats['weeks_of_data'] / total_weeks

        # Consolidate metrics to the latest week's data for current stock merge
        #latest_week_metrics = weekly_std_stats.sort_values('week').drop_duplicates(['store_id', 'sku_id'], keep='last')
        weekly_std_stats.to_csv('E:/test123.csv',index=False)
        print(weekly_std_stats)

        
        # Merge with current stock
        store_sku_metrics = pd.merge(
            latest_week_metrics,
            stock_data,
            on=['store_id', 'sku_id'],
            how='inner',
            validate='1:1'
        )
        
        # 2. Calculate Stock Coverage
        store_sku_metrics['weeks_coverage'] = (
            store_sku_metrics['current_stock'] / 
            store_sku_metrics['avg_weekly_sales']
        ).replace([np.inf, -np.inf], np.nan)
        
        # 3. Sales Velocity Calculation
        recent_sales = sales_data.sort_values('date').groupby(['store_id', 'sku_id']).tail(30)
        
        def calculate_velocity(group):
            daily_sales = group.groupby('date')['sales_units'].sum().reset_index()
            """if len(daily_sales) > 1:
                x = np.arange(len(daily_sales))
                y = daily_sales['sales_units'].values
                slope, _, _, _, _ = stats.linregress(x, y)
                return slope"""

            return 0
        
        #sales_velocity = recent_sales.groupby(['store_id', 'sku_id']).apply(calculate_velocity)
        #sales_velocity = sales_velocity.reset_index(name='velocity')
        store_sku_metrics['sales_velocity']=store_sku_metrics['total_sales']/store_sku_metrics['sales_days']
        
        # Merge velocity back to metrics
        """store_sku_metrics = pd.merge(
            store_sku_metrics,
            sales_velocity,
            on=['store_id', 'sku_id'],
            how='left'
        )"""
        
        # 4. SKU Segmentation
        store_sku_metrics['revenue_rank'] = store_sku_metrics.groupby('store_id')['avg_weekly_revenue'].rank(ascending=False).round()
        #store_sku_metrics['weekly_sales_frequency'] = store_sku_metrics.groupby(['store_id','sku_id','week']).size().reset_index(name='weekly_sales_frequency')
        
        def assign_sku_segment(row):
            if row['revenue_rank'] < 10:
                return 'A - High Value'
            elif row['weekly_sale_frequency'] > 3:
                return 'B - Regular'
            elif row['weekly_sale_frequency'] > 1:
                return 'C - Moderate'
            else:
                return 'D - Slow Moving'
        
        store_sku_metrics['sku_segment'] = store_sku_metrics.apply(assign_sku_segment, axis=1)
        
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
        )
        print(store_sku_metrics['safety_stock'])
        
        store_sku_metrics['reorder_point'] = (
            store_sku_metrics['avg_weekly_sales'] * lead_time_weeks + 
            store_sku_metrics['safety_stock']
        )
        
        # 6. Lost Sales Risk Analysis
        store_sku_metrics['weeks_until_stockout'] = np.where(
            store_sku_metrics['sales_velocity'] > 0,
            store_sku_metrics['current_stock'] / (store_sku_metrics['sales_velocity'] * 7),
            float('inf')
        )
        
        store_sku_metrics['potential_weekly_revenue_loss'] = np.where(
            store_sku_metrics['weeks_coverage'] < lead_time_weeks,
            store_sku_metrics['avg_weekly_revenue'] * (lead_time_weeks - store_sku_metrics['weeks_coverage']),
            0
        )
        
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
        print(store_sku_metrics)
        store_metrics_df = pd.DataFrame(store_sku_metrics)
        store_metrics_df.to_csv('E:/Nykaa_Analysis/store_metric.csv',index=False)
        # Store results
        insights['store_sku_metrics'] = store_sku_metrics
        #insights['dow_patterns'] = dow_patterns
        
        # Critical SKUs requiring attention
        """ insights['critical_skus'] = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < 2) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular'])) &
            (store_sku_metrics['velocity'] > 0)
        ].sort_values(['sku_segment', 'potential_weekly_revenue_loss'], ascending=[True, False]) """
        #insights=pd.DataFrame(insights)
        #insights.to_csv('E:/Nykaa_Analysis/insights2.csv',index=False)
        
        #print(insights)
        return insights
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        raise

def generate_sku_recommendations(insights):
    """
    Generate detailed recommendations at store-SKU level
    """
    lead_time =3 #weeks
    try:
        recommendations = []
        store_sku_metrics = insights['store_sku_metrics']
        
        # 1. Immediate Replenishment Needs
        critical_skus = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < lead_time+1) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular']))
        ]
        
        for _, item in critical_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'sku_id': item['sku_id'],
                'priority': 'CRITICAL',
                'category': 'Stock Alert',
                'action': f"SKU {item['sku_id']} ({item['sku_segment']}) will stockout in {item['weeks_until_stockout']:.1f} weeks. "
                         f"Order {max(item['reorder_point'] - item['current_stock'], 0):.0f} units. "
                         f"Potential weekly revenue loss: INR {item['potential_weekly_revenue_loss']:.2f}"
            })
        # 1b. Non-Critical Replenishment Needs
        critical_skus = store_sku_metrics[
            (store_sku_metrics['weeks_until_stockout'] < lead_time+1) & 
            (store_sku_metrics['sku_segment'].isin(['C - Moderate', 'D - Slow Moving']))
        ]
        
        for _, item in critical_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'sku_id': item['sku_id'],
                'priority': 'Non-CRITICAL',
                'category': 'Stock Alert',
                'action': f"SKU {item['sku_id']} ({item['sku_segment']}) will stockout in {item['weeks_until_stockout']:.1f} weeks. "
                         f"Order {max(item['reorder_point'] - item['current_stock'], 0):.0f} units. "
                         f"Potential weekly revenue loss: INR {item['potential_weekly_revenue_loss']:.2f}"
            })
        
        # 2. Overstock Situations
        overstock_threshold = lead_time+2
        overstock_skus = store_sku_metrics[
            (store_sku_metrics['weeks_coverage'] > overstock_threshold) & 
            (store_sku_metrics['sku_segment'].isin(['A - High Value', 'B - Regular']))
        ]
        
        for _, item in overstock_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'sku_id': item['sku_id'],
                'priority': 'MEDIUM',
                'category': 'Inventory Optimization',
                'action': f"Excess inventory of SKU {item['sku_id']}. Consider redistributing "
                         f"{(item['current_stock'] - item['reorder_point']):.0f} units. "
                         f"Current coverage: {item['weeks_coverage']:.1f} weeks"
            })

        # 2b. Overstock Situations
        overstock_threshold = lead_time+2
        overstock_skus = store_sku_metrics[
            (store_sku_metrics['weeks_coverage'] > overstock_threshold) & 
            (store_sku_metrics['sku_segment'].isin(['C - Moderate', 'D - Slow Moving']))
        ]
        
        for _, item in overstock_skus.iterrows():
            recommendations.append({
                'store_id': item['store_id'],
                'sku_id': item['sku_id'],
                'priority': 'MEDIUM - NC',
                'category': 'Inventory Optimization',
                'action': f"Excess inventory of SKU {item['sku_id']}. Consider redistributing "
                         f"{(item['current_stock'] - item['reorder_point']):.0f} units. "
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
            ).sum(),
            'total_weekly_revenue_at_risk': metrics['potential_weekly_revenue_loss'].sum()
        }
        #summary_df = pd.DataFrame(summary)
        #summary_df.to_csv('E:/Nykaa_Analysis/analysis_summary.csv',index=False)
        with open('E:/Nykaa_Analysis/analysis_summary.txt', 'w') as f:
            f.write(json.dumps(summary))
        print (summary)
        return summary
        
    except Exception as e:
        print(f"Error generating summary report: {str(e)}")
        raise

def main(sales_data, stock_data):
    """
    Main function to run the entire analysis
    """
    try:
        print("Starting analysis...")
        
        # Step 1: Preprocess the data
        sales_df, stock_df = preprocess_data(sales_data, stock_data)
        print("Data preprocessing completed")
        
        # Step 2: Run the analysis
        insights = analyze_store_sku_performance(sales_df, stock_df)
        print("Analysis completed")
        
        # Step 3: Generate recommendations
        recommendations = generate_sku_recommendations(insights)
        print("Recommendations generated")
        
        # Step 4: Generate summary report
        summary = generate_summary_report(insights)
        print("Summary report generated")
        
        return insights, recommendations, summary
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise

# Example usage:
"""
# Load your data
sales_data = pd.read_csv('sales_data.csv')
stock_data = pd.read_csv('stock_data.csv')

# Run the analysis
insights, recommendations, summary = main(sales_data, stock_data)

# View results
print("\nSummary Report:")
print(pd.Series(summary))

print("\nTop Critical SKUs:")
print(recommendations[recommendations['priority'] == 'CRITICAL'].head())

# Export results if needed
insights['store_sku_metrics'].to_csv('store_sku_metrics.csv', index=False)
recommendations.to_csv('recommendations.csv', index=False)
"""
def process_files(sales_data, inventory_data):
    
    sales_data = pd.read_csv(sales_data)
    stock_data = pd.read_csv(inventory_data)
    stock_data = stock_data.groupby(['store_id','sku_id']).agg({
            'current_stock' : ['sum']
        }).round(2)
    stock_data.columns=['current_stock']
    stock_data=stock_data.reset_index()
    #processed_data = 
    sales_df, stock_df = preprocess_data(sales_data,stock_data)
    insights = analyze_store_sku_performance(sales_df,stock_df)
    recommendations = generate_sku_recommendations(insights)
    summary = generate_summary_report(insights)
    
    
    return 




inventory_data='E:/Nykaa_Analysis/stock_df.csv'
sales_data = 'E:/Nykaa_Analysis/sales_df.csv'
# Run the processing
result = process_files(sales_data, inventory_data)