import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def preprocess_data(df):
    """
    Clean and convert data types in the DataFrame
    """
    try:
        # Convert discount_percentage to numeric, removing any '%' signs if present
        df['discount_percentage'] = pd.to_numeric(df['discount_percentage'].astype(str).str.replace('%', '').astype(float))
        
        # Convert other numeric columns
        df['original_price'] = pd.to_numeric(df['original_price'], errors='coerce')
        df['units_sold'] = pd.to_numeric(df['units_sold'], errors='coerce')
        
        # Remove rows with NaN values after conversion
        df_clean = df.dropna(subset=['original_price', 'discount_percentage', 'units_sold'])
        
        # Print data quality report
        print("\nData Quality Report:")
        print(f"Original rows: {len(df)}")
        print(f"Rows after cleaning: {len(df_clean)}")
        print("\nData Types:")
        print(df_clean.dtypes)
        
        return df_clean
        
    except Exception as e:
        print(f"Error in data preprocessing: {str(e)}")
        print("\nSample of problematic data:")
        print(df.head())
        raise

def analyze_price_elasticity(df):
    """
    Analyze price elasticity of demand for SKUs based on sales and discount data.
    """
    # Preprocess the data first
    df = preprocess_data(df)
    # Calculate effective price
    df['effective_price'] = df['original_price'] * (1 - df['discount_percentage']/100)
    df['revenue'] = df['effective_price'] * df['units_sold']

    #filter data which has significant discount coverage for elasticity analysis
    daily_data = df.groupby(['sku', 'date']).agg({
        'units_sold': 'sum',
        'discount_percentage': 'mean'
    }).reset_index()
    # Calculate stats for filtering
    sku_stats = daily_data.groupby('sku').agg({
        'units_sold': 'count',  # Total days of sales data
        'discount_percentage': lambda x: sum(x > 10),  # Days with significant discounts
    }).rename(columns={
        'units_sold': 'total_days',
        'discount_percentage': 'discounted_days'
    }).reset_index()

    # Filter criteria
    min_total_days = 80
    min_discounted_days = 30
    filtered_skus = sku_stats[
        (sku_stats['total_days'] >= min_total_days) &
        (sku_stats['discounted_days'] >= min_discounted_days)
    ]['sku']
    # Filter original data
    filtered_data = df[df['sku'].isin(filtered_skus)]
    sku_day_data = (filtered_data.groupby(['sku','date'])
                    .agg(daily_units_sold=('units_sold','sum'),
                         daily_revenue=('revenue','sum'),
                         daily_effective_price=('effective_price','mean'),
                         daily_avg_discount=('discount_percentage','mean')).reset_index())
    
    #print(df)
    sku_day_data.to_csv('E:/elasticity/Nykaa/regression_df.csv', index=False)
    # Initialize results storage
    elasticity_results = []
    monthly_impact = []
    discount_effect = []
    
    # Analyze each SKU
    for sku in df['sku'].unique():
        sku_data = df[df['sku'] == sku]
        sku_data1=sku_day_data[sku_day_data['sku']==sku]
        print(len(sku_data))
        
        # Only calculate elasticity if we have enough data points
        if len(sku_data1) >= 10:  # Need at least 2 points for regression
            # Calculate overall elasticity
            log_quantity = np.log(sku_data1['daily_units_sold'].clip(lower=1))  # Clip to avoid log(0)
            log_price = np.log(sku_data1['daily_effective_price'].clip(lower=0.01))  # Clip to avoid log(0)
            print(log_quantity,log_price)
            # Linear regression to calculate elasticity
            try:
             slope, intercept, r_value, p_value, std_err = stats.linregress(log_price, log_quantity)
            except Exception as e:
             print(f"Error1 processing data: {e}")
             continue

            
            # Store overall elasticity
            elasticity_results.append({
                'sku': sku,
                'price_elasticity': -slope,
                'r_squared': r_value**2,
                'p_value': p_value,
                'data_points': len(sku_data)
            })
        if len(sku_data) >=10:
            baseline_sales= sku_data['units_sold'].mean()
            no_discount = sku_data[sku_data['discount_percentage'] <= 10]
            with_discount = sku_data[sku_data['discount_percentage'] > 10]
            if len(no_discount) > 0 and len(with_discount) > 0:
             # Baseline metrics
             baseline_daily_units = no_discount.groupby('date')['units_sold'].sum()
             baseline_daily_revenue = no_discount.groupby('date')['revenue'].sum()
            
              # Group by discount ranges
             discount_ranges = pd.cut(with_discount['discount_percentage'], bins=[0, 20, 40, 60, 100])
             for discount_range in discount_ranges.unique():
                range_data = with_discount[discount_ranges == discount_range]
                if len(range_data) > 0:
                    # Calculate key metrics
                    avg_discount = range_data['discount_percentage'].mean()
                    discounted_daily_units = range_data.groupby('date')['units_sold'].sum()
                    discounted_daily_revenue = range_data.groupby('date')['revenue'].sum()
                    
                    # Calculate lifts
                    unit_lift = ((discounted_daily_units.mean() - baseline_daily_units.mean()) / 
                               baseline_daily_units.mean() * 100)
                    revenue_lift = ((discounted_daily_revenue.mean() - baseline_daily_revenue.mean()) /
                                  baseline_daily_revenue.mean() * 100)
                    
                    # Calculate efficiency metrics
                    discount_efficiency = unit_lift / avg_discount  # Lift per discount percentage
                    incremental_units = discounted_daily_units.mean() - baseline_daily_units.mean()
                    revenue_impact = discounted_daily_revenue.mean() - baseline_daily_revenue.mean()
                    
                    discount_effect.append({
                        'sku': sku,
                        'discount_range': discount_range,
                        'avg_discount': avg_discount.round(),
                        'unit_lift_percentage': unit_lift.round(),
                        'revenue_lift_percentage': revenue_lift.round(),
                        'discount_efficiency': discount_efficiency.round(),
                        'incremental_units': incremental_units.round(),
                        'revenue_impact': revenue_impact.round(),
                        'sample_size': len(range_data),
                        'baseline_avg_daily_units': baseline_daily_units.mean().round(),
                        'with_disc_avg_daily_units': discounted_daily_units.mean().round(),
                        'baseline_svg_daily_revenue': baseline_daily_revenue.mean().round(),
                        'with_disc_avg_daily_revenue': discounted_daily_revenue.mean().round()
                    })
            #baseline_discount=sku['discount_percentage'].mean()
            # Calculate monthly impact
            for month in sku_data['month'].unique():
                month_data = sku_data[sku_data['month'] == month]
                daily_units_sold= month_data.groupby('date')['units_sold'].sum()
                avg_discount = month_data['discount_percentage'].mean()
                avg_sales = daily_units_sold.mean()
                #baseline_sales = month_data[month_data['discount_percentage'] == month_data['discount_percentage'].min()]['units_sold'].mean()
                
                if not pd.isna(baseline_daily_units.mean()) and baseline_daily_units.mean() != 0:
                    sales_lift = ((avg_sales - baseline_daily_units.mean()) / baseline_daily_units.mean()) * 100
                else:
                    sales_lift = np.nan
                    
                monthly_impact.append({
                    'sku': sku,
                    'month': month,
                    'avg_discount': avg_discount.round(),
                    'sales_lift_percentage': sales_lift.round(),
                    'data_point':len(month_data),
                    'baseline_sales':baseline_daily_units.mean().round()*30,
                    'avg_sales': daily_units_sold.sum().round()
                    #'baseline_discount':baseline_discount
                })   
         
    if not elasticity_results:
        raise ValueError("No valid data for elasticity calculation")
    
    # Convert results to DataFrames
    elasticity_df = pd.DataFrame(elasticity_results)
    monthly_impact_df = pd.DataFrame(monthly_impact)
    discount_impact_df = pd.DataFrame(discount_effect)
    
    # Add elasticity interpretation
    elasticity_df['elasticity_category'] = pd.cut(
        elasticity_df['price_elasticity'].abs(),
        bins=[0, 0.5, 1, np.inf],
        labels=['Inelastic', 'Moderately Elastic', 'Highly Elastic']
    )
    
    return elasticity_df, monthly_impact_df, discount_impact_df

def summarize_elasticity(elasticity_df, monthly_impact_df, discount_impact_df):
    """
    Generate insights from the elasticity analysis
    """
    summary = {
        'most_elastic_skus': elasticity_df.nlargest(5, 'price_elasticity')[['sku', 'price_elasticity', 'elasticity_category']],
        'least_elastic_skus': elasticity_df.nsmallest(5, 'price_elasticity')[['sku', 'price_elasticity', 'elasticity_category']],
        'category_distribution': elasticity_df['elasticity_category'].value_counts(),
        'avg_elasticity': elasticity_df['price_elasticity'].mean()
    }
    
    return summary

def plot_elasticity_distribution(elasticity_df, output_dir):
    """
    Create visualizations for elasticity analysis
    """
    # Elasticity distribution plot
    plt.figure(figsize=(10, 6))
    sns.histplot(data=elasticity_df, x='price_elasticity', bins=30)
    plt.title('Distribution of Price Elasticity Across SKUs')
    plt.xlabel('Price Elasticity')
    plt.ylabel('Count')
    plt.savefig(output_dir / 'elasticity_distribution.png')
    plt.close()

    # Category distribution plot
    plt.figure(figsize=(8, 6))
    elasticity_df['elasticity_category'].value_counts().plot(kind='bar')
    plt.title('Distribution of Elasticity Categories')
    plt.xlabel('Category')
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig(output_dir / 'elasticity_categories.png')
    plt.close()

def plot_monthly_trends(monthly_impact_df, output_dir):
    """
    Create visualizations for monthly trends
    """
    # Sales lift vs discount scatter plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=monthly_impact_df, x='avg_discount', y='sales_lift_percentage')
    plt.title('Sales Lift vs Discount Percentage')
    plt.xlabel('Average Discount (%)')
    plt.ylabel('Sales Lift (%)')
    plt.savefig(output_dir / 'sales_lift_vs_discount.png')
    plt.close()

def main():
    # Create output directory for plots
    output_dir = Path('E:/elasticity/Nykaa')
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Read the CSV file
        print("Reading CSV file...")
        #file_name = input("E:/elasticity/Nykaa.csv")
        df = pd.read_csv('E:/elasticity/Nykaa/Nykaa.csv')
        print(df)
        
        # Check if required columns exist
        required_columns = ['sku', 'month', 'original_price', 'discount_percentage', 'units_sold', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Error: Missing required columns: {missing_columns}")
            print(f"Available columns: {df.columns.tolist()}")
            return
            
        # Print sample of input data
        print("\nSample of input data:")
        print(df.head())
        print("\nColumn data types:")
        print(df.dtypes)
        
        # Run analysis
        print("\nRunning elasticity analysis...")
        elasticity_df, monthly_impact_df, discount_impact_df = analyze_price_elasticity(df)
        
        # Generate summary
        summary = summarize_elasticity(elasticity_df, monthly_impact_df, discount_impact_df)
        
        # Create visualizations
        print("Generating plots...")
        plot_elasticity_distribution(elasticity_df, output_dir)
        plot_monthly_trends(monthly_impact_df, output_dir)
        
        # Save results to CSV
        elasticity_df.to_csv(output_dir / 'sku_elasticity_results.csv', index=False)
        monthly_impact_df.to_csv(output_dir / 'monthly_impact_results.csv', index=False)
        discount_impact_df.to_csv(output_dir / 'discount_impact_results.csv', index=False)
        
        # Save summary to text file
        with open(output_dir / 'analysis_summary.txt', 'w') as f:
            f.write("Price Elasticity Analysis Summary\n")
            f.write("================================\n\n")
            f.write("Most Elastic SKUs:\n")
            f.write(summary['most_elastic_skus'].to_string())
            f.write("\n\nLeast Elastic SKUs:\n")
            f.write(summary['least_elastic_skus'].to_string())
            f.write("\n\nCategory Distribution:\n")
            f.write(summary['category_distribution'].to_string())
            f.write(f"\n\nAverage Elasticity: {summary['avg_elasticity']:.2f}")
        
        # Print summary to console
        print("\nAnalysis Summary:")
        print("\nMost Elastic SKUs:")
        print(summary['most_elastic_skus'])
        print("\nLeast Elastic SKUs:")
        print(summary['least_elastic_skus'])
        print("\nCategory Distribution:")
        print(summary['category_distribution'])
        print(f"\nAverage Elasticity: {summary['avg_elasticity']:.2f}")
        
        print("\nResults have been saved to the 'elasticity_analysis_output' directory")
        
    except FileNotFoundError:
        print("Error: CSV file not found. Please check the file name and path.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print("\nFor debugging, please check:")
        print("1. File exists and is readable")
        print("2. All required columns are present")
        print("3. Numeric columns contain valid numbers")
        print("4. No missing values in key columns")

if __name__ == "__main__":
    main()