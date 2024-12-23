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
    
    print(df)
    # Initialize results storage
    elasticity_results = []
    monthly_impact = []
    
    # Analyze each SKU
    for sku in df['sku'].unique():
        sku_data = df[df['sku'] == sku]
        print(sku_data)
        
        # Only calculate elasticity if we have enough data points
        if len(sku_data) >= 28:  # Need at least 2 points for regression
            # Calculate overall elasticity
            log_quantity = np.log(sku_data['units_sold'].clip(lower=1))  # Clip to avoid log(0)
            log_price = np.log(sku_data['effective_price'].clip(lower=0.01))  # Clip to avoid log(0)
            
            # Linear regression to calculate elasticity
            slope, intercept, r_value, p_value, std_err = stats.linregress(log_price, log_quantity)
            
            # Store overall elasticity
            elasticity_results.append({
                'sku': sku,
                'price_elasticity': -slope,
                'r_squared': r_value**2,
                'p_value': p_value,
                'data_points': len(sku_data)
            })
            baseline_sales= sku_data['units_sold'].mean()
            #baseline_discount=sku['discount_percentage'].mean()
            # Calculate monthly impact
            for month in sku_data['month'].unique():
                month_data = sku_data[sku_data['month'] == month]
                
                avg_discount = month_data['discount_percentage'].mean()
                avg_sales = month_data['units_sold'].mean()
                #baseline_sales = month_data[month_data['discount_percentage'] == month_data['discount_percentage'].min()]['units_sold'].mean()
                
                if not pd.isna(baseline_sales) and baseline_sales != 0:
                    sales_lift = ((avg_sales - baseline_sales) / baseline_sales) * 100
                else:
                    sales_lift = np.nan
                    
                monthly_impact.append({
                    'sku': sku,
                    'month': month,
                    'avg_discount': avg_discount,
                    'sales_lift_percentage': sales_lift,
                    'data_point':len(month_data),
                    #'baseline_discount':baseline_discount
                })
    
    if not elasticity_results:
        raise ValueError("No valid data for elasticity calculation")
    
    # Convert results to DataFrames
    elasticity_df = pd.DataFrame(elasticity_results)
    monthly_impact_df = pd.DataFrame(monthly_impact)
    
    # Add elasticity interpretation
    elasticity_df['elasticity_category'] = pd.cut(
        elasticity_df['price_elasticity'],
        bins=[-np.inf, 1, 2, np.inf],
        labels=['Inelastic', 'Moderately Elastic', 'Highly Elastic']
    )
    
    return elasticity_df, monthly_impact_df

def summarize_elasticity(elasticity_df, monthly_impact_df):
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

def analyze_discount_effectiveness(df):
    """
    Parameters:
    df: Transaction level data with columns:
        - sku
        - date
        - original_price
        - discount_percentage
        - units_sold
        - revenue (price * units)
    """
    results = []
    
    for sku in df['sku'].unique():
        sku_data = df[df['sku'] == sku]
        
        # Compare periods with and without discounts
        no_discount = sku_data[sku_data['discount_percentage'] <= 1]
        with_discount = sku_data[sku_data['discount_percentage'] > 1]
        
        if len(no_discount) > 0 and len(with_discount) > 0:
            # Baseline metrics
            baseline_daily_units = no_discount.groupby('date')['units_sold'].mean()
            baseline_daily_revenue = no_discount.groupby('date')['revenue'].mean()
            
            # Group by discount ranges
            discount_ranges = pd.cut(with_discount['discount_percentage'], 
                                   bins=[0, 10, 20, 30, 40, 50, 100])
            
            for discount_range in discount_ranges.unique():
                range_data = with_discount[discount_ranges == discount_range]
                if len(range_data) > 0:
                    # Calculate key metrics
                    avg_discount = range_data['discount_percentage'].mean()
                    discounted_daily_units = range_data.groupby('date')['units_sold'].mean()
                    discounted_daily_revenue = range_data.groupby('date')['revenue'].mean()
                    
                    # Calculate lifts
                    unit_lift = ((discounted_daily_units.mean() - baseline_daily_units.mean()) / 
                               baseline_daily_units.mean() * 100)
                    revenue_lift = ((discounted_daily_revenue.mean() - baseline_daily_revenue.mean()) /
                                  baseline_daily_revenue.mean() * 100)
                    
                    # Calculate efficiency metrics
                    discount_efficiency = unit_lift / avg_discount  # Lift per discount percentage
                    incremental_units = discounted_daily_units.mean() - baseline_daily_units.mean()
                    revenue_impact = discounted_daily_revenue.mean() - baseline_daily_revenue.mean()
                    
                    results.append({
                        'sku': sku,
                        'discount_range': discount_range,
                        'avg_discount': avg_discount,
                        'unit_lift_percentage': unit_lift,
                        'revenue_lift_percentage': revenue_lift,
                        'discount_efficiency': discount_efficiency,
                        'incremental_units': incremental_units,
                        'revenue_impact': revenue_impact,
                        'sample_size': len(range_data)
                    })
    
    return pd.DataFrame(results)

def main():
    # Create output directory for plots
    output_dir = Path('E:/elasticity/shopify')
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Read the CSV file
        print("Reading CSV file...")
        #file_name = input("E:/elasticity/Shopify.csv")
        df = pd.read_csv('E:/elasticity/shopify/shopify.csv')
        print(df)
        
        # Check if required columns exist
        required_columns = ['sku', 'month', 'original_price', 'discount_percentage', 'units_sold']
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
        elasticity_df, monthly_impact_df = analyze_price_elasticity(df)
        
        # Generate summary
        summary = summarize_elasticity(elasticity_df, monthly_impact_df)
        
        # Create visualizations
        print("Generating plots...")
        plot_elasticity_distribution(elasticity_df, output_dir)
        plot_monthly_trends(monthly_impact_df, output_dir)
        
        # Save results to CSV
        elasticity_df.to_csv(output_dir / 'sku_elasticity_results.csv', index=False)
        monthly_impact_df.to_csv(output_dir / 'monthly_impact_results.csv', index=False)
        
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