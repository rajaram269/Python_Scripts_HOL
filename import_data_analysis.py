import pandas as pd

# Step 1: Load the CSV file
file_path = "D:/Import_Analysis/import_data.csv"  # Replace with the actual path to your .csv file
data = pd.read_csv(file_path)

# Step 2: Data cleaning
# Drop duplicates and handle missing values
data = data.dropna(subset=["brand_name", "total_value_usd", "product_name","unit_value_usd"])  # Ensure critical columns are not null
#print(data)
# Convert `total_value_usd` and `qty` to numeric for calculations
data["total_value_usd"] = pd.to_numeric(data["total_value_usd"], errors="coerce")
data["unit_value_usd"]=pd.to_numeric(data["unit_value_usd"],errors="coerce")
def average_of_lowest_50_percent(prices):
    sorted_prices = prices.sort_values()  # Sort prices in ascending order
    lower_half = sorted_prices.iloc[:len(sorted_prices) // 2]  # Take the lowest 50%
    return lower_half.mean()  # Calculate the average of the lower half

# Compute the average for each product and map back to the original data
data["unit_value_usd"] = data.groupby("product_name")["unit_value_usd"].transform(
    lambda x: average_of_lowest_50_percent(x)
)
data["qty"]=data["total_value_usd"]/data["unit_value_usd"]
data["qty"] = pd.to_numeric(data["qty"], errors="coerce")
data = data.dropna(subset=["total_value_usd", "qty"])  # Drop rows where values are invalid
#print(data)
# Step 3: Get top 15 brands by total_value_usd
top_15_brands = (
    data.groupby("brand_name")["total_value_usd"]
    .sum()
    .sort_values(ascending=False)
    .head(15)
    .index
)
#print(top_15_brands)
top_brands_data = data[data["brand_name"].isin(top_15_brands)]
#print(top_brands_data)
# Step 4: Aggregate data by product_name for the top 15 brands
aggregated_products = (
    top_brands_data.groupby(['product_name','size'])
    .agg(
        brand_name=("brand_name", "first"),  # First occurrence of brand_name
        total_value_usd=("total_value_usd", "sum"),
        total_qty=("qty", "sum"),
        #size=("size", "first"),  # Use the first occurrence of uom
        unit_value_usd=("unit_value_usd", "mean"),  # Mean value for unit_value_USD
        coo=("coo", "first"),  # Use the first occurrence of coo
        hs_code=("hs_code", "first"),  # Use the first occurrence of hs_code
    )
    .reset_index()
)

# Step 5: Get top 100 products across all brands after aggregation
top_100_products = (
    aggregated_products.sort_values(by="total_value_usd", ascending=False)
    .head(100)
)
#print(top_100_products)

# Step 6: Create a summary for the top 15 brands
brand_summary = (
    top_brands_data.groupby("brand_name")
    .agg(
        total_value_usd=("total_value_usd", "sum"),
        total_qty=("qty", "sum"),
        total_products=("product_name", "nunique"),
    )
    .sort_values(by="total_value_usd", ascending=False)
    .reset_index()
)

# Relevant columns to keep
relevant_columns = [
    "product_name", "brand_name", "total_value_usd", 
    "total_qty", "size", "unit_value_usd", "coo", "hs_code"
]

# Step 6: Create brandwise top 15 products
with pd.ExcelWriter("D:/Import_Analysis/competitor_analysis.xlsx") as writer:
    # Sheet 1: Top 100 products across brands
    top_100_products[relevant_columns].to_excel(
        writer, sheet_name="Top_100_Products", index=False
    )
    
    # Sheet 2: Top 15 brands summary
    brand_summary.to_excel(writer, sheet_name="Top_15_Brands_Summary", index=False)
# Sheets for each brand
    for brand in top_15_brands:
        # Aggregate the data at the product level for the current brand
        brand_data_aggregated = (
            top_brands_data[top_brands_data["brand_name"] == brand]
            .groupby(['product_name','size'])
            .agg(
                total_value_usd=("total_value_usd", "sum"),
                total_qty=("qty", "sum"),
                #size=("size", "first"),  # Use the first occurrence of uom
                unit_value_usd=("unit_value_usd", "mean"),  # Average unit value
                coo=("coo", "first"),  # Use the first occurrence of coo
                hs_code=("hs_code", "first"),  # Use the first occurrence of hs_code
            )
            .reset_index()
        )
        relevant_columns = ["product_name", "total_value_usd", "total_qty", "size", "unit_value_usd", "coo", "hs_code"]
    
        # Sort the aggregated data by total_value_usd and pick the top 15 SKUs
        top_15_skus = brand_data_aggregated.sort_values(
            by="total_value_usd", ascending=False
        ).head(15)

        # Save the top 15 SKUs for the current brand to a separate sheet
        top_15_skus[relevant_columns].to_excel(
            writer, sheet_name=brand[:31], index=False  # Sheet name max length is 31 chars
        )


print("Competitor benchmarking analysis complete. File saved as 'competitor_analysis.xlsx'.")
