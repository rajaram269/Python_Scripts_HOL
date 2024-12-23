import pandas as pd
import openai
import json
import time
import os

# Set up OpenAI API Key
openai.api_key = "sk-L50sGDoqWwCHRL-uBgAzd3_nogeUS6XxOsmD18Z_rPT3BlbkFJbEMi5pkxXfAiv7o9NiCilyMs4AeUpLvgc6x9vH4eIA"

def process_with_llm_chunk(product_names):
    """
    Send a chunk of product names to LLM and get structured output.
    """
    prompt = """
    You are a data analyst. Use information available in the internet and general understanding. Extract the following details from the product description: 
    1. Brand Name (If available)
    2. Product Name
    3. Size (if available)

    Provide the output as a JSON array called 'products' for each product name. If you can't find any of the fields fill it with 'NA', this is important. Batch output shall be strict JSON.

    Example:
    Input: BEAUTY OF JOSEON RADIANCE CLEANSING BALM 100ML (COSMETIC PRODUCT)
    Output: {"Brand Name": "Beauty of Joseon", "Product Name": "Radiance Cleansing Balm", "Size": "100ml"}
    Example:
    Input:  Rice Ceramide Moisturiser  (COSMETIC PRODUCT)
    Output: {"Brand Name": "NA", "Product Name": "Rice Ceramide Moisturiser", "Size": "NA"}

    Input: """ + " ".join(product_names) + """
    Output:
    """
    response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=1000,
            temperature=0,
            response_format={"type":"json_object"}
        )
    result = json.loads(response.choices[0].message.content.strip())
    #print(result["products"])
    results = result  # Parse the JSON string into a Python list
    return results
def process_in_batches(df, batch_size=7):
    """
    Process the data in batches to avoid hitting the rate limit.
    """
    results = []
    total_rows = len(df)
    
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df["Original Name"].iloc[start_idx:end_idx]
        
        print(f"Processing batch {start_idx // batch_size + 1} ({start_idx}-{end_idx})")
        
        # Call the function to process the batch of product names
        print(batch.tolist())
        batch_results = process_with_llm_chunk(batch.tolist())
        
       # Split the result into individual JSON objects (one per product)
        #batch_json_results = batch_results.split('\n')  # Assuming each result is separated by a newline

       # Convert the products data into a DataFrame
        dfcsv = pd.DataFrame(batch_results["products"])
        print(dfcsv)
       # Check if the file already exists
        file_exists = os.path.isfile(output_csv_path)
    
       # Write the data to the CSV, appending if the file exists
        if file_exists:
            dfcsv.to_csv(output_csv_path, mode='a', header=False, index=False)  # Append without header
        else:
            dfcsv.to_csv(output_csv_path, mode='w', header=True, index=False)  # Create with header
        print(f"Data appended to {output_csv_path}.")



        # Add a small delay to avoid hitting rate limits too fast
        time.sleep(5)
    
    return results

# Load your data
file_path = "D:/Import_Analysis/cleanuptest.xlsx"
df = pd.read_excel(file_path)
# File path to save the CSV
output_csv_path = "products_data2.csv"

# Process the data in batches and get results
results = process_in_batches(df)