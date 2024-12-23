import pandas as pd
import openai
import json
import time

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
    Provide the output as a JSON object for each product name. If you can't find any of the fields fill it with 'NA'. Output shall be strict JSON.

    Example:
    Input: BEAUTY OF JOSEON RADIANCE CLEANSING BALM 100ML (COSMETIC PRODUCT)
    Output: {"Brand Name": "Beauty of Joseon", "Product Name": "Radiance Cleansing Balm", "Size": "100ml"}

    Input: """ + " ".join(product_names) + """
    Output:
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": prompt}],
            max_tokens=1000,
            temperature=0,
            response_format={"type":"json_object"}
        )
        result = response.choices[0].message.content.strip()
        results = json.loads(result)  # Parse the JSON string into a Python list
        return results
    except Exception as e:
        print(f"Error processing batch: {e}")
        return [None] * len(product_names)  # Return empty results for this batch if error occurs

def process_in_batches(df, batch_size=1000):
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
        batch_results = process_with_llm_chunk(batch.tolist())
        
        # Append the results
        results.extend(batch_results)
        
        # Create a DataFrame for this batch of results
        batch_df = pd.DataFrame(batch_results)
        
        # Write the batch results to CSV immediately
        batch_df.to_csv('processed_products.csv', mode='a', header=not bool(results), index=False)
        
        # Add a small delay to avoid hitting rate limits too fast
        time.sleep(5)
    
    return results

# Load your data
file_path = "D:/Import_Analysis/cleanuptest.xlsx"
df = pd.read_excel(file_path)

# Process the data in batches and get results
results = process_in_batches(df)

# Convert the results into a DataFrame for final output
final_df = pd.DataFrame(results)

# Save the cleaned data to an Excel file (optional)
final_df.to_excel("cleaned_products.xlsx", index=False)

print("Processing complete. Results saved to processed_products.csv and cleaned_products.xlsx.")
