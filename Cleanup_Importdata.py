import pandas as pd
import openai
import json

# Set up OpenAI API Key
openai.api_key = "sk-L50sGDoqWwCHRL-uBgAzd3_nogeUS6XxOsmD18Z_rPT3BlbkFJbEMi5pkxXfAiv7o9NiCilyMs4AeUpLvgc6x9vH4eIA"

def process_with_llm(product_name):
    """
    Send product name to LLM and get structured output.
    """
    prompt = f"""
    You are a data analyst. Use information available in the internet and general understanding. Extract the following details from the product description: 
    1. Brand Name (If available)
    2. Product Name
    3. Size (if available)
    Provide the output as a JSON object. If you can't find any of the fields fill it with 'NA' Out put shall be strict JSON

    Example:
    Input: BEAUTY OF JOSEON RADIANCE CLEANSING BALM 100ML (COSMETIC PRODUCT)
    Output: {{"Brand Name": "Beauty of Joseon", "Product Name": "Radiance Cleansing Balm", "Size": "100ml"}}

    Input: {product_name}
    Output:
    """
   # print(prompt)
    response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[ 
                {"role": "system", "content": prompt}
            #prompt=prompt,
            #max_tokens=100,
            #temperature= 
            
            ],
            response_format={"type":"json_object"}
        )
  #  print(response.choices[0].message.content.strip())
    result = (response.choices[0].message.content.strip())
    print(result)
    return eval(result)  # Convert JSON string to Python dictionary

# Load your data
file_path = "D:/Import_Analysis/cleanuptest.xlsx"
df = pd.read_excel(file_path)

# Process each row
results = df["Original Name"].apply(process_with_llm)
#test_product = "COSRX FULL FIT PROPOLIS SYNERGY TONER 150ML (COSMETIC PRODUCT)"
#results=(process_with_llm(test_product))

# Expand the JSON results into separate columns
df_results = pd.DataFrame(results.to_list())

# Merge the results back with the original data
#final_df = pd.concat([df, df_results], axis=1)

# Save the cleaned data
results.to_excel("cleaned_products.xlsx", index=False)
output_path = 'cleaned_products1.csv'
df_results.to_csv(output_path, index=False)
print("Processing complete. Results saved to cleaned_products.xlsx")
