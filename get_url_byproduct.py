import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm  # For progress bar

def search_amazon(product_name):
    base_url = "https://www.amazon.in/s?k="
    search_url = base_url + "+".join(product_name.split())
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    try:
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            results = soup.find_all('a', class_='a-link-normal s-no-outline', href=True)
            if results:
                return "https://www.amazon.in" + results[4]['href']
    except Exception as e:
        print(f"Error searching Amazon for {product_name}: {e}")
    return "No results found on Amazon."

def search_nykaa(product_name):
    base_url = "https://www.nykaa.com/search/result/?q="
    search_url = base_url + "+".join(product_name.split())
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    try:
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            results = soup.find_all('a', class_='css-qlopj4', href=True)
            if results:
                return "https://www.nykaa.com" + results[0]['href']
    except Exception as e:
        print(f"Error searching Nykaa for {product_name}: {e}")
    return "No results found on Nykaa."

def process_products(input_file, output_file):
    try:
        # Read input CSV into DataFrame
        df = pd.read_csv(input_file)
        
        # Ensure the product name column exists
        if df.empty:
            raise ValueError("Input CSV is empty")
            
        # Get the name of the first column
        product_col = df.columns[0]
        
        # Create empty lists to store results
        amazon_urls = []
        nykaa_urls = []
        
        # Process each product with progress bar
        print("\nSearching for products...")
        for product in tqdm(df[product_col]):
            # Search on Amazon
            amazon_url = search_amazon(product)
            amazon_urls.append(amazon_url)
            
            # Add delay
            time.sleep(1)
            
            # Search on Nykaa
            nykaa_url = search_nykaa(product)
            nykaa_urls.append(nykaa_url)
            
            # Add delay before next product
            time.sleep(1)
        
        # Add results to DataFrame
        df['Amazon_URL'] = amazon_urls
        df['Nykaa_URL'] = nykaa_urls
        
        # Create timestamp for output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{output_file}_{timestamp}.csv"
        
        # Save results to CSV
        df.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"\nResults have been saved to {output_filename}")
        
        # Display summary
        print("\nSummary:")
        print(f"Total products processed: {len(df)}")
        print(f"Products with Amazon results: {len(df[df['Amazon_URL'] != 'No results found on Amazon.'])}")
        print(f"Products with Nykaa results: {len(df[df['Nykaa_URL'] != 'No results found on Nykaa.'])}")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return None

def main():
    input_file = "D:\Import_Analysis\product_list.csv"
    output_file = "D:\Import_Analysis\product_links.csv"
    
    df = process_products(input_file, output_file)
    if df is not None:
        # Display first few results
        print("\nFirst few results:")
        print(df.head())

if __name__ == "__main__":
    main()