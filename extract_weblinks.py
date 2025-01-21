import requests
from bs4 import BeautifulSoup
from langchain.agents import initialize_agent, Tool
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI
from langchain.agents import AgentExecutor

# Base URLs for the websites
BASE_URLS = {
    'amazon': 'https://www.amazon.in/s?k=',
    'flipkart': 'https://www.flipkart.com/search?q=',
    'nykaa': 'https://www.nykaa.com/search/result/?q='
}

# Function to perform search and extract best matching product URL
def search_and_extract_url(product_name, brand_name):
    search_query = f"{product_name} {brand_name}"
    
    # Initialize a dictionary to store the results
    results = {}

    # Perform searches on each of the sites
    for site, base_url in BASE_URLS.items():
        response = requests.get(base_url + search_query)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract links and titles based on each site's structure
        if site == 'amazon':
            links = soup.find_all('a', {'class': 'a-link-normal'})
            for link in links:
                title = link.get('title', '').lower()
                if product_name.lower() in title and brand_name.lower() in title:
                    results[site] = "https://www.amazon.in" + link.get('href')
                    break
        elif site == 'flipkart':
            links = soup.find_all('a', {'class': '_1fQZEK'})
            for link in links:
                title = link.get('title', '').lower()
                if product_name.lower() in title and brand_name.lower() in title:
                    results[site] = "https://www.flipkart.com" + link.get('href')
                    break
        elif site == 'nykaa':
            links = soup.find_all('a', {'class': 'css-1s9w6n2'})
            for link in links:
                title = link.get('title', '').lower()
                if product_name.lower() in title and brand_name.lower() in title:
                    results[site] = "https://www.nykaa.com" + link.get('href')
                    break

    # Return the best matching result (or a default message if none found)
    return results if results else {"error": "No matching product URL found"}

# Example usage:
product_name = "advanced snail mucin"
brand_name = "Cosrx"
result = search_and_extract_url(product_name, brand_name)
print(result)
