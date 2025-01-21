import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import random
import time
from fake_useragent import UserAgent
import logging

def search_amazon(product_name, max_retries=3, delay_between_retries=5):
    """
    Search Amazon for a product and return the URL of the closest matching result.
    Includes retry logic and improved error handling.
    """
    base_url = "https://www.amazon.in/s?k="
    search_url = base_url + "+".join(product_name.split())
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Initialize UserAgent
    ua = UserAgent()
    
    def get_random_headers():
        """Generate random headers for each request"""
        return {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        }
    
    session = requests.Session()
    
    for attempt in range(max_retries):
        try:
            # Add a random delay between requests
            if attempt > 0:
                sleep_time = delay_between_retries + random.uniform(1, 3)
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            
            headers = get_random_headers()
            logger.info(f"Attempting request {attempt + 1}/{max_retries}")
            
            response = session.get(
                search_url,
                headers=headers,
                timeout=10
            )
            
            response.raise_for_status()
            print(response)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try multiple selector patterns as Amazon's HTML structure might vary
            selectors = [
                'div[data-component-type="s-search-result"] h2 a.a-link-normal',
                'div.s-result-item h2 a.a-link-normal',
                'div.s-result-item a.a-link-normal.s-no-outline'
            ]
            
            results = []
            for selector in selectors:
                results.extend(soup.select(selector))
                #print(results)
                if results:
                    break
            
            if results:
                product_links = []
                
                for link in results[:20]:
                    title = link.get_text().strip()
                    #print(title)
                    url = link.get('href')
                    
                    if title and url:
                        if not url.startswith('http'):
                            url = f"https://www.amazon.in{url}"
                        product_links.append((title, url))
                
                if product_links:
                    # Find matches with high similarity
                    partial_matches = [
                        link for link in product_links 
                        if SequenceMatcher(None, product_name.lower(), link[0].lower()).ratio() >= 0.9
                    ]
                    
                    if partial_matches:
                        logger.info("Found exact match")
                        return partial_matches[0][1]
                    
                    # Find closest match if no high-similarity matches found
                    closest_match = max(
                        product_links,
                        key=lambda x: SequenceMatcher(None, product_name.lower(), x[0].lower()).ratio()
                    )
                    logger.info("Found closest match")
                    return closest_match[1]
            
            logger.warning("No results found in the current attempt")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            if attempt == max_retries - 1:
                raise Exception(f"Failed to search Amazon after {max_retries} attempts: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            if attempt == max_retries - 1:
                raise Exception(f"Unexpected error after {max_retries} attempts: {str(e)}")
    
    return None

def main():
    product_name = input("Enter the product name: ")
 
    print("\nSearching for product...\n")
 
    amazon_result = search_amazon(product_name)
    print(f"Amazon: {amazon_result}")
 
    #nykaa_result = search_nykaa(product_name)
    #print(f"Nykaa: {nykaa_result}")
 
    #flipkart_result = search_flipkart(product_name)
    #print(f"Flipkart: {flipkart_result}")
 
if __name__ == "__main__":
    main()