import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import time
import os
import json

# Enhanced: Retry mechanism for requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Setup retry strategy for all requests
retry_strategy = Retry(
    total=3, 
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_price_with_retry(url, headers):
    try:
        response = session.get(url, headers=headers)
        if response.status_code == 200:
            return response
        else:
            st.warning(f"Request to {url} failed with status {response.status_code}.")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Network error for {url}: {e}")
        return None

# Define scraping functions for different channels
def fetch_nykaa_price(url):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US, en;q=0.5'
    }
    response = fetch_price_with_retry(url, headers)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        price = soup.find("span", class_="css-1jczs19")
        return price.get_text().strip().replace('₹', '').replace(',', '') if price else "NA"
    return "NA"

def fetch_amazon_price(url):
    headers = {
        'User-Agent': UserAgent().random,
        'Accept-Language': 'en-US, en;q=0.5'
    }
    response = fetch_price_with_retry(url, headers)
    if response:
        soup = BeautifulSoup(response.content, "lxml")
        if "captcha" in soup.text.lower():
            st.warning("Captcha detected on Amazon. Skipping.")
            return "Captcha"
        price = (soup.find("span", id='priceblock_ourprice') or 
                 soup.find("span", id='priceblock_dealprice') or 
                 soup.select_one("span.a-price span.a-offscreen"))
        return price.get_text().strip().replace(',', '').replace('₹', '') if price else "NA"
    return "NA"

def fetch_flipkart_price(url):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US, en;q=0.5'
    }
    response = fetch_price_with_retry(url, headers)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        if "captcha" in soup.text.lower():
            st.warning("Captcha detected on Flipkart. Skipping.")
            return "Captcha"
        price = (soup.find("div", class_="_30jeq3 _16Jk6d") or
                 soup.find("div", class_="_16Jk6d") or
                 soup.select_one("._25b18c ._30jeq3"))
        return price.get_text().strip().replace('₹', '').replace(',', '') if price else "NA"
    return "NA"

def fetch_myntra_price(url):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US, en;q=0.5'
    }
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url} (Status Code: {response.status_code})")
            return "NA"

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            script = soup.find("script", string=lambda t: t and "pdpData" in t)
            if script:
                json_text = script.string.strip()
                json_start = json_text.find("{")
                extracted_json = json_text[json_start:]
                json_data = json.loads(extracted_json)
                price_data = json_data.get("pdpData", {}).get("price", {})
                return price_data.get("discounted", "NA")
            else:
                return "NA"
        except Exception as e:
            print(f"Error fetching Myntra price: {e}")
            return e
    except Exception as e:
        print(f"Error fetching Myntra price: {e}")
        return e

def fetch_zepto_price(url):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US, en;q=0.5'
    }
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url} (Status Code: {response.status_code})")
            return e

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            price_element = soup.find("span", itemprop="price")
            return price_element['content'] if price_element else "NA"
        except AttributeError:
            return e
    except Exception as e:
        print(f"Error fetching Zepto price: {e}")
        return e

def fetch_faceshop_price(url):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US, en;q=0.5'
    }
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url} (Status Code: {response.status_code})")
            return e

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            price = soup.find("span", class_="price-item price-item--sale").get_text().strip().replace('₹', '').replace(',', '')
        except AttributeError:
            price = "NA"

        return price
    except Exception as e:
        print(f"Error fetching Faceshop price: {e}")
        return e

# Add more fetch functions for other platforms as needed...

def identify_sales_channel(url):
    channels = {
        'nykaa': 'Nykaa',
        'amazon': 'Amazon',
        'flipkart': 'Flipkart',
        'myntra': 'Myntra',
        'zepto': 'Zepto',
        'faceshop': 'Faceshop'
    }
    for keyword, channel in channels.items():
        if keyword in url.lower():
            return channel
    return 'Unknown'

def fetch_price(channel, url):
    if channel == 'Amazon':
        return fetch_amazon_price(url)
    elif channel == 'Nykaa':
        return fetch_nykaa_price(url)
    #elif channel == 'Flipkart':
        #return fetch_flipkart_price(url)
    elif channel == 'Myntra':
        return fetch_myntra_price(url)
    elif channel == 'Faceshop':
        return fetch_faceshop_price(url)
    elif channel == 'Zepto':
        return fetch_zepto_price(url)
    return "NA"

# Streamlit App
st.set_page_config(page_title="Price Scraper Tool", layout="wide")
st.title("E-Commerce Price Scraper Tool")
st.markdown("Upload a CSV file with URLs to scrape prices from supported platforms.")

uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
save_path = "scraped_prices.csv"

if uploaded_file:
    try:
        input_data = pd.read_csv(uploaded_file)

        if 'URL' not in input_data.columns:
            st.error("CSV must contain a column named 'URL'.")
        else:
            input_data['Sales Channel'] = input_data['URL'].apply(identify_sales_channel)

            if st.button("Fetch Prices"):
                with st.spinner("Fetching prices. Please wait..."):
                    input_data['Price INR'] = input_data.apply(
                        lambda row: fetch_price(row['Sales Channel'], row['URL']), axis=1
                    )
                    input_data['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                st.success("Price data fetched successfully!")

                # Append to CSV if exists
                if os.path.exists(save_path):
                    existing_data = pd.read_csv(save_path)
                    input_data = pd.concat([existing_data, input_data], ignore_index=True)

                input_data.to_csv(save_path, index=False)

                st.write("Updated Data:", input_data)
                st.download_button(
                    label="Download Updated CSV",
                    data=input_data.to_csv(index=False),
                    file_name='updated_prices.csv',
                    mime='text/csv'
                )

                # Visualization
                st.markdown("### SKU-wise Price Comparison Across Platforms")
                filtered_data = input_data[input_data['Price INR'] != "NA"]
                if not filtered_data.empty:
                    chart = alt.Chart(filtered_data).mark_bar().encode(
                        x='SKU CODE:O',
                        y='Price INR:Q',
                        color='Sales Channel:N',
                        tooltip=['Sales Channel', 'Price INR', 'SKU CODE']
                    ).properties(width=800, height=400, title='Price Comparison by SKU')
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.warning("No valid price data available for visualization.")

    except Exception as e:
        st.error(f"Error processing the file: {e}")
