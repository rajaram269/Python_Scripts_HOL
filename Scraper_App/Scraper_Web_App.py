import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import io
import time
import json
import lxml
import html.parser

# Define price-fetching functions
def fetch_nykaa_price(url):
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
            price = soup.find("span", class_="css-1jczs19").get_text().strip().replace('₹', '').replace(',', '')
        except AttributeError:
            price = "NA"

        return price
    except Exception as e:
        print(f"Error fetching Nykaa price: {e}")
        return e

def fetch_amazon_price(url):
    HEADERS = {
        'User-Agent': UserAgent().random,
        'Accept-Language': 'en-US, en;q=0.5'
    }
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url} (Status Code: {response.status_code})")
            return "NA"

        soup = BeautifulSoup(response.content, "lxml")
        if "captcha" in soup.text.lower():
            print("Captcha detected. Unable to fetch price.")
            return "NA"

        try:
            price = soup.find("span", attrs={'id': 'priceblock_ourprice'}).get_text().strip().replace(',', '').replace('₹', '')
        except AttributeError:
            try:
                price = soup.find("span", attrs={'id': 'priceblock_dealprice'}).get_text().strip().replace(',', '').replace('₹', '')
            except AttributeError:
                try:
                    price = soup.select_one("span.a-price span.a-offscreen").get_text().strip().replace(',', '').replace('₹', '')
                except AttributeError:
                    price = "NA"

        return price
    except Exception as e:
        print(f"Error fetching Amazon price: {e}")
        return e

def fetch_flipkart_price(url):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US, en;q=0.5'
    }

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to fetch URL: {url} (Status Code: {response.status_code})")
            return "NA"

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            price = soup.find("div", class_="Nx9bqj CxhGGd").get_text().strip().replace('₹', '').replace(',', '')
        except AttributeError:
            try:
                price = soup.find("div", class_="_30jeq3 _16Jk6d").get_text().strip().replace('₹', '').replace(',', '')
            except AttributeError:
                try:
                    price = soup.select_one("._25b18c ._30jeq3").get_text().strip().replace('₹', '').replace(',', '')
                except AttributeError:
                    price = "NA"

        return price

    except requests.exceptions.RequestException as e:
        print(f"Request error for URL {url}: {e}")
        return e

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

def identify_sales_channel(url):
    if not isinstance(url, str):
        return 'Unknown'

    channels = {
        'nykaa': 'Nykaa',
        'amazon': 'Amazon',
        'myntra': 'Myntra',
        #'flipkart': 'Flipkart',
        'zepto': 'Zepto',
        'faceshop': 'Faceshop'
    }
    for keyword, channel in channels.items():
        if re.search(keyword, url, re.IGNORECASE):
            return channel
    return 'NA'

def fetch_price(channel, url):
    log=[]
    if channel == 'Amazon':
        return fetch_amazon_price(url)
    elif channel == 'Nykaa':
        return fetch_nykaa_price(url)
    elif channel == 'Flipkart':
        time.sleep(3)
        return fetch_flipkart_price(url)
    elif channel == 'Myntra':
        return fetch_myntra_price(url)
    elif channel == 'Zepto':
        return fetch_zepto_price(url)
    elif channel == 'Faceshop':
        return fetch_faceshop_price(url)
    else:
        return "NA"

# Streamlit UI components
st.title("Price Scraper Tool")
st.markdown("Upload a CSV file containing product URLs, and the tool will fetch prices from supported channels.")

# File upload
uploaded_file = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_file:
    try:
        input_file = pd.read_csv(uploaded_file)
        st.success("File uploaded successfully!")

        if 'Channel wise URL' not in input_file.columns:
            st.error("CSV must contain a column named 'Channel wise URL'.")
        else:
            # Process file
            input_file['Sales Channel'] = input_file['Channel wise URL'].apply(identify_sales_channel)
            st.write("Sales Channels Identified:", input_file)

            if st.button("Fetch Prices"):
                with st.spinner("Fetching prices..."):
                    input_file['Price INR'] = input_file.apply(
                        lambda row: fetch_price(row['Sales Channel'], row['Channel wise URL']), axis=1
                    )
                    input_file['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.success("Prices fetched successfully!")
                
                # Display results
                #input_file['Price INR'] = pd.to_numeric(input_file['Price INR'], errors='coerce').fillna(0)
                #input_file_1 = input_file[input_file['Price INR']>0]
                input_file_1=input_file
                st.write("Results:", input_file_1)

                # Download button
                csv = input_file_1.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name='price_data.csv',
                    mime='text/csv'
                )

                # Plot data
                st.markdown("### Price Trends")
                if not input_file_1.empty:
                    input_file['Price INR'] = pd.to_numeric(input_file['Price INR'], errors='coerce').fillna(0)
                    chart = alt.Chart(input_file).mark_line().encode(
                        x='SKU CODE:T',
                        y='Price INR:Q',
                        color='Sales Channel:N',
                        tooltip=['Sales Channel', 'Price INR', 'Timestamp']
                    ).properties(width=600, height=400, title='Price Trends Across Channels')
                    st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error processing file: {e}")
