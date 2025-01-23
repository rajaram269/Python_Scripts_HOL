import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import re
import time
import os
import json
from fake_useragent import UserAgent

# Configure Selenium options
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Get IST timestamp
def get_ist_timestamp():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# Setup requests session with retries
session = requests.Session()
retry = requests.adapters.HTTPAdapter(max_retries=3)
session.mount('https://', retry)
session.mount('http://', retry)

def fetch_price_with_retry(url, headers):
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        st.warning(f"Request failed for {url}: {str(e)}")
        return None

# Updated scraping functions
def fetch_flipkart_price(url):
    try:
        driver = get_driver()
        driver.get(url)
        time.sleep(2)
        
        # Handle popups
        try:
            driver.find_element("xpath", "//button[contains(.,'✕')]").click()
            time.sleep(1)
        except:
            pass
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        
        price_element = soup.find("div", class_="_30jeq3") or soup.find("div", class_="_16Jk6d")
        if price_element:
            return re.sub(r'[^\d.]', '', price_element.text.strip())
        return "NA"
    except Exception as e:
        print(f"Flipkart Error: {e}")
        return "NA"

def fetch_myntra_price(url):
    try:
        driver = get_driver()
        driver.get(url)
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        
        price_element = soup.find("span", class_="pdp-price") or soup.find("span", class_="product-discountedPrice")
        if price_element:
            return re.sub(r'[^\d.]', '', price_element.text.strip())
        return "NA"
    except Exception as e:
        print(f"Myntra Error: {e}")
        return "NA"

def fetch_amazon_price(url):
    headers = {'User-Agent': UserAgent().random}
    response = fetch_price_with_retry(url, headers)
    if response:
        soup = BeautifulSoup(response.content, 'lxml')
        if "captcha" in soup.text.lower():
            return "Captcha"
        price_element = soup.select_one('#priceblock_ourprice, #priceblock_dealprice, .a-price .a-offscreen')
        return re.sub(r'[^\d.]', '', price_element.text.strip()) if price_element else "NA"
    return "NA"

def fetch_nykaa_price(url):
    headers = {'User-Agent': UserAgent().random}
    response = fetch_price_with_retry(url, headers)
    if response:
        soup = BeautifulSoup(response.content, 'html.parser')
        price_element = soup.find("span", class_="css-1jczs19")
        return re.sub(r'[^\d.]', '', price_element.text.strip()) if price_element else "NA"
    return "NA"

def identify_sales_channel(url):
    domain_patterns = {
        'amazon': 'Amazon',
        'nykaa': 'Nykaa',
        'flipkart': 'Flipkart',
        'myntra': 'Myntra',
        'zepto': 'Zepto',
        'faceshop': 'Faceshop'
    }
    for pattern, name in domain_patterns.items():
        if pattern in url.lower():
            return name
    return 'Other'

def fetch_price(channel, url):
    if channel == 'Amazon': return fetch_amazon_price(url)
    elif channel == 'Nykaa': return fetch_nykaa_price(url)
    elif channel == 'Flipkart': return fetch_flipkart_price(url)
    elif channel == 'Myntra': return fetch_myntra_price(url)
    else: return "NA"

def clean_data(df):
    df['Price INR'] = pd.to_numeric(df['Price INR'], errors='coerce')
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df.dropna(subset=['Price INR'])

def create_price_chart(df):
    return alt.Chart(df).mark_line(point=True).encode(
        x='Timestamp:T',
        y='Price INR:Q',
        color='Sales Channel:N',
        tooltip=['SKU CODE', 'Price INR', 'Sales Channel', 'Timestamp']
    ).properties(
        width=800,
        height=400,
        title='Price Trend Analysis'
    ).interactive()

# Streamlit UI
st.set_page_config(page_title="Price Tracking Dashboard", layout="wide")
st.title("🛒 E-Commerce Price Tracker")
st.markdown("Track product prices across major Indian e-commerce platforms")

uploaded_file = st.file_uploader("Upload CSV with product URLs", type=["csv"])
save_path = "price_history.csv"

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    if 'URL' not in df.columns:
        st.error("CSV must contain 'URL' column")
    else:
        df['Sales Channel'] = df['URL'].apply(identify_sales_channel)
        
        if st.button("🔄 Fetch Current Prices"):
            with st.spinner("Scraping prices... This may take a few minutes"):
                df['Price INR'] = df.apply(
                    lambda row: fetch_price(row['Sales Channel'], row['URL']), 
                    axis=1
                )
                df['Timestamp'] = get_ist_timestamp()
                
                if os.path.exists(save_path):
                    history_df = pd.read_csv(save_path)
                    final_df = pd.concat([history_df, df], ignore_index=True)
                else:
                    final_df = df
                
                final_df.to_csv(save_path, index=False)
                st.success(f"Updated {len(df)} records!")
                
                st.dataframe(final_df.tail(10), use_container_width=True)
                st.download_button(
                    "💾 Download Full Dataset",
                    final_df.to_csv(index=False),
                    "price_history.csv",
                    "text/csv"
                )

        if os.path.exists(save_path):
            st.markdown("## 📈 Price Analytics")
            history_df = clean_data(pd.read_csv(save_path))
            
            col1, col2 = st.columns(2)
            with col1:
                selected_sku = st.selectbox("Select Product", options=history_df['SKU CODE'].unique())
            with col2:
                date_range = st.date_input("Select Date Range", 
                    value=[history_df['Timestamp'].min(), history_df['Timestamp'].max()],
                    min_value=history_df['Timestamp'].min(),
                    max_value=history_df['Timestamp'].max()
                )
            
            filtered_df = history_df[
                (history_df['SKU CODE'] == selected_sku) &
                (history_df['Timestamp'].between(*date_range))
            ]
            
            if not filtered_df.empty:
                st.altair_chart(create_price_chart(filtered_df), use_container_width=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Lowest Price", 
                        value=f"₹{filtered_df['Price INR'].min():.2f}",
                        delta=f"{filtered_df['Sales Channel'].value_counts().idxmax()}"
                    )
                with col2:
                    st.metric("Average Price", 
                        value=f"₹{filtered_df['Price INR'].mean():.2f}",
                        delta_color="off"
                    )
                with col3:
                    st.metric("Current Price", 
                        value=f"₹{filtered_df.iloc[-1]['Price INR']:.2f}",
                        delta=f"{filtered_df.iloc[-1]['Sales Channel']}"
                    )
            else:
                st.warning("No data available for selected filters")