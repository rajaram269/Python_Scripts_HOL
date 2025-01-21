import tkinter as tk
from tkinter import filedialog, messagebox
import os
from openpyxl import load_workbook
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import json
import time

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
        return "NA"

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
        return "NA"

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
            return "NA"
    except Exception as e:
        print(f"Error fetching Myntra price: {e}")
        return "NA"

def fetch_zepto_price(url):
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
            price_element = soup.find("span", itemprop="price")
            return price_element['content'] if price_element else "NA"
        except AttributeError:
            return "NA"
    except Exception as e:
        print(f"Error fetching Zepto price: {e}")
        return "NA"

def fetch_faceshop_price(url):
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
            price = soup.find("span", class_="price-item price-item--sale").get_text().strip().replace('₹', '').replace(',', '')
        except AttributeError:
            price = "NA"

        return price
    except Exception as e:
        print(f"Error fetching Faceshop price: {e}")
        return "NA"

def identify_sales_channel(url):
    if not isinstance(url, str):
        return 'Unknown'

    channels = {
        'nykaa': 'Nykaa',
        'amazon': 'Amazon',
        'myntra': 'Myntra',
        'flipkart': 'Flipkart',
        'zepto': 'Zepto',
        'faceshop': 'Faceshop'
    }
    for keyword, channel in channels.items():
        if re.search(keyword, url, re.IGNORECASE):
            return channel
    return 'NA'

def fetch_price(channel, url):
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

def process_urls(input_csv, output_excel):
    df = pd.read_csv(input_csv)
    df['Channel wise URL'] = df['Channel wise URL'].fillna('')  
    df['Sales Channel'] = df['Channel wise URL'].apply(identify_sales_channel)
    df['Price INR'] = df.apply(lambda row: fetch_price(row['Sales Channel'], row['Channel wise URL']), axis=1)

    total_rows = len(df)
    for index, row in df.iterrows():
        df.at[index, 'Price INR'] = fetch_price(row['Sales Channel'], row['Channel wise URL'])
        progress_label.config(text=f"Processing row {index + 1} of {total_rows}")
        app.update_idletasks()
        time.sleep(0.1)  # Allow UI to update smoothly

    df['Price INR'] = (
        df['Price INR']
        .astype(str)
        .str.replace(r'[^\d.]', '', regex=True)
        .replace('', '0')
    )
    df['Price INR'] = pd.to_numeric(df['Price INR'], errors='coerce').fillna(0)
    df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if os.path.exists(output_excel):
        existing_data = pd.read_excel(output_excel)
        combined_data = pd.concat([existing_data, df], ignore_index=True)
    else:
        combined_data = df

    with pd.ExcelWriter(output_excel, engine='openpyxl', mode='w') as writer:
        combined_data.to_excel(writer, index=False)

    return output_excel

def select_csv_file():
    file_path = filedialog.askopenfilename(
        title="Select CSV File",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )
    if file_path:
        input_file_var.set(file_path)

def select_output_file():
    file_path = filedialog.askopenfilename(
        title="Select Existing Excel File to Append",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )
    if file_path:
        output_file_var.set(file_path)


def generate_output():
    input_csv = input_file_var.get()
    output_excel = output_file_var.get()

    if not input_csv:
        messagebox.showerror("Error", "Please select an input CSV file.")
        return
    if not output_excel:
        messagebox.showerror("Error", "Please select an existing Excel file to append data.")
        return

    try:
        process_urls(input_csv, output_excel)
        messagebox.showinfo("Success", f"Data appended and saved to {output_excel}")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

# Create the Tkinter application
app = tk.Tk()
app.title("Price Scraper Tool")
app.geometry("400x375")

# Input file selection
input_file_var = tk.StringVar()
output_file_var = tk.StringVar()

tk.Label(app, text="Input CSV File:").pack(pady=5)
input_file_entry = tk.Entry(app, textvariable=input_file_var, width=50)
input_file_entry.pack(pady=5)
select_input_button = tk.Button(app, text="Browse", command=lambda: input_file_var.set(filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("All files", "*.*")], title="Select CSV File")))
select_input_button.pack(pady=5)

tk.Label(app, text="Existing Excel File to Append:").pack(pady=5)
output_file_entry = tk.Entry(app, textvariable=output_file_var, width=50)
output_file_entry.pack(pady=5)
select_output_button = tk.Button(app, text="Browse", command=select_output_file)
select_output_button.pack(pady=5)

tk.Label(app, text="Progress:").pack(pady=5)
progress_label = tk.Label(app, text="Waiting to start...", font=("Arial", 10))
progress_label.pack(pady=5)

# Generate button
generate_button = tk.Button(app, text="Generate Output", command=generate_output)
generate_button.pack(pady=20)

# Run the application
app.mainloop()
