import requests
import time
import pandas as pd
import json
from io import StringIO

# Step 1: Create a session to persist cookies
session = requests.Session()

# Step 2: Fetch initial cookies by visiting the login page (critical for JSESSIONID)
login_page_url = "https://holistique.vineretail.com/eRetailWeb/eRetailLogin.action?popup=true"
login_page_response = session.get(login_page_url)

# Extract initial cookies (for debugging)
initial_cookies = session.cookies.get_dict()
print("Initial cookies:", initial_cookies)

# Step 3: Log out all existing sessions if possible
logout_url = "https://holistique.vineretail.com/eRetailWeb/eRetailLogoff"
logout_response = session.get(logout_url)
print(logout_response)

if logout_response.status_code == 200:
    print("Logged out of all sessions successfully.")
else:
    print(f"Logout failed. Status code: {logout_response.status_code}")

# Step 4: Login with credentials (retain initial cookies)
login_url = "https://holistique.vineretail.com/eRetailWeb/doLogin"
login_data = {
    "browserId": "chrome",
    "forcefulFlag": "",
    "prepaidFlag": "",
    "menuInnerTabWidth": "1530",
    "menuInnerTabHeight": "211",
    "userOrgId": "",
    "userName": "rajaram.v",
    "googleId": "",
    "microsoftId": "",
    "password": "ZTE0NjdmNGJlOTAwYTUzOTkzMTdmMjU3MmYxMzFiYjY6OmZlOTZmMzBjMzQwNTRlYmI2NmMyYjIyN2Y0ZDg2OTFlOjo3SkZ5ODVwczQ3bUQwMUJHei9KVGpRPT0=",
    "otp": ""
}

login_headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9,te-IN;q=0.8,te;q=0.7,hi;q=0.6",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://holistique.vineretail.com",
    "referer": "https://holistique.vineretail.com/eRetailWeb/eRetailLogin.action?popup=true",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "dnt": "1",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
}

login_response = session.post(login_url, headers=login_headers, data=login_data)

# Check login success
if login_response.status_code != 200:
    print(f"Login failed. Status code: {login_response.status_code}")
    exit()
else:
    print("Login successful. Post-login cookies:", session.cookies.get_dict())
    print("; ".join([f"{k}={v}" for k, v in session.cookies.items()]))

# Step 5: Prepare inventory request with fresh timestamp and cookies
#inventory_url = "https://holistique.vineretail.com/eRetailWeb/jsonInventoryBySkuBinLotSearch"
inventory_url = "https://holistique.vineretail.com/eRetailWeb/downloadReportTemplate?uuId=e8b3a12457d4445a8cbd51e81cc6e3e0&reportID=141995"

""" inventory_data = {
    "_search": "true",
    "nd": str(int(time.time() * 1000)),  # Use current timestamp
    "rows": "20000",
    "page": "1",
    "sidx": "sku",
    "sord": "desc",
    "skuDesc": "",
    "skuCode": "",
    "mfgSkuCode": "",
    "lotCode": "",
    "lot01": "",
    "brandCode": "",
    "fromBinLot02Date": "",
    "toBinLot02Date": "",
    "fromBinLot03Date": "",
    "toBinLot03Date": "",
    "fromBinLot04Date": "",
    "toBinLot04Date": "",
    "lot05": "",
    "lot06": "",
    "lot07": "",
    "zoneCode": "-1",
    "siteCode": "-1",
    "binCode": "",
    "hierarchyCode": "",
    "sohQty": "",
    "freeQty": "",
    "clientId": "0",
    "boxIdByBinLot": "",
    "lpnStatusByBinLot": "false",
    "skuOrBarcode": "true",
    "REQ_SEARCH_FLAG": "true",
    "onHold": "0",
}"""
inventory_headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9,te-IN;q=0.8,te;q=0.7,hi;q=0.6",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://holistique.vineretail.com",
    "referer": "https://holistique.vineretail.com/eRetailWeb/pendingDisplay?uuId=2d48675b1e964c8a995524bb99e43beb",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "cookie": "; ".join([f"{k}={v}" for k, v in session.cookies.items()]),  # Explicit cookies
    "dnt": "1",
    "priority": "u=1, i",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# Step 6: Fetch inventory data
#inventory_response = session.post(
    #inventory_url, headers=inventory_headers, data=inventory_data
#)
inventory_response = requests.get(inventory_url, headers=inventory_headers)

# Check response
if inventory_response.status_code == 200:
    print("Inventory data fetched successfully!")
    #print(inventory_response.json())
    data_frame = pd.read_csv(StringIO(inventory_response.text))
    print(data_frame.head())
else:
    print(f"Failed to fetch inventory. Status code: {inventory_response.status_code}")
    print("Response:", inventory_response.text)


#if "gridModel" in inventory_response:
#inventory_response=inventory_response.json()
#grid_model_data = inventory_response["gridModel"]
    
# Convert to Pandas DataFrame
#df = pd.DataFrame(grid_model_data)
print(1)
    
# Save as CSV
output_file = "D:/inventory_data.csv"
data_frame.to_csv(output_file, index=False)
print(f"Data saved to {output_file}")