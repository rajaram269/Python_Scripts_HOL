import requests
import time
import pandas as pd
from io import StringIO
from datetime import datetime

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

# Step 5: Fetch the list of reports
report_list_url = "https://holistique.vineretail.com/eRetailWeb/jsonPendingSearch?REQ_SEARCH_FLAG=true"
timestamp = str(int(time.time() * 1000))  # Generate current timestamp

report_list_headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9,te-IN;q=0.8,te;q=0.7,hi;q=0.6",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://holistique.vineretail.com",
    "referer": "https://holistique.vineretail.com/eRetailWeb/pendingDisplay",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "priority": "u=1, i",
    "dnt": "1",
}

report_list_data = {
    "_search": "false",
    "nd": timestamp,
    "rows": "30",
    "page": "1",
    "sidx": "",
    "sord": "asc",
}

report_list_response = session.post(report_list_url, headers=report_list_headers, data=report_list_data)

if report_list_response.status_code != 200:
    print(f"Failed to fetch report list. Status code: {report_list_response.status_code}")
    exit()

# Parse the JSON response
try:
    report_data = report_list_response.json()
    reports = report_data['dto']['rowList']
except (KeyError, json.JSONDecodeError) as e:
    print("Error parsing report list response:", e)
    exit()

# Function to parse date strings
def parse_date(date_str):
    return datetime.strptime(date_str, '%d/%m/%Y %I:%M %p')

# Sort reports by createDateText in descending order to get the latest first
sorted_reports = sorted(reports, key=lambda x: parse_date(x['createDateText']), reverse=True)

# Get the last 4 reports
last_four_reports = sorted_reports[:4]

# Step 6: Download each report and merge them
data_frames = []

for report in last_four_reports:
    uuId = report['uuId']
    report_id = report['reportID']
    download_url = f"https://holistique.vineretail.com/eRetailWeb/downloadReportTemplate?uuId={uuId}&reportID={report_id}"
    
    # Download the report
    response = session.get(download_url)
    if response.status_code == 200:
        # Read CSV data into DataFrame
        df = pd.read_csv(StringIO(response.text))
        data_frames.append(df)
        print(f"Successfully downloaded report {report_id}")
    else:
        print(f"Failed to download report {report_id}. Status code: {response.status_code}")

# Merge all DataFrames
if data_frames:
    merged_df = pd.concat(data_frames, ignore_index=True)
    output_file = "D:/merged_reports.csv"
    merged_df.to_csv(output_file, index=False)
    print(f"Merged report saved to {output_file}")
else:
    print("No reports were downloaded successfully.")