import requests

# API endpoint
url = "https://login.microsoftonline.com/2cc477df-31d6-4498-bbeb-0f68fa05821f/oauth2/v2.0/token"

# Headers
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    #"Cookie": "fpc=AsYWQDcsZc9Ckv2oTN8wWOFdIhGMAgAAAA7u8d4OAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd"
}

# Form data
data = {
    "grant_type": "client_credentials",
    "client_id": "65b18248-0db4-4287-8b7e-54685dd48a52",
    "scope": "https://api.businesscentral.dynamics.com/.default",
    "client_secret": "NVD8Q~dARH.Nr6Jj7eOtcx8Dt0j.HIcBDl41Page"
}

# Make the GET request
response = requests.post(url, headers=headers, data=data)

# Print the response
print("Response Status Code:", response.status_code)
try:
    print("Response JSON:", response.json())  # If response is in JSON format
except ValueError:
    print("Response Text:", response.text)  # If response is not JSON
