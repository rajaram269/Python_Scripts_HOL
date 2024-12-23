import requests
import json

# URL of the Firestore API
url = "https://firestore.googleapis.com/v1/projects/blinkitpo/databases/(default)/documents/orders?key=AIzaSyBe6538fXAXjEKNYIPFY3ZcQOWfTaAQ9y8"

# Payload to be sent
payload = {
  "receiver_code": 11935,
  "item_data": [
    {
      "line_number": 0,
      "units_ordered": 2400,
      "landing_rate": "32.56",
      "case_size": 240,
      "cgst_value": "2.50",
      "item_id": 10016623,
      "name": "Name of Item 0",
      "sgst_value": "2.50",
      "mrp": "42.00",
      "upc": "8901774002349",
      "cess_value": "0.00",
      "igst_value": "",
      "uom": "100 g",
      "cost_price": "31.01"
    },
    {
      "line_number": 1,
      "units_ordered": 240,
      "landing_rate": "29.46",
      "case_size": 240,
      "cgst_value": "2.50",
      "item_id": 10008111,
      "name": "Name of Item 1",
      "sgst_value": "2.50",
      "mrp": "38.00",
      "upc": "8901774002332",
      "cess_value": "0.00",
      "igst_value": "",
      "uom": "100 g",
      "cost_price": "28.06"
    }
  ],
  "event_name": "PO_CREATION",
  "financial_details": {
    "gst_tin": "27AADCH7038R1ZX",
    "purchasing_entity": "HANDS ON TRADES PRIVATE LIMITED"
  },
  "event_message": "PO_CREATION",
  "purchase_order_details": {
    "po_expiry_date": "2024-05-13",
    "purchase_order_number": "1679310079700",
    "issue_date": "2024-05-06"
  },
  "grofers_delivery_details": {
    "grofers_outlet_id": 4162
  }
}
# Headers for the request
headers = {
    "Content-Type": "application/json",
   # "Authorization": "Bearer YOUR_ACCESS_TOKEN"  # Replace with your Firebase Auth Token
}

# Make the POST request
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Print the response
if response.status_code == 200:
    print("Document created successfully:")
    print(response.json())
else:
    print(f"Failed to create document: {response.status_code}")
    print(response.json())
