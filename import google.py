import PyPDF2
import os
import openai
import csv
import json
openai.api_key = "sk-L50sGDoqWwCHRL-uBgAzd3_nogeUS6XxOsmD18Z_rPT3BlbkFJbEMi5pkxXfAiv7o9NiCilyMs4AeUpLvgc6x9vH4eIA"
def read_pdf_file(file_path):

    with open(file_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
        return text

# Example usage:
file_path = "D:/NykaaPO/1.pdf"
pdf_text = read_pdf_file(file_path)
print(pdf_text)
print(1)
# Create a prompt for the generative AI model
# prompt = f"You are a data analyst. Process the following PDF text:\n\n{pdf_text}. Extract PO line item details in Json format. Extract all line items. Don't exclude any item. Format of the item list is SNo, Item Code, EAN, Vendor Sku, SKU Name, HSN, Qty, MRP, Unit Price, Taxable Value, CGST Rate, CGST Amount, SGST Rate, SGST Amount, IGST Rate, IGST Amount, Total"

# #client = openai()

# completion = openai.chat.completions.create(
#   model="gpt-4o",
#   messages=[
#     {"role": "user", "content": prompt}
#   ],
#   temperature=1,
#   max_tokens=2048,
#   top_p=1,
#   frequency_penalty=0,
#   presence_penalty=0,
#   response_format={
#     "type": "json_schema",
#    "json_schema": {
#     "name": "purchase_order",
#     "strict": True,
#     "schema": {
#         "type": "object",
#         "properties": {
#             "items": {
#                 "type": "array",
#                 "description": "List of items included in the purchase order.",
#                 "items": {
#                     "type": "object",
#                     "properties": {
#                         "EAN": {
#                             "type": "string",
#                             "description": "Unique identifier for the item."
#                         },
#                         "description": {
#                             "type": "string",
#                             "description": "Description of the item."
#                         },
#                         "Qty": {
#                             "type": "number",
#                             "description": "Number of units purchased."
#                         },
#                         "Unit Price": {
#                             "type": "number",
#                             "description": "Price per unit of the item."
#                         },
#                         "CGST Amount": {
#                             "type": "number",
#                             "description": "CGST of the item."
#                         },
#                         "SGST Amount": {
#                             "type": "number",
#                             "description": "SGST of the item."
#                         },
#                         "IGST Amount": {
#                             "type": "number",
#                             "description": "IGST of the item."
#                         },
#                         "Total Amount": {
#                             "type": "number",
#                             "description": "Total of the item."
#                         }
#                     },
#                     "required": [
#                         "EAN",
#                         "description",
#                         "Qty",
#                         "Unit Price",
#                         "CGST Amount",
#                         "SGST Amount",
#                         "IGST Amount",
#                         "Total Amount"
#                     ],
#                     "additionalProperties": False
#                 }
#             }
#         },
#         "required": ["items"],
#         "additionalProperties": False
#     }
# }
#   }
# )
# print("-----------------------")
# print("-----------------------")
# print(completion.choices[0].message.content)
# data = completion.choices[0].message.content
# with open("purchase_order.csv", "w", newline="") as csvfile:
#   # Create a CSV writer object
#   writer = csv.writer(csvfile)

#   # Write header row
#   header = ["EAN", "Description", "Qty", "Unit Price", "CGST Amount", "SGST Amount", "IGST Amount", "Total Amount"]
#   writer.writerow(header)

#   # Write data rows
#   for item in data["items"]:
#     row = [
#       item["EAN"],
#       item["description"],
#       item["Qty"],
#       item["Unit Price"],
#       item["CGST Amount"],
#       item["SGST Amount"],
#       item["IGST Amount"],
#       item["Total Amount"],
#     ]
#     writer.writerow(row)

# print("CSV file 'purchase_order.csv' created successfully!")

