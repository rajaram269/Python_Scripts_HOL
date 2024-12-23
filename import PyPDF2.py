
import camelot
import cv2
import ghostscript
import pandas as pd
import openai
import csv
import json
openai.api_key = "sk-L50sGDoqWwCHRL-uBgAzd3_nogeUS6XxOsmD18Z_rPT3BlbkFJbEMi5pkxXfAiv7o9NiCilyMs4AeUpLvgc6x9vH4eIA"
file_path = "D:/NykaaPO/dmart.pdf"
image_path = "https://i.postimg.cc/vBK4VsDM/Nykaa3.png"
tables =camelot.read_pdf(file_path, pages='1-end')
#pdf_text = read_pdf_file(file_path)
final_table = []
for table in tables:
    df=table.df
    final_table.append(df)
print(final_table)
merged_df=pd.concat(final_table,ignore_index=True)
#pdf_text[0].to_csv('D:/Sample/test.csv', sep='\t', encoding='utf-8', index=False)
#tables.export('D:/Sample/foo.csv', f='csv', compress=True)
#merged_df.to_csv('D:/Sample/fo2o.csv',index=False)
print("-----------------------")
print("-----------------------")
# Create a prompt for the generative AI model
#prompt = f"You are a data analyst. Process the following PDF text:\n\n{merged_df}. Extract PO line item details. Use Indian tax laws to cleanup the errors wherever necessary. Also validate the tax rate and values using standard tax rates in India. Create the output intelligently. Extract all line items. Don't exclude any item. Format of the item list is EAN, Vendor Sku, SKU Name, HSN, Qty, MRP, Unit Price, Taxable Value, CGST Rate, CGST Amount, SGST Rate, SGST Amount, IGST Rate, IGST Amount, Total. Provided data is unstructured so don't rely on the labels, collate the data logically. Use Python libraries to clean or better process the data and bind the logic. Here is the logic to bind every time. apply regex if needed EAN is numeric code of 13 or 14 digits.HSN Code is numeric of 8 digits. SKU code can be text or numeric typically available near EAN code it will be more than 5 charecters. description is a string it is more than 10 words, will contain a product name ensure to pick complete name. Qty is numeric with no decimal points.Unit Price is a numeric with decimals. look for the logic (Unit Price = Total Value/Qty) MRP is always greater than or equal to Unit Price. If MRP is not available then MRP is equal to unit price. Total value = (Qty * Unit Price). GST rate is typically a  numeric may contain symbol, sometimes may contain decimals, if there are two tax items then IGST is not applicable. If IGST applicable then IGST AMount = (Total Value* GST Rate). CGST and SGST rate and Amounts are zero. If IGST is not applicable then CGST Rate=SGST Rate. CGST Amount = (Total Value* CGST Rate). SGST Amount = (Total Value* SGST Rate) apply. Strictly follow this logic and reverify and readjust the output before giving output"

#client = openai()
prompt = f"You are a data analyst. Process the following image input attached. Extract PO line item details.Create the output intelligently. Extract all line items. Don't exclude any item. Format of the item list is EAN, Vendor Sku, SKU Name, HSN, Qty, MRP, Unit Price, Taxable Value, CGST Rate, CGST Amount, SGST Rate, SGST Amount, IGST Rate, IGST Amount, Total. Provided data is unstructured so don't rely on the labels, collate the data logically. "

completion = openai.chat.completions.create(
  model="gpt-4o",
  messages=[
    {
        "role": "user", 
        "content": [
            {
                "type":"text",
                "text":prompt},
           {          
               "type": "image_url",
               "image_url": {
                "url": "https://i.postimg.cc/vBK4VsDM/Nykaa3.png",
           }, },
  ],
    }
  ],
  temperature=1,
  max_tokens=2048,
  top_p=1,
  #frequency_penalty=0,
  #presence_penalty=0,
  response_format={
    "type": "json_schema",
   "json_schema": {
    "name": "purchase_order",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "description": "List of items included in the purchase order.",
                "items": {
                    "type": "object",
                    "properties": {
                        "EAN": {
                            "type": "string",
                            "description": "Unique identifier for the item."
                        },
                        "description": {
                            "type": "string",
                            "description": "Description of the item."
                        },
                        "Qty": {
                            "type": "number",
                            "description": "Number of units purchased."
                        },
                        "Unit Price": {
                            "type": "number",
                            "description": "Price per unit of the item."
                        },
                        "CGST Amount": {
                            "type": "number",
                            "description": "CGST of the item."
                        },
                        "SGST Amount": {
                            "type": "number",
                            "description": "SGST of the item."
                        },
                        "IGST Amount": {
                            "type": "number",
                            "description": "IGST of the item."
                        },
                        "Total Amount": {
                            "type": "number",
                            "description": "Total of the item."
                        }
                    },
                    "required": [
                        "EAN",
                        "description",
                        "Qty",
                        "Unit Price",
                        "CGST Amount",
                        "SGST Amount",
                        "IGST Amount",
                        "Total Amount"
                    ],
                    "additionalProperties": False
                }
            }
        },
        "required": ["items"],
        "additionalProperties": False
    }
}
  }
)
print("-----------------------")
print("-----------------------")
print(completion.choices[0].message.content)
data = json.loads(completion.choices[0].message.content)
items = data['items']
# Write the data to a CSV file
with open('items.csv', 'w', newline='') as csvfile:
    fieldnames = items[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(items)