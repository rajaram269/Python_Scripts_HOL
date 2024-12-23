import fitz  # PyMuPDF
import pandas as pd
import openai
import csv
import json
import os
import camelot

# OpenAI API key
openai.api_key = "sk-L50sGDoqWwCHRL-uBgAzd3_nogeUS6XxOsmD18Z_rPT3BlbkFJbEMi5pkxXfAiv7o9NiCilyMs4AeUpLvgc6x9vH4eIA"

# PDF file path
file_path = "D:/NykaaPO/dmart.pdf"

# Function to extract tables from PDF and convert to CSV
def extract_tables_from_pdf(file_path):
    try:
        tables = camelot.read_pdf(file_path, pages='1-end')
        final_table = []
        for table in tables:
            df = table.df
            final_table.append(df)
        merged_df = pd.concat(final_table, ignore_index=True)
        merged_df.to_csv('extracted_table.csv', index=False)
        print("Table data extracted and saved as CSV.")
    except Exception as e:
        print(f"Error extracting tables: {e}")

# Function to extract images from PDF using PyMuPDF and send to OpenAI
def process_pdf_with_openai(file_path):
    try:
        doc = fitz.open(file_path)
        for page_num in range(len(doc)):
            page = doc[page_num]
            image_list = page.get_images(full=True)

            if not image_list:
                print(f"No images found on page {page_num + 1}.")
                continue

            for img_index, img in enumerate(image_list):
                xref = img[0]
                base_image = doc.extract_image(xref)
                image_bytes = base_image["image"]

                # Save the image temporarily
                temp_image_path = f"temp_page_{page_num + 1}_img_{img_index + 1}.jpg"
                with open(temp_image_path, "wb") as image_file:
                    image_file.write(image_bytes)

                # Create prompt for OpenAI API
                prompt = (
                    "You are a data analyst. Process the following image input. "
                    "Extract PO line item details. Create the output intelligently. "
                    "Extract all line items. Don't exclude any item. Format of the item list is "
                    "EAN, Vendor Sku, SKU Name, HSN, Qty, MRP, Unit Price, Taxable Value, "
                    "CGST Rate, CGST Amount, SGST Rate, SGST Amount, IGST Rate, IGST Amount, Total. "
                    "Provided data is unstructured, so collate the data logically.\n"
                )

                # API call
                with open(temp_image_path, "rb") as image_file:
                    image_bytes = image_file.read()

                response = openai.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        
                        {"role": "system", "content": "You are an expert in processing and analyzing image-based data."},
                        {"role": "user", 
                         "content": [
            {
                "type":"text",
                "text":prompt},
           {          
               "type": "image_url",
               "image_url": {
                "url": "https://i.postimg.cc/vBK4VsDM/Nykaa3.png",
           }, },
  ],},
                    ],
                    files=[{"name": "image.jpg", "data": image_bytes}],
                )

                # Parse response
                response_data = json.loads(response['choices'][0]['message']['content'])
                items = response_data['items']

                # Write to CSV
                with open('items.csv', 'w', newline='') as csvfile:
                    fieldnames = items[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(items)

                # Clean up temporary files
                os.remove(temp_image_path)
    except Exception as e:
        print(f"Error processing PDF: {e}")

# Main execution
extract_tables_from_pdf(file_path)
process_pdf_with_openai(file_path)



