import pandas as pd
import os
 
def clean_dataframe(df):
   # Step 1: Identify the header row (row with the maximum non-empty values)
    header_row_index = df.notna().sum(axis=1).idxmax()
 
    # Step 2: Set the headers
    df.columns = df.iloc[header_row_index]  # Use this row as the header
    df = df.iloc[header_row_index :].reset_index(drop=True)  # Keep rows after the header row
 
    # Step 3: Drop columns where all values are empty (if necessary)
    df = df.dropna(axis=1, how='all')
 
    # Step 4: Drop rows where all values are empty
    df = df.dropna(how='all').reset_index(drop=True)
 
   
    # Remove rows where a certain percentage of columns are NaN
    #threshold = 0.7  # 70% of columns must have a non-NaN value
    #df = df.dropna(thresh=int(len(df.columns) * (1 - threshold)))
   
    return df
 
 
 
def load_sku_mapping(sku_map_path):
    try:
        excel_file = pd.ExcelFile(sku_map_path)
        sheet_names = excel_file.sheet_names
        all_sheets_data = []
       
        for sheet_name in sheet_names:
            print(f"Processing SKU mapping sheet: {sheet_name}")
            df = pd.read_excel(sku_map_path, sheet_name=sheet_name)
            df = df.iloc[:, :3]
            df.columns = ['Channel_SKU', 'Master_SKU', 'SKU_Name']
            df['Channel'] = sheet_name
            all_sheets_data.append(df)
       
        combined_sku_mapping = pd.concat(all_sheets_data, ignore_index=True)
        print(f"Combined SKU mapping loaded successfully. Total rows: {len(combined_sku_mapping)}")
        combined_sku_mapping.to_csv('combined_sku_map.csv',
           index=False,           # Don't write row index
           sep=',',               # Use comma as separator (default)
           encoding='utf-8',      # Specify encoding
           header=True            # Write column names
 )
 
        return combined_sku_mapping
    except Exception as e:
        print(f"Error loading SKU mapping file: {e}")
        return None
 
 
def process_inventory_files(folder_path, sku_map_path):
    sku_mapping = load_sku_mapping(sku_map_path)
    if sku_mapping is None:
        print("SKU mapping failed. Exiting.")
        return None
 
    file_mappings = {
          'Amazon': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'asin',
                'SKU_Name': 'product-name',
                'Status':'order-status',
                'Sale Quantity': 'quantity',
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Availble
                'Location': '',#Not Availble
                'sale_date':'purchase-date'
            }
        },
        'BB TBF': {
            'sheet_name': 'analytics_vd_manufacturer_sales',
            'columns': {
                'Channel_SKU': 'source_sku_id',
                'SKU_Name': 'sku_description',
                'Sale Quantity': 'total_quantity',
                'MRP':'total_mrp',
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'total_sales',
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                 'sale_date':'date_range'
            }
        },
         'BB TFS': {
            'sheet_name': 'analytics_vd_manufacturer_sales',
            'columns': {
                'Channel_SKU': 'source_sku_id',
                'SKU_Name': 'sku_description',
                'Sale Quantity': 'total_quantity',
                'MRP':'total_mrp',
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'total_sales',
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':'date_range'
            }
        },
        'Blinkit': {
            'sheet_name': 'sales_csv-202931',
            'columns': {
                'Channel_SKU': 'item_id',
                'SKU_Name': 'item_name',
                'Sale Quantity': 'qty_sold',
                'MRP':'mrp',
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':''#Not Availble
            }
        },
        'Cred TBF': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty',
                'MRP':'MRP',
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
        'Flipkart MTD': {
            'sheet_name': 'earn_more_report',
            'columns': {
                'Channel_SKU': 'Product Id',
                'SKU_Name': 'Product Title',#Not Available
                'Sale Quantity': 'Total qty', #Not Available
                'MRP':'Final Sale Amount', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':'Order Date'
            }
        },
       'Flipkart OR': {
            'sheet_name': 'Sheet0',
            'columns': {
                'Channel_SKU': 'FSN',
                'SKU_Name': '',#Not Available
                'Sale Quantity': 'Final Sale Units', #Not Available
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':'Date'
            }
        },
        'Flipkart TBF': {
            'sheet_name': '',
            'columns': {
                'Channel_SKU': '',#Not Availble
                'SKU_Name': '',#Not Available
                'Sale Quantity': '', #Not Available
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':''#Not Availble
            }
        },
        'Flipkart DHC': {
            'sheet_name': '',
            'columns': {
                'Channel_SKU': '',#Not Availble
                'SKU_Name': '',#Not Available
                'Sale Quantity': '', #Not Available
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':''#Not Availble
            }
        },
          'Flipkart SIXAM': {
            'sheet_name': '',
            'columns': {
                'Channel_SKU': '',#Not Availble
                'SKU_Name': '',#Not Available
                'Sale Quantity': '', #Not Available
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':''#Not Availble
            }
        
        },
         'Jio Mart TFS': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                 'sale_date':'OrderDate'
            }
        },
          'Maccaron DHC': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                 'sale_date':'OrderDate'
            }
        },          
         'Maccaron TFS': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                 'sale_date':'OrderDate'
            }
        },
          'Myntra OR': {
            'sheet_name': 'Sales',
            'columns': {
                'Channel_SKU': 'style_id',
                'SKU_Name': '',#Not Availble
                'Sale Quantity': 'qty', 
                'MRP':'item_mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':'FD'
            }
        },
          'Myntra PPMP DHC': {
            'sheet_name': 'Myntra PPMP DHC',
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': '',#Not Availble
                'Sale Quantity': '', #Not Availble
                'MRP':'total mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'order status',
                'sale_date':''#Not Availble
            }
        },
          'Myntra PPMP SIXAM': {
            'sheet_name': 'Myntra PPMP sixam',
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': 'style name',
                'Sale Quantity': '', #Not Availble
                'MRP':'total mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'order status',
                'sale_date':''#Not Availble
            }
        },
          'Myntra PPMP TFS&BELIF': {
            'sheet_name': 'Myntra PPMP TFS Belif',
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': 'style name',
                'Sale Quantity': '', #Not Availble
                'MRP':'total mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'order status',
                'sale_date':''#Not Availble
            }
        },
          'Myntra SJIT': {
            'sheet_name': 'MYNTRA SJIT',
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': 'style name',
                'Sale Quantity': '', #Not Availble
                'MRP':'total mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'order status',
                'sale_date':''#Not Availble
            }
        },
             'Nykaa': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'SKU Code',
                'SKU_Name': 'SKU Name',
                'Sale Quantity': 'Total Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Selling Price', 
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'order status',
                'sale_date':'date'
            }
        },
              'Purplle MTD': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'ean_code',
                'SKU_Name': 'product_name',
                'Sale Quantity': 'qty', 
                'MRP':'mrp', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Availble
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Availble
                'sale_date':'order_date'
            }
        },
            'Smytten DHC': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
            'Smytten Sixam': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
            'Smytten TBF': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
             'Smytten TFS': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
         'Swiggy Sales': {
            'sheet_name': 'InstaMart (TFS) Sales - 1st to ',
            'columns': {
                'Channel_SKU': 'ITEM_CODE',
                'SKU_Name': '',#Not Available
                'Sale Quantity': 'UNITS_SOLD', 
                'MRP':'GMV', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'BASE_MRP',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Available
                'sale_date':'ORDERED_DATE'
            }
        },
            'Swiggy TBf': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'ITEM_CODE',
                'SKU_Name': 'PRODUCT_NAME',
                'Sale Quantity': 'TOTAL_QUANTITY', 
                'MRP':'MRP_PRICE', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'STORE_PRICE',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Available
                'sale_date':'MONTH_AND_YEAR'
            }
        },
             'Tata cliq TFS': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
              'Tata one mg': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
            'Tata one mg': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
         'Tata one mg-sixam': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
         'Tata one mg-TBF': {
            'sheet_name': 'Export',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
              'Workstore tbf': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Sale Quantity': 'Order Qty', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'Unit Price',
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'Status',
                'sale_date':'OrderDate'
            }
        },
               'Zepto MTD': {
            'sheet_name': 'Zepto Sales',
            'columns': {
                'Channel_SKU': 'EAN',
                'SKU_Name': 'SKU Name',
                'Sale Quantity': 'Sales (Qty) - Units', 
                'MRP':'MRP', 
                'MRP Sales':'', #Not Available
                'Sale_Price':'',#Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Availble
                'Status':'',#Not Available
                'sale_date':'Date'
            }
        },
    }
   
 
    all_inventory_data = []
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls'))]
 
    for file_name in excel_files:
        try:
            file_path = os.path.join(folder_path, file_name)
            mapping = None
           
            for channel, map_config in file_mappings.items():
                if channel.lower() in file_name.lower():
                    mapping = map_config
                    break
           
            if mapping is None:
                print(f"No mapping found for {file_name}. Skipping.")
                continue
 
            print(f"Processing file: {file_name} with mapping: {mapping}")
 
            try:
                soh_data = (pd.read_excel(file_path, sheet_name=mapping['sheet_name']))
            except Exception as e:
                print(f"Error loading sheet {mapping['sheet_name']} from {file_name}: {e}")
                continue
           
 
            print(f"Loaded data from {file_name}: {soh_data.shape} rows, {soh_data.columns.tolist()} columns.")
 
            extract_columns = {}
            for output_col, source_cols in mapping['columns'].items():
                if isinstance(source_cols, str):
                    source_cols = [source_cols]
                matched_cols = [col for col in source_cols if col in soh_data.columns]
               
                if matched_cols:
                    extract_columns[output_col] = matched_cols
                else:
                    print(f"Warning: Missing column(s) for '{output_col}' in {file_name}: {source_cols}")
 
            print(f"Extract columns mapping: {extract_columns}")
 
            inventory_data = soh_data.copy()
 
            for output_col, matched_cols in extract_columns.items():
                if output_col == 'Total Available Quantity':
                    inventory_data[output_col] = soh_data[matched_cols].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
                else:
                    inventory_data[output_col] = soh_data[matched_cols[0]]
           
            inventory_data = inventory_data[list(extract_columns.keys())]
            inventory_data['Channel'] = channel
 
            print(f"Mapped inventory data for {file_name}: {inventory_data.shape} rows.")
           
            inventory_data = pd.merge(
                inventory_data,
                sku_mapping[['Channel_SKU', 'Master_SKU']],
                on='Channel_SKU',
                how='left'
            )
           
            inventory_data['SKU_Mapped'] = inventory_data['Master_SKU'].notna()
            all_inventory_data.append(inventory_data)
 
            print(f"Finished processing {file_name}.")
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
 
    if all_inventory_data:
        final_inventory_data = pd.concat(all_inventory_data, ignore_index=True)
        output_path = 'Consolidated_Sales_Debug1.csv'
        final_inventory_data.to_csv(output_path, index=False)
        print(f"\nTotal files processed: {len(all_inventory_data)}")
        print(f"Total records: {len(final_inventory_data)}")
        print(f"Output saved to: {output_path}")
        return final_inventory_data
    else:
        print("No files were successfully processed.")
        return None
 
# Specify the folder containing Excel files
folder_path = 'e:/extract'
sku_map_path = 'e:/extract/SKU_Mapping.xlsx'
 
# Run the processing
result = process_inventory_files(folder_path, sku_map_path)