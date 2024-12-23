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
        'Apollo': {
            'sheet_name': 'Sale',  # Specific sheet for Apollo
            'columns': {
                'Channel_SKU': 'itemid',# Source column : Desired output column
                'SKU_Name': 'itemname',
                'Sale Quantity': 'SALEQTY',
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'SALEVAL',
                'Location': 'Site_Name',
            }
        },
         'Nykaa': {
            'sheet_name': 'Sales', 
            'columns': {
                'Channel_SKU': 'sku', 
                'SKU_Name': 'Sku_Name',
                'Sale Quantity': 'QTY_Sales',
                'MRP':'', #Not Availble
                'MRP Sales':'MRP Value',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Final_store_name',
            }
        },
         'Enrich': {
            'sheet_name': 'Sales Data Nov_2024',
            'columns': {
                'Channel_SKU': 'ESIN', 
                'SKU_Name': 'Title',
                'Sale Quantity': 'Qty',
                'MRP':'MRP',
                'MRP Sales':' MRP Value ',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Available
            }
        },
         'Noble plus': {
            'sheet_name': 'sale',
            'columns': {
                'Channel_SKU': 'Item Code', 
                'SKU_Name': 'Item Name',
                'Sale Quantity': 'Sl.Qty',
                'MRP':'',#Not Available
                'MRP Sales':' Value ',# not sure
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Available
            }
        },
         'Pantaloons ': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'Article EAN', 
                'SKU_Name': 'Article Description',
                'Sale Quantity': ' Sales Qty ',
                'MRP':'MRP',
                'MRP Sales':'MRP Value',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Site Description',
            }
        },
          'SSL': {
            'sheet_name': 'SalesDetails.rdl',
            'columns': {
                'Channel_SKU': 'Style Code', 
                'SKU_Name': 'Style Desc',
                'Sale Quantity': 'Qty Sold',#Not Available
                'MRP':'MRP Per Unit',
                'MRP Sales':'',
                'Sale_Price':'Net Retail Value', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Location ',
            }
        },
          'Tata ': {
            'sheet_name': 'Sale',
            'columns': {
                'Channel_SKU': 'Sku Code', 
                'SKU_Name': 'Sku Name',
                'Sale Quantity': 'Qty',
                'MRP':'',#Not Available
                'MRP Sales':'Sum of MRP Sales',#Confused
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Store Full Name',
            }
        },
         'Tata cliq': {
            'sheet_name': 'Data Sheet 1',
            'columns': {
                'Channel_SKU': 'EAN', 
                'SKU_Name': 'PRODUCTNAME',
                'Sale Quantity': 'SOLD QTY',
                'MRP':'',#Not Available
                'MRP Sales':'MRP Val',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Store Name',
            }
        },
          'Tira': {
            'sheet_name': 'Sales',
            'columns': {
                'Channel_SKU': 'Article', 
                'SKU_Name': 'Article Description',
                'Sale Quantity': 'Billing Quantity',
                'MRP':'',#Not Available
                'MRP Sales':'',#Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'Net Sales With Tax',
                'Location': 'Site Name',
            }
        },
        'Boddess ': {
            'sheet_name': 'Sale',
            'columns': {
                'Channel_SKU': 'Article code', 
                'SKU_Name': 'ProductName',
                'Sale Quantity': 'Quantity',#Recheck
                'MRP':'',#Not Available
                'MRP Sales':'RRPTotal',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'StoreName',
            }
        },
         'Dabur': {
            'sheet_name': 'Sale',
            'columns': {
                'Channel_SKU': 'Material', 
                'SKU_Name': 'Material Description',
                'Sale Quantity': 'Qty in unit of entry',#Recheck
                'MRP':'',#Not Available
                'MRP Sales':'Sales Value inc. VAT',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'City',#not confirmed
            }
        },
         'D Mart': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'Material', 
                'SKU_Name': 'Description',
                'Sale Quantity': 'Sale Quantity ',
                'MRP':'',#Not Available
                'MRP Sales':'MRP Sales Value',  
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Fc Name',
            }
        },
         'Broadway ': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': '',#confused
                'SKU_Name': 'Item Name',
                'Sale Quantity': 'QTY',
                'MRP':'MRP',
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'Net Sales',
                'Location': 'Store Name',
            }
        },
        'Clinikally': {
            'sheet_name': 'The Face Shop',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'Title',
                'Sale Quantity': 'Sales Qty',
                'MRP':'MRP',
                'MRP Sales':'Sales Value', 
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': '',#Not Available
            }
        },
         'Purplle': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'Product Name',
                'Sale Quantity': 'Qty',
                'MRP':'Unit Price',
                'MRP Sales':'Subtotal', 
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'Store Name',
            }
        },
         'H&G': {
            'sheet_name': 'working',
            'columns': {
                'Channel_SKU': 'P99.Item Code',
                'SKU_Name': 'P99.Item Description',
                'Sale Quantity': 'SL.Sales Qty',
                'MRP':'mrp ',
                'MRP Sales':'sales value',
                'Sale_Price':'', #same as mrp
                'Net_Sales':'',#Not Available
                'Location': 'ST02.City',
            }
        },
        'B&N': {
            'sheet_name': 'Sheet1',
            'columns': {
                'Channel_SKU': 'EANCode',
                'SKU_Name': 'Product Description',
                'Sale Quantity': 'Quantity',
                'MRP':'MRP',
                'MRP Sales':'MRP Total',
                'Sale_Price':'', #Not Available
                'Net_Sales':'',#Not Available
                'Location': 'StoreName',
            }
        },
         'Mumbai T2': {
            'sheet_name': 'Adani Sales data',
            'columns': {
                'Channel_SKU': 'Item Code',
                'SKU_Name': '',#Not Available
                'Sale Quantity': 'Sales Qty',
                'MRP':'Total Amount',
                'MRP Sales':'',#Not Available
                'Sale_Price':'Net Amount', 
                'Net_Sales':'',#Not Available
                'Location': '',#Not Available
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
folder_path = 'E:/'
sku_map_path = 'E:/SKU_Mapping.xlsx'

# Run the processing
result = process_inventory_files(folder_path, sku_map_path)
