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
                'Channel_SKU': 'itemid',         # Source column : Desired output column
                'SKU_Name': 'itemname',
                'Sale Quantity': 'SALEQTY',
                'MRP':'', #Not Availble
                'MRP Sales':'', #Not Available
                'Sale_Price':'', #Not Available
                'Net_Sales':'SALEVAL',
                'Location': 'Site_Name',
            }
        },
        'Nykaa Sales': {
            'sheet_name': 'SOH',  # Specific sheet for Nykaa
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Total Available Quantity': 'Available Qty',
                'Inventory Location': 'Site Location '
            }
        },
        'Tira': {
            'sheet_name': 'SOH',  # Another example with different mapping
            'columns': {
                'Channel_SKU': 'Article',
                'SKU_Name': 'Article Description',
                'Total Available Quantity': 'Qty',
                'Inventory Location': 'Site'
            }
        },
        'Amazon': {
            'sheet_name': 'Sheet1',  # Another example with different mapping
            'columns': {
                'Channel_SKU': 'sku',
                'SKU_Name': 'product-name',
                'Total Available Quantity': ['afn-total-quantity','afn-inbound-shipped-quantity'],
                #'Available Qty2': 'afn-inbound-shipped-quantity',
                'Inventory Location': 'store'
            },
            'sum_inventory_columns': ['Total Available Quantity']
        },
        'BB whole': {
            'sheet_name': 'Sheet1',  # Another example with different mapping
            'columns': {
                'Channel_SKU': 'sku_id',
                'SKU_Name': 'sku_name',
                'Total Available Quantity': 'soh',
                'Inventory Location': 'city'
            }
        },
         'Boddess': {
            'sheet_name': 'SOH', 
            'columns': {
                'Channel_SKU': '',#Id is missing in the sheet
                'SKU_Name': 'Product name',
                'Total Available Quantity': 'Qty',
                'Inventory Location': ''#Store is missing in the sheet
            }
        },
         'D Mart': {
            'sheet_name': 'Sheet1', 
            'columns': {
                'Channel_SKU': 'Material',
                'SKU_Name': 'Description',
                'Total Available Quantity': 'Stock',
                'Inventory Location': 'Fc Name'
            }
        },
        'Dabur': {
            'sheet_name': 'SOH', 
            'columns': {
                'Channel_SKU': 'Material',
                'SKU_Name': 'Material Description',
                'Total Available Quantity': 'Total inventory',
                'Inventory Location': 'City'
            }
        },
        'Enrich': {
            'sheet_name': 'SOH_02_Dec_2024', 
            'columns': {
                'Channel_SKU': 'esin',
                'SKU_Name': 'Product Name',
                'Total Available Quantity': 'Store Available',
                'Inventory Location': 'Store Name'
            }
        },
         'Flipkart': {
            'sheet_name': 'Sheet0', 
            'columns': {
                'Channel_SKU': 'FSN',
                'SKU_Name': 'Product Title',
                'Total Available Quantity': 'Inventory Available in Units',
                'Inventory Location': 'Warehouse'
            }
        },
         'Myntra': {
            'sheet_name': 'VSWPe4d7_2024-12-03_SJIT_Invent',  #need to address
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': 'sku code',# title is missing
                'Total Available Quantity': 'sellable inventory count',
                'Inventory Location': 'warehouse name'
            }
        },
         'NoblePlus': {
            'sheet_name': 'STOCK VALUATION DETAILED',  #address Blank Rows
            'columns': {
                'Channel_SKU': 'Item Code',
                'SKU_Name': 'Item Name',
                'Total Available Quantity': 'Pack Q',
                'Inventory Location': 'Branch' 
            }
            },
         'Nykaa Whole': {
            'sheet_name': '092aaca4b76c48f19c1dfe24b794e66', 
            'columns': {
                'Channel_SKU': 'SKU',
                'SKU_Name': 'SKU Desc',
                'Total Available Quantity': 'Total Quantity',
                'Inventory Location': 'Site Location'
            }
        },
        'Reliance': {
            'sheet_name': 'Sheet1', 
            'columns': {
                'Channel_SKU': 'sku_code',
                'SKU_Name': 'brand',#it is missing
                'Total Available Quantity': 'unicommerce_quantity',
                'Inventory Location': 'facility_code' #code is given
            }
        },
        'SSL': {
            'sheet_name': 'StockDetails.rdl',   #not available verify
            'columns': {
                'Channel_SKU': 'Style Code',
                'SKU_Name': 'Style Desc',
                'Total Available Quantity': 'Qty Available',
                'Inventory Location': 'Location'
            }
        },
         'Swiggy': {
            'sheet_name': 'warehouse_stock_data', 
            'columns': {
                'Channel_SKU': 'item_code',
                'SKU_Name': 'product_name',
                'Total Available Quantity': 'wh_soh',
                'Inventory Location': 'wh_name'
            }
        },
         'Tata': {
            'sheet_name': 'SOH',  #address first row empty
            'columns': {
                'Channel_SKU': 'onemg_sku_id',
                'SKU_Name': 'sku_name',
                'Total Available Quantity': 'free_qty',
                'Inventory Location': 'store_full_name'
            }
        },
          'TataCliq': {
            'sheet_name': 'SOH', 
            'columns': {
                'Channel_SKU': 'ALU',
                'SKU_Name': 'ARTICLE NO',
                'Total Available Quantity': 'ON_HAND',
                'Inventory Location': 'Store Name'
            }
        },
        'Pantaloons': {
            'sheet_name': 'Sheet1',   #address first row
            'columns': {
                'Channel_SKU': 'Article EAN',
                'SKU_Name': 'Article Description',
                'Total Available Quantity': 'Stock Qty', #not available
                'Inventory Location': 'Site Description'
            }
        },
         'Zepto ': {
            'sheet_name': '81cf71f069141895_NON_FBZ_INVENT', 
            'columns': {
                'Channel_SKU': 'SKU ID',
                'SKU_Name': 'SKU Name',
                'Total Available Quantity': 'Total Quantity',
                'Inventory Location': 'Store Name'
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
        output_path = 'Consolidated_Inventory_Debug1.csv'
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
