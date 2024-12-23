import pandas as pd
import os

def load_sku_mapping(sku_map_path):
    # Read all sheets from the Excel file
    try:
        # Get all sheet names
        excel_file = pd.ExcelFile(sku_map_path)
        sheet_names = excel_file.sheet_names
        
        # List to collect dataframes from each sheet
        all_sheets_data = []
        
        # Process each sheet
        for sheet_name in sheet_names:
            # Read sheet
            print (sheet_name)
            df=[]
            df = pd.read_excel(sku_map_path, sheet_name=sheet_name)
            df = df.iloc[:, :3]
            
            # Rename columns to standard names
            df.columns = ['Channel_SKU', 'Master_SKU', 'SKU_Name']
            
            # Add a column with the sheet name
            df['Channel'] = sheet_name
            
            # Append to the list
            all_sheets_data.append(df)
        
        # Concatenate all sheets
        combined_sku_mapping = pd.concat(all_sheets_data, ignore_index=True)
        
        # Print some information about the mapping
        print("SKU Mapping Sheets Merged:")
        print(f"Total Sheets: {len(sheet_names)}")
        print(f"Total Rows: {len(combined_sku_mapping)}")
        print("\nFirst few rows:")
        print(combined_sku_mapping.head())
#         combined_sku_mapping.to_csv('combined_sku_map.csv', 
#           index=False,           # Don't write row index
#           sep=',',               # Use comma as separator (default)
#           encoding='utf-8',      # Specify encoding
#           header=True            # Write column names
# )
        
        return combined_sku_mapping
    
    except Exception as e:
        print(f"Error loading SKU mapping file: {e}")
        return None

def clean_dataframe(df):
    """
    Clean the dataframe by removing empty rows and rows with mostly NaN values
    Args:
        df (pd.DataFrame): Input dataframe to clean
    Returns:
        pd.DataFrame: Cleaned dataframe
    """
    # Remove rows where all values are NaN
    df = df.dropna(how='all')
    
    # Remove rows where a certain percentage of columns are NaN
    threshold = 0.7  # 70% of columns must have a non-NaN value
    df = df.dropna(thresh=int(len(df.columns) * (1 - threshold)))
    
    return df

def sum_inventory_columns(df, inventory_columns):
    """
    Sum multiple inventory columns, replacing with a single SOH column
    Args:
        df (pd.DataFrame): Input dataframe
        inventory_columns (list): List of column names to sum
    Returns:
        pd.DataFrame: Dataframe with summed inventory column
    """
    # Find existing columns
    existing_cols = [col for col in inventory_columns if col in df.columns]
    
    if len(existing_cols) == 0:
        print(f"No inventory columns found: {inventory_columns}")
        return df
    
    # Convert columns to numeric, replacing non-numeric values with 0
    for col in existing_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Sum the existing inventory columns
    df['Total Available Quantity'] = df[existing_cols].sum(axis=1)
    
    # If multiple columns were found, print a note
    if len(existing_cols) > 1:
        print(f"Summed inventory columns: {existing_cols}")
    
    # Drop the original inventory columns
    df.drop(columns=existing_cols, inplace=True)
    
    return df

def process_inventory_files(folder_path, sku_map_path):

    # Load SKU mapping
    sku_mapping = load_sku_mapping(sku_map_path)
    if sku_mapping is None:
        print("SKU mapping failed. Exiting.")
        return None
    
    # Detailed configuration for different file mappings
    file_mappings = {
        'Apollo': {
            'sheet_name': 'SOH',  # Specific sheet for Apollo
            'columns': {
                'Channel_SKU': 'Item',         # Source column : Desired output column
                'SKU_Name': 'itemname',
                'Total Available Quantity': 'QOH',
                'Inventory Location': 'Site_Name'
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
    
     
    # List to store dataframes from all files
    all_inventory_data = []
    
    # Get all Excel files in the specified folder
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls'))]
    
    # Process each Excel file
    for file_name in excel_files:
        try:
            # Construct full file path
            file_path = os.path.join(folder_path, file_name)
            
            # Find mapping based on partial filename match
            mapping = None
            for channel, map_config in file_mappings.items():
                if channel.lower() in file_name.lower():
                    mapping = map_config
                    break
            
            # Skip if no mapping found
            if mapping is None:
                print(f"No mapping found for {file_name}. Skipping.")
                continue
            
            # Load the specific sheet
            try:
                soh_data = pd.read_excel(file_path, sheet_name=mapping['sheet_name'])
            except Exception as sheet_error:
                print(f"Error loading sheet {mapping['sheet_name']} in {file_name}: {sheet_error}")
                continue
            
            # Prepare extraction dictionary
            # Extract columns based on the mapping
            extract_columns = {}
            missing_columns = []

            for output_col, source_cols in mapping['columns'].items():
             # Ensure source_cols is always a list
                 if isinstance(source_cols, str):
                    source_cols = [source_cols]

                # Collect all matching columns from source_cols
                 matched_cols = [col for col in source_cols if col in soh_data.columns]

                 if matched_cols:
                      # Map all matched columns to the same output column
                    extract_columns[output_col] = matched_cols
                 else:
                    # Append to missing columns if no match
                    missing_columns.append(output_col)

                # Warn about missing columns
            if missing_columns:
                print(f"Warning: Missing columns in {file_name}: {missing_columns}")

            # Flatten the matched columns for extraction
            # This ensures that all columns in the `extract_columns` dictionary are included
            columns_to_extract = []
            for cols in extract_columns.values():
                if isinstance(cols, list):
                 columns_to_extract.extend(cols)
                else:
                    columns_to_extract.append(cols)
            columns_to_extract = list(set(columns_to_extract))

            # Extract the relevant columns from the DataFrame
            inventory_data = soh_data[columns_to_extract].copy()

            # Sum inventory columns if specified for `Total Available Quantity`
            if 'Total Available Quantity' in extract_columns:
                inventory_columns = extract_columns['Total Available Quantity']
                if isinstance(inventory_columns, list):  # Check if it's a list of columns
                    inventory_data['Total Available Quantity'] = inventory_data[inventory_columns].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
                # Drop the original columns after summing
                    inventory_data.drop(columns=inventory_columns, inplace=True)
                else:
                    inventory_data['Total Available Quantity'] = pd.to_numeric(inventory_data[inventory_columns], errors='coerce').fillna(0)
            """ extract_columns = {}
            missing_columns = []
            
             # Check and map columns
            for output_col, source_cols in mapping['columns'].items():
                # Handle both single column and list of possible columns
                if isinstance(source_cols, str):
                    source_cols = [source_cols]
                
                # Find the first existing column
                found_col = next((col for col in source_cols if col in soh_data.columns), None)
                
                if found_col:
                    extract_columns[output_col] = found_col
                else:
                    missing_columns.append(output_col)
            
            # Validate column extraction
            if missing_columns:
                print(f"Warning: Missing columns in {file_name}: {missing_columns}")
                print(f"Available columns: {list(soh_data.columns)}")
                continue
            
            # Extract the data
            inventory_data = soh_data[list(extract_columns.values())].copy()
             """
            # Rename columns
            print(extract_columns.items())
            inventory_data.rename(columns={v: k for k, v in extract_columns.items()}, inplace=True)

            # Sum inventory columns if specified
            """  if 'sum_inventory_columns' in mapping:
                for inv_col_group in mapping['sum_inventory_columns']:
                    source_cols = mapping['columns'][inv_col_group]
                    inventory_data = sum_inventory_columns(inventory_data, source_cols)
                print(f"{channel}  SUM Success") """
            
            
            # Add Channel column
            #inventory_data["Channel"] = os.path.splitext(file_name)[0]
            inventory_data["Channel"] = channel

            try:
                # Filter SKU mapping for current channel
                #channel_sku_map = sku_mapping[sku_mapping['Channel'] == channel]
                
                # Merge inventory data with SKU mapping
                inventory_data = pd.merge(
                    inventory_data, 
                    sku_mapping[['Channel_SKU', 'Master_SKU']], 
                    on='Channel_SKU', 
                    how='left'
                )
                
                # Add a flag for unmapped SKUs
                inventory_data['SKU_Mapped'] = inventory_data['Master_SKU'].notna()
            except Exception as mapping_error:
                print(f"Error during SKU mapping for {file_name}: {mapping_error}")
                print(f"Inventory data:\n{inventory_data.head()}")
                print(f"Channel SKU map:\n{sku_mapping.head()}")
                # Optionally, you can choose to continue without mapping
                inventory_data['Master_SKU'] = None
                inventory_data['SKU_Mapped'] = False

            # Append to the list of dataframes
            all_inventory_data.append(inventory_data)
            
            print(f"Processed {file_name} successfully")
        
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
    
    # Combine all dataframes
    if all_inventory_data:
        final_inventory_data = pd.concat(all_inventory_data, ignore_index=True)
        
        # Output to CSV
        output_path = 'Consolidated_Inventory1.csv'
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
result = process_inventory_files(folder_path,sku_map_path)