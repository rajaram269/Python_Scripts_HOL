import pandas as pd
import os

def process_inventory_files(folder_path):
    # Configuration for different file mappings
    file_mappings = {
        'Apollo': {
            'item_col': 'item',
            'itemname_col': 'itemname',
            'qoh_col': 'QOH',
            'site_col': 'Site_Name'
        },
        'Nykaa': {
            'item_col': 'SKU',
            'itemname_col': 'SKU Desc',
            'qoh_col': 'Available Qty',
            'site_col': 'Site Location '
        },
        'Tira': {
            'item_col': 'Article',
            'itemname_col': 'Article Description',
            'qoh_col': 'Qty',
            'site_col': 'Site'
        },
        # Add more file-specific mappings as needed
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
            for key, map_config in file_mappings.items():
                if key.lower() in file_name.lower():
                    mapping = map_config
                    break
            
            # Use default mapping if no match found
            if mapping is None:
                mapping = {
                    'item_col': 'Item',
                    'itemname_col': 'ItemName',
                    'qoh_col': 'Quantity',
                    'site_col': 'Location'
                }
            
            # Load the Excel file
            soh_data = pd.read_excel(file_path, sheet_name="SOH")
            
            # Flexible column matching
            def find_best_match(possible_columns):
                for col in possible_columns:
                    matches = [c for c in soh_data.columns if col.lower() in c.lower()]
                    if matches:
                        return matches[0]
                return None
            
            # Find best matching columns
            item_col = find_best_match([mapping['item_col']]) or mapping['item_col']
            itemname_col = find_best_match([mapping['itemname_col']]) or mapping['itemname_col']
            qoh_col = find_best_match([mapping['qoh_col']]) or mapping['qoh_col']
            site_col = find_best_match([mapping['site_col']]) or mapping['site_col']
            
            # Validate column existence
            columns_to_check = [item_col, itemname_col, qoh_col, site_col]
            if not all(col in soh_data.columns for col in columns_to_check):
                print(f"Warning: Could not find all required columns in {file_name}")
                print(f"Available columns: {list(soh_data.columns)}")
                continue
            
            # Extract and rename the relevant columns
            inventory_data = soh_data[[item_col, itemname_col, qoh_col, site_col]].copy()
            
            inventory_data.rename(columns={
                item_col: "SKU Code",
                itemname_col: "SKU Name",
                qoh_col: "Total Available Quantity",
                site_col: "Inventory Location"
            }, inplace=True)
            
            # Add Channel column
            inventory_data["Channel"] = os.path.splitext(file_name)[0]
            
            # Append to the list of dataframes
            all_inventory_data.append(inventory_data)
            
            print(f"Processed {file_name} successfully")
        
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
    
    # Combine all dataframes
    if all_inventory_data:
        final_inventory_data = pd.concat(all_inventory_data, ignore_index=True)
        
        # Output to CSV
        output_path = 'Consolidated_Inventory.csv'
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

# Run the processing
result = process_inventory_files(folder_path)