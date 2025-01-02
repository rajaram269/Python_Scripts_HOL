import os
import sys
import argparse
from openpyxl import load_workbook
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
default_file_mappings = {
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
            'sheet_name': 'BB TFS',  # Another example with different mapping
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
         'Myntra Inventory': {
            'sheet_name': 'mZw6zUMt_2024-12-17_SJIT_Invent',  #need to address
            'columns': {
                'Channel_SKU': 'style id',
                'SKU_Name': 'sku code',# title is missing
                'Total Available Quantity': 'sellable inventory count',
                'Inventory Location': 'warehouse name'
            }
        },
         'Myntra OR Inventory': {
            'sheet_name': 'Sheet1',  #need to address
            'columns': {
                'Channel_SKU': 'style_id',
                'SKU_Name': 'style_name',# title is missing
                'Total Available Quantity': 'inv_units_q1',
                'Inventory Location': 'warehouse_name'
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
            'sheet_name': '7a6b2178ad80415fbfecfcefa6fa1f6', 
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
         'Blinkit': {
            'sheet_name': 'Blinkit Inventory', 
            'columns': {
                'Channel_SKU': 'item_id',
                'SKU_Name': 'item_name',
                'Total Available Quantity': ['backend_inv_qty','frontend_inv_qty'],
                'Inventory Location': 'backend_facility_name'
            },
            'sum_inventory_columns': ['Total Available Quantity']
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
            'sheet_name': 'f81c1f0766ad5e56_NON_FBZ_INVENT', 
            'columns': {
                'Channel_SKU': 'SKU ID',
                'SKU_Name': 'SKU Name',
                'Total Available Quantity': 'Total Quantity',
                'Inventory Location': 'Store Name'
            }
        },
    }

file_mappings = default_file_mappings.copy()

def clean_dataframe(df, header_threshold=0.8):
    """
    Clean erratic or unhygienic data in a DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame to be cleaned
    header_threshold : float, default=0.8
        Minimum proportion of non-NA values required for a row to be considered as header
        
    Returns:
    --------
    pandas.DataFrame
        Cleaned DataFrame with proper headers and no empty rows/columns
        
    Raises:
    -------
    ValueError
        If DataFrame is empty or no valid header row can be detected
    """
    if df.empty:
        raise ValueError("The DataFrame is empty.")
        
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Calculate the threshold for non-NA values
    min_non_na_count = len(df.columns) * header_threshold
    
    # Find rows that meet the threshold
    non_na_counts = df.notna().sum(axis=1)
    valid_header_rows = non_na_counts[non_na_counts >= min_non_na_count]
    
    if valid_header_rows.empty:
        raise ValueError(f"No row found with at least {header_threshold*100}% non-NA values to use as header.")
    
    # Use the first row that meets the threshold
    header_row_index = valid_header_rows.index[0]-1
    
    # Validate the detected header row
    potential_header = df.iloc[header_row_index]
    if potential_header.isnull().all():
        raise ValueError("Selected header row is entirely empty.")
    
    # Clean and set the column names
    """ new_columns = (potential_header
                  .fillna("Column_" + pd.Series(range(len(potential_header))).astype(str))
                  .astype(str)
                  .str.strip()
                  .str.lower()
                  .str.replace(r'[^\w\s-]', '')  # Remove special characters
                  .str.replace(r'\s+', '_'))     # Replace spaces with underscores
     """
    
    # Handle duplicate column names
    """ seen = {}
    cleaned_columns = []
    for col in new_columns:
        if col in seen:
            seen[col] += 1
            cleaned_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            cleaned_columns.append(col)
    
    df.columns = cleaned_columns """
    
    # Remove rows above the header and reset index
    df = df.iloc[header_row_index + 1:].reset_index(drop=True)
    
    # Drop entirely empty rows and columns
    df = df.dropna(how='all')
    df = df.dropna(how='all', axis=1)
    
    # Remove duplicate rows
    df = df.drop_duplicates()
    
    # Convert empty strings to NaN for consistency
    #df = df.replace(r'^\s*$', np.nan, regex=True)
    
    # Fill forward for continuity (optional - comment out if not needed)
    #df = df.fillna(method='ffill')
    
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
        sku_mapping = combined_sku_mapping.drop_duplicates(subset=['Channel_SKU'], keep='first')
        print(f"Combined SKU mapping loaded successfully. Total rows: {len(combined_sku_mapping)}")
        sku_mapping.to_csv('E:/sku_map.csv', 
           index=False,           # Don't write row index
           sep=',',               # Use comma as separator (default)
           encoding='utf-8',      # Specify encoding
           header=True            # Write column names
 )

        return sku_mapping
    except Exception as e:
        print(f"Error loading SKU mapping file: {e}")
        return None

class PrintToGUI:
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, message):
        self.text_widget.insert(tk.END, message)
        self.text_widget.see(tk.END)  # Auto-scroll to the bottom

    def flush(self):
        pass  # For compatibility with Python's print/logging behavior

# Function to simulate data extraction
def extract_data(folder_path, output_path):
    # Simulate processing each file
    sku_mapping = load_sku_mapping('E:/SKU_Mapping.xlsx')
    if sku_mapping is None:
        print("SKU mapping failed. Exiting.")
        return None

    all_inventory_data = []
    excel_files = [f for f in os.listdir(folder_path) if f.endswith(('.xlsx', '.xls','.csv'))]
    #file_extension =

    for file_name in excel_files:
        file_extension = os.path.splitext(file_name)[1].lower()
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
                if file_extension == ".xlsx":
                    soh_data = (pd.read_excel(file_path, sheet_name=mapping['sheet_name']))
                    soh_data = clean_dataframe(soh_data)
                    #process_dataframe(df, sheet, results, classifier)
                elif file_extension == ".csv":
                    soh_data = pd.read_csv(file_path)
                    #soh_data = clean_dataframe(soh_data)
                    #process_dataframe(df, "Sheet1", results, classifier)
                else:
                    raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")

                #soh_data = (pd.read_excel(file_path, sheet_name=mapping['sheet_name']))
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
        output_path = 'E:/Consolidated_Inventory_Debug1.csv'
        final_inventory_data.to_csv(output_path, index=False)
        print(f"\nTotal files processed: {len(all_inventory_data)}")
        print(f"Total records: {len(final_inventory_data)}")
        print(f"Output saved to: {output_path}")
        return final_inventory_data
    else:
        print("No files were successfully processed.")
        return None

# GUI Functionality
def run_gui():
    def select_input_dir():
        input_path.set(filedialog.askdirectory())

    def select_output_dir():
        output_path.set(filedialog.askdirectory())

    def process_files():
        inp = input_path.get()
        out = output_path.get()

        if not inp or not out:
            messagebox.showerror("Error", "Please select both input and output directories.")
            return

        if not os.path.exists(inp) or not os.path.exists(out):
            messagebox.showerror("Error", "One or both directories do not exist.")
            return

        text_output.delete(1.0, tk.END)  # Clear the text box
        for step in extract_data(inp, out):
            text_output.insert(tk.END, step + "\n")
            text_output.see(tk.END)  # Auto-scroll
            root.update_idletasks()

    def edit_mappings():
        # Open a new window to edit mappings
        editor = tk.Toplevel(root)
        editor.title("Edit File Mappings")

        text_editor = ScrolledText(editor, width=100, height=15)
        text_editor.pack(padx=10, pady=10)

        # Load the current file mappings into the text editor
        mapping_text = ""
        for channel, config in file_mappings.items():
            mapping_text += f"Channel: {channel}\nSheet Name: {config['sheet_name']}\nColumns:\n"
            for output_col, source_col in config['columns'].items():
                mapping_text += f"  {output_col}: {source_col}\n"
            mapping_text += "\n"

        text_editor.insert(1.0, mapping_text)

        def save_mappings():
            # Parse the edited text back into the file_mappings structure
            try:
                new_mappings = {}
                lines = text_editor.get(1.0, tk.END).strip().split("\n")
                channel = None
                for line in lines:
                    line = line.strip()
                    if line.startswith("Channel:"):
                        channel = line.split(":", 1)[1].strip()
                        new_mappings[channel] = {'columns': {}}
                    elif line.startswith("Sheet Name:"):
                        new_mappings[channel]['sheet_name'] = line.split(":", 1)[1].strip()
                    elif ":" in line and channel:
                        key, value = map(str.strip, line.split(":", 1))
                        new_mappings[channel]['columns'][key] = value

                global file_mappings
                file_mappings = new_mappings
                messagebox.showinfo("Success", "Mappings updated successfully!")
                editor.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to update mappings: {e}")

        tk.Button(editor, text="Save Mappings", command=save_mappings).pack(pady=10)

    # GUI Layout
    root = tk.Tk()
    root.title("Batch Data Extractor")

    tk.Label(root, text="Input Files Directory:").pack(anchor="w", padx=10, pady=5)
    input_path = tk.StringVar()
    tk.Entry(root, textvariable=input_path, width=50).pack(padx=10, pady=5)
    tk.Button(root, text="Browse", command=select_input_dir).pack(pady=5)

    tk.Label(root, text="Output Files Directory:").pack(anchor="w", padx=10, pady=5)
    output_path = tk.StringVar()
    tk.Entry(root, textvariable=output_path, width=50).pack(padx=10, pady=5)
    tk.Button(root, text="Browse", command=select_output_dir).pack(pady=5)

    tk.Button(root, text="Edit Mappings", command=edit_mappings).pack(pady=5)
    tk.Button(root, text="Process Files", command=process_files).pack(pady=10)

    text_output = ScrolledText(root, width=150, height=40)
    text_output.pack(padx=10, pady=10)
    sys.stdout = PrintToGUI(text_output)

    root.mainloop()

# CLI Functionality
def run_cli():
    parser = argparse.ArgumentParser(description="Batch Data Extractor")
    parser.add_argument("input_dir", help="Path to the input files directory")
    parser.add_argument("output_dir", help="Path to the output files directory")
    args = parser.parse_args()

    inp = args.input_dir
    out = args.output_dir

    if not os.path.exists(inp) or not os.path.exists(out):
        print("Error: One or both directories do not exist.")
        return

    for step in extract_data(inp, out):
        print(step)

# Entry point for the app
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        run_cli()
    else:
        run_gui()
