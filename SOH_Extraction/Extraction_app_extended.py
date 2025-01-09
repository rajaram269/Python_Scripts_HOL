import os
import sys
import argparse
from openpyxl import load_workbook
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText
default_file_mappings = {
    "SOH_All_Combined": {
    "Amazon": {
      "sheet_name": "Sheet1",  
      "columns": {
        "Channel_SKU": "asin",
        "SKU_Name": "product-name",
        "Quantity": ["afn-fulfillable-quantity", "afn-reserved-quantity", "afn-inbound-shipped-quantity"],
        "Inventory Location": "store"
      },
      "sum_inventory_columns": ["Quantity"]
    },
    "Apollo": {
      "sheet_name": "SOH",  
      "columns": {
        "Channel_SKU": "Item",
        "SKU_Name": "itemname",
        "Quantity": "QOH",
        "Inventory Location": "Site_Name"
      }
    },
    "BB whole": {
      "sheet_name": "BB TFS",  
      "columns": {
        "Channel_SKU": "sku_id",
        "SKU_Name": "sku_name",
        "Quantity": "soh",
        "Inventory Location": "city"
      }
    },
    "Blinkit": {
      "sheet_name": "Blinkit Inventory", 
      "columns": {
        "Channel_SKU": "item_id",
        "SKU_Name": "item_name",
        "Quantity": ["backend_inv_qty", "frontend_inv_qty"],
        "Inventory Location": "backend_facility_name"
      },
      "sum_inventory_columns": ["Quantity"]
    },
    "Boddess": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "", 
        "SKU_Name": "Product name",
        "Quantity": "Qty",
        "Inventory Location": "" 
      }
    },
    "D Mart": {
      "sheet_name": "Sheet1", 
      "columns": {
        "Channel_SKU": "Material",
        "SKU_Name": "Description",
        "Quantity": "Stock",
        "Inventory Location": "Fc Name"
      }
    },
    "Dabur": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "Material",
        "SKU_Name": "Material Description",
        "Quantity": "Total Inventory",
        "Inventory Location": "City"
      }
    },
    "Enrich": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "esin",
        "SKU_Name": "Product Name",
        "Quantity": "Store Available",
        "Inventory Location": "Store Name"
      }
    },
    "Flipkart": {
      "sheet_name": "Sheet0", 
      "columns": {
        "Channel_SKU": "FSN",
        "SKU_Name": "Product Title",
        "Quantity": "Inventory Available in Units",
        "Inventory Location": "Warehouse"
      }
    },
    "Myntra Inventory": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "style id",
        "SKU_Name": "sku code", 
        "Quantity": "sellable inventory count",
        "Inventory Location": "warehouse name"
      }
    },
    "Myntra OR Inventory": {
      "sheet_name": "Sheet1", 
      "columns": {
        "Channel_SKU": "style_id",
        "SKU_Name": "style_name", 
        "Quantity": "inv_units_q1",
        "Inventory Location": "warehouse_name"
      }
    },
    "NoblePlus": {
      "sheet_name": "STOCK VALUATION DETAILED", 
      "columns": {
        "Channel_SKU": "Item Code",
        "SKU_Name": "Item Name",
        "Quantity": "Pack Q",
        "Inventory Location": "Branch" 
      }
    },
    "Nykaa Sales": {
      "sheet_name": "SOH",  
      "columns": {
        "Channel_SKU": "SKU",
        "SKU_Name": "SKU Desc",
        "Quantity": "Available Qty",
        "Inventory Location": "Site Location "
      }
    },
    "Nykaa Whole": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "SKU",
        "SKU_Name": "SKU Desc",
        "Quantity": "Available Quantity",
        "Inventory Location": "Site Location "
      }
    },
    "Pantaloons": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "EAN_Code",
        "SKU_Name": "Article Description",
        "Quantity": "Closing Stock", 
        "Inventory Location": "Site_Name"
      }
    },
    "Reliance": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "sku_code",
        "SKU_Name": "brand", 
        "Quantity": "unicommerce_quantity",
        "Inventory Location": "facility_code" 
      }
    },
    "SSL": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "Style Code",
        "SKU_Name": "Style Desc",
        "Quantity": "Qty Available",
        "Inventory Location": "Location"
      }
    },
    "Swiggy": {
      "sheet_name": "warehouse_stock_data", 
      "columns": {
        "Channel_SKU": "item_code",
        "SKU_Name": "product_name",
        "Quantity": "wh_soh",
        "Inventory Location": "wh_name"
      }
    },
    "Tata1mg": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "onemg_sku_id",
        "SKU_Name": "sku_name",
        "Quantity": "free_qty",
        "Inventory Location": "store_full_name"
      }
    },
    "TataCliq": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "ALU",
        "SKU_Name": "EAN CODE",
        "Quantity": "ON_HAND",
        "Inventory Location": "Store Name"
      }
    },
    "Tira": {
      "sheet_name": "SOH", 
      "columns": {
        "Channel_SKU": "Article",
        "SKU_Name": "Article Description",
        "Quantity": "Qty",
        "Inventory Location": "Site"
      }
    },
    "Zepto": {
      "sheet_name": "SOH",
      "columns": {
        "Channel_SKU": "SKU ID",
        "SKU_Name": "SKU Name",
        "Quantity": "Total Quantity",
        "Inventory Location": "Store Name"
      }
    }
  },
  "Ecomm_Sales":{
        'test':{
            'sheet_name': 'sample', 
            'columns': {
                'Channel_SKU': 'SKU ID',
                'SKU_Name': 'SKU Name',
                'Quantity': 'Total Quantity',
                'Inventory Location': 'Store Name'
            }
        },

    }
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
            #print(f"Processing SKU mapping sheet: {sheet_name}")
            df = pd.read_excel(sku_map_path, sheet_name=sheet_name)
            df = df.iloc[:, :3]
            df.columns = ['Channel_SKU', 'Master_SKU', 'SKU_Name']
            df['Channel'] = sheet_name
            all_sheets_data.append(df)
        
        combined_sku_mapping = pd.concat(all_sheets_data, ignore_index=True)
        sku_mapping = combined_sku_mapping.drop_duplicates(subset=['Channel_SKU'], keep='first')
        print(f"Combined SKU mapping loaded successfully. Total rows: {len(combined_sku_mapping)}")
        """ sku_mapping.to_csv('E:/sku_map.csv', 
           index=False,           # Don't write row index
           sep=',',               # Use comma as separator (default)
           encoding='utf-8',      # Specify encoding
           header=True            # Write column names
 ) """

        return sku_mapping
    except Exception as e:
        print(f"Error loading SKU mapping file: {e}")
        return None

def load_bundle_map(bundle_map_path):
    try:
        excel_file = pd.ExcelFile(bundle_map_path)
        sheet_name = "BOM SKU"
        bundle_data = pd.read_excel(bundle_map_path, sheet_name)
        bundle_data = bundle_data.iloc[:, :7]
        bundle_data.columns = ['Master_SKU', 'Master_SKU_Name', 'Master_SKU_Barcode','Child_SKU','Child_SKU_Name','Child_SKU_BarCode','Qty']
        #bundle_data_unique =bundle_data.drop_duplicates(subset=['Bundle_Master_SKU'], keep='first')
        return bundle_data
    except Exception as e:
        print(f"Error loading SKU mapping file: {e}")
        return None

def split_row(row, bundle_mapping):
                if row['Bundle?']:
                    # Filter the bundle_mapping for the current Master_SKU
                    bundle_rows = bundle_mapping[bundle_mapping['Master_SKU'] == row['Master_SKU']]
                    child_data = []
                    for _, bundle_row in bundle_rows.iterrows():
                            child_data.append({
                                **row.to_dict(),
                                'Master_SKU': bundle_row['Child_SKU'],  # Replace with the child SKU
                                'Quantity': bundle_row['Qty']*row['Quantity'],  # Each row now represents one unit of the child SKU
                                'Channel_SKU':row['Channel_SKU'],
                                'SKU_Name':bundle_row['Master_SKU_Name'],
                                'Channel':row['Channel'],
                                'SKU_Mapped':row['SKU_Mapped'],
                                'Master_SKU_Barcode':row['Master_SKU_Barcode'],
                                'Bundle?':row['Bundle?']
                            })
                    #print (child_data)        
                    return child_data
                else:
                    return [row.to_dict()]


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
    sku_mapping = load_sku_mapping('c:/references/SKU_Mapping.xlsx')
    bundle_mapping = load_bundle_map('c:/references/Bundle_Mapping.xlsx')
    bundle_mapping_unique =bundle_mapping.drop_duplicates(subset=['Master_SKU'], keep='first')
    if sku_mapping is None:
        print("SKU mapping failed. Exiting.")
        return None

    all_inventory_data = []
    all_warnings=[]
    all_errors=[]
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
                all_errors.append(f"No mapping found for {file_name}. Skipping.")
                continue

            #print(f"Processing file: {file_name}")

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
                    all_errors.append("Unsupported file format. Only .xlsx and .csv are supported.")
                    raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
                    
                #soh_data = (pd.read_excel(file_path, sheet_name=mapping['sheet_name']))
            except Exception as e:
                print(f"Error loading sheet {mapping['sheet_name']} from {file_name}: {e}")
                all_errors.append(f"Error loading sheet {mapping['sheet_name']} from {file_name}: {e}")
                continue
            

            #print(f"Loaded data from {file_name}: {soh_data.shape} rows, columns.")

            extract_columns = {}
            for output_col, source_cols in mapping['columns'].items():
                if isinstance(source_cols, str):
                    source_cols = [source_cols]
                matched_cols = [col for col in source_cols if col in soh_data.columns]
                
                if matched_cols:
                    extract_columns[output_col] = matched_cols
                else:
                    print(f"Warning: Missing column(s) for '{output_col}' in {file_name}: {source_cols}")
                    all_warnings.append(f"Warning: Missing column(s) for '{output_col}' in {file_name}: {source_cols}")

            #print(f"Extract columns mapping: {extract_columns}")

            inventory_data = soh_data.copy()

            for output_col, matched_cols in extract_columns.items():
                if output_col == 'Quantity':
                    inventory_data[output_col] = soh_data[matched_cols].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)
                else:
                    inventory_data[output_col] = soh_data[matched_cols[0]]
            
            inventory_data = inventory_data[list(extract_columns.keys())]
            inventory_data['Channel'] = channel

            #print(f"Mapped inventory data for {file_name}: {inventory_data.shape} rows.")
            
            inventory_data = pd.merge(
                inventory_data,
                sku_mapping[['Channel_SKU', 'Master_SKU']],
                on='Channel_SKU',
                how='left'
            )
            inventory_data['SKU_Mapped'] = inventory_data['Master_SKU'].notna()
            #print(inventory_data.columns)
            inventory_data1=inventory_data.copy()
            inventory_data1 = pd.merge(
                inventory_data1,
                bundle_mapping_unique[['Master_SKU', 'Master_SKU_Barcode']],
                on='Master_SKU',
                how='left'
            )
            inventory_data1['Bundle?'] = inventory_data1['Master_SKU_Barcode'].notna()
            all_inventory_data.append(inventory_data1)

            print(f"Finished processing {file_name}.")
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            all_errors.append(f"Error processing {file_name}: {e}")

    if all_inventory_data:
        final_inventory_data = pd.concat(all_inventory_data, ignore_index=True)
        timestamp = pd.Timestamp.now().strftime('%m%d_%H%M')
        output_filename = f'Consolidated_Inventory_{timestamp}.csv'
        final_output_path = os.path.join(output_path, output_filename)
        final_inventory_data.to_csv(final_output_path, index=False)
        print(f"\nTotal files processed: {len(all_inventory_data)}, Total files ignored:{len(excel_files)-len(all_inventory_data)}")
        print(f"Total records: {len(final_inventory_data)}")
        print(f"Output saved to: {final_output_path}")
        
        #unbundled file creation. Two files will be created
        columns = final_inventory_data.columns
        bundle_data=final_inventory_data[final_inventory_data['Bundle?']]
        non_bundle_data=final_inventory_data[~final_inventory_data['Bundle?']]
        unbundled_rows = bundle_data.apply(lambda row: split_row(row, bundle_mapping), axis=1)
        inventory_data_unbundled = pd.DataFrame([item for sublist in unbundled_rows for item in sublist])
        final_inventory_data = pd.concat([non_bundle_data, inventory_data_unbundled], ignore_index=True)
        print(f"Unbundled data for : {inventory_data_unbundled.shape} rows.")
        output_filename = f'unbundled_Inventory_{timestamp}.csv'
        final_output_path = os.path.join(output_path, output_filename)
        final_inventory_data.to_csv(final_output_path, index=False)
        print(all_warnings)
        print("-----  -----")
        print("-----  -----")
        print(all_errors)
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

    def update_mappings(*args):
        selected_usecase = usecase_var.get()
        global file_mappings
        file_mappings = default_file_mappings[selected_usecase].copy()
        print(f"Updated mappings for use case: {selected_usecase}")

    def process_files():
        inp = input_path.get()
        out = output_path.get()

        if not inp or not out:
            messagebox.showerror("Error", "Please select both input and output directories.")
            return

        if not os.path.exists(inp) or not os.path.exists(out):
            messagebox.showerror("Error", "One or both directories do not exist.")
            return

        text_output.delete(1.0, tk.END)
        for step in extract_data(inp, out):
            text_output.insert(tk.END, step + "\n")
            text_output.see(tk.END)
            root.update_idletasks()

    def edit_mappings():
        editor = tk.Toplevel(root)
        editor.title("Edit File Mappings")
        editor.configure(bg='#f5f5f5')

        # Style the editor window
        editor.geometry("720x400")
        
        frame = tk.Frame(editor, bg='#f5f5f5', padx=20, pady=20)
        frame.pack(fill=tk.BOTH, expand=True)

        text_editor = ScrolledText(
            frame,
            width=100,
            height=15,
            font=('Segoe UI', 10),
            bg='white',
            fg='#333333',
            insertbackground='#2196F3',
            selectbackground='#90CAF9',
            selectforeground='white',
            relief=tk.FLAT,
            padx=10,
            pady=10
        )
        text_editor.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        mapping_text = ""
        for channel, config in file_mappings.items():
            mapping_text += f"Channel: {channel}\nSheet Name: {config['sheet_name']}\nColumns:\n"
            for output_col, source_col in config['columns'].items():
                mapping_text += f"  {output_col}: {source_col}\n"
            mapping_text += "\n"

        text_editor.insert(1.0, mapping_text)

        def save_mappings():
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

        save_btn = tk.Button(
            frame,
            text="Save Mappings",
            command=save_mappings,
            bg='#2196F3',
            fg='white',
            font=('Segoe UI', 10, 'bold'),
            relief=tk.FLAT,
            padx=20,
            pady=10
        )
        save_btn.pack(pady=15)
        tk.Button(editor, text="Save Mappings", command=save_mappings).pack(pady=10)

    # Main Window Setup
    root = tk.Tk()
    root.title("Batch Data Extractor")
    root.configure(bg='#f5f5f5')
    root.geometry("1200x900")  # Set initial window size

    # Style Configuration
    style = ttk.Style()
    style.theme_use('clam')
    style.configure('TCombobox', 
                   fieldbackground='white',
                   background='white',
                   foreground='#333333',
                   selectbackground='#2196F3',
                   selectforeground='white',
                   padding=5)
    
    # Create main frame
    main_frame = tk.Frame(root, bg='#f5f5f5', padx=30, pady=20)
    main_frame.pack(fill=tk.BOTH, expand=True)

    # Title
    title_label = tk.Label(
        main_frame,
        text="Batch Data Extractor",
        font=('Segoe UI', 24, 'bold'),
        bg='#f5f5f5',
        fg='#333333'
    )
    title_label.pack(pady=(0, 20))

    # Use Case Selection
    usecase_frame = tk.Frame(main_frame, bg='#f5f5f5')
    usecase_frame.pack(fill=tk.X, pady=10)
    
    tk.Label(
        usecase_frame,
        text="Select Use Case:",
        font=('Segoe UI', 11),
        bg='#f5f5f5',
        fg='#333333'
    ).pack(side=tk.LEFT)
    
    usecase_var = tk.StringVar(value="Select use case")
    usecase_dropdown = ttk.Combobox(
        usecase_frame,
        textvariable=usecase_var,
        values=list(default_file_mappings.keys()),
        state="readonly",
        width=40
    )
    usecase_dropdown.pack(side=tk.LEFT, padx=(10, 0))
    usecase_dropdown.bind('<<ComboboxSelected>>', update_mappings)

    # Warning Message
    warning_label = tk.Label(
        main_frame,
        text="Please save SKU mapping master file in 'c:/references/SKU_Mapping.xlsx' and Bundle SKU Master file in 'c:/references/Bundle_Mapping.xlsx' else there will be an error",
        font=('Segoe UI', 10),
        bg='#fff3e0',
        fg='#e65100',
        padx=15,
        pady=10,
        relief=tk.FLAT
    )
    warning_label.pack(fill=tk.X, pady=10)

    # Input Directory
    input_frame = tk.Frame(main_frame, bg='#f5f5f5')
    input_frame.pack(fill=tk.X, pady=10)
    
    tk.Label(
        input_frame,
        text="Input Files Directory:",
        font=('Segoe UI', 11),
        bg='#f5f5f5',
        fg='#333333'
    ).pack(side=tk.LEFT)
    
    input_path = tk.StringVar()
    tk.Entry(
        input_frame,
        textvariable=input_path,
        width=50,
        font=('Segoe UI', 10),
        relief=tk.SOLID,
        bd=1
    ).pack(side=tk.LEFT, padx=10)
    
    tk.Button(
        input_frame,
        text="Browse",
        command=select_input_dir,
        bg='#2196F3',
        fg='white',
        font=('Segoe UI', 10),
        relief=tk.FLAT,
        padx=15
    ).pack(side=tk.LEFT)

    # Output Directory
    output_frame = tk.Frame(main_frame, bg='#f5f5f5')
    output_frame.pack(fill=tk.X, pady=10)
    
    tk.Label(
        output_frame,
        text="Output Files Directory:",
        font=('Segoe UI', 11),
        bg='#f5f5f5',
        fg='#333333'
    ).pack(side=tk.LEFT)
    
    output_path = tk.StringVar()
    tk.Entry(
        output_frame,
        textvariable=output_path,
        width=50,
        font=('Segoe UI', 10),
        relief=tk.SOLID,
        bd=1
    ).pack(side=tk.LEFT, padx=10)
    
    tk.Button(
        output_frame,
        text="Browse",
        command=select_output_dir,
        bg='#2196F3',
        fg='white',
        font=('Segoe UI', 10),
        relief=tk.FLAT,
        padx=15
    ).pack(side=tk.LEFT)

    # Action Buttons
    button_frame = tk.Frame(main_frame, bg='#f5f5f5')
    button_frame.pack(pady=20)
    
    tk.Button(
        button_frame,
        text="Edit Mappings",
        command=edit_mappings,
        bg='#FFA726',
        fg='white',
        font=('Segoe UI', 10, 'bold'),
        relief=tk.FLAT,
        padx=15,
        pady=10
    ).pack(side=tk.LEFT, padx=10)
    
    tk.Button(
        button_frame,
        text="Process Files",
        command=process_files,
        bg='#66BB6A',
        fg='white',
        font=('Segoe UI', 10, 'bold'),
        relief=tk.FLAT,
        padx=15,
        pady=10
    ).pack(side=tk.LEFT, padx=10)

    # Output Text Area
    text_output = ScrolledText(
        main_frame,
        width=150,
        height=15,
        font=('Consolas', 10),
        bg='white',
        fg='#333333',
        relief=tk.FLAT,
        padx=5,
        pady=5
    )
    text_output.pack(pady=10)
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
