import os
import pandas as pd
from flask import Flask, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure upload and processing directories
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER

def load_sku_mapping(sku_map_path):
    try:
        # Get all sheet names
        excel_file = pd.ExcelFile(sku_map_path)
        sheet_names = excel_file.sheet_names
        
        all_sheets_data = []
        
        for sheet_name in sheet_names:
            df = pd.read_excel(sku_map_path, sheet_name=sheet_name)
            
            # Rename columns to standard names
            df.columns = ['Channel SKU', 'SKU Name', 'Master SKU']
            
            df['Channel'] = sheet_name
            all_sheets_data.append(df)
        
        combined_sku_mapping = pd.concat(all_sheets_data, ignore_index=True)
        
        return combined_sku_mapping
    
    except Exception as e:
        logger.error(f"Error loading SKU mapping file: {e}")
        return None

def process_inventory_files(folder_path, sku_map_path):
    sku_mapping = load_sku_mapping(sku_map_path)
    
    if sku_mapping is None:
        logger.error("SKU mapping failed.")
        return None
    
    file_mappings = {
        'Apollo': {
            'sheet_name': 'SOH',
            'columns': {
                'Channel SKU': 'Item',
                'SKU Name': 'itemname',
                'Total Available Quantity': 'QOH',
                'Inventory Location': 'Site_Name'
            }
        }
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
                    channel_name = channel
                    break
            
            if mapping is None:
                logger.warning(f"No mapping found for {file_name}. Skipping.")
                continue
            
            try:
                soh_data = pd.read_excel(file_path, sheet_name=mapping['sheet_name'])
            except Exception as sheet_error:
                logger.error(f"Error loading sheet {mapping['sheet_name']} in {file_name}: {sheet_error}")
                continue
            
            extract_columns = {}
            missing_columns = []
            
            for output_col, source_col in mapping['columns'].items():
                if source_col in soh_data.columns:
                    extract_columns[output_col] = source_col
                else:
                    missing_columns.append(source_col)
            
            if missing_columns:
                logger.warning(f"Missing columns in {file_name}: {missing_columns}")
                logger.warning(f"Available columns: {list(soh_data.columns)}")
                continue
            
            inventory_data = soh_data[list(extract_columns.values())].copy()
            
            inventory_data.rename(columns={v: k for k, v in extract_columns.items()}, inplace=True)
            
            inventory_data["Channel"] = channel_name
            
            try:
                channel_sku_map = sku_mapping[sku_mapping['Channel'] == channel_name]
                
                inventory_data = pd.merge(
                    inventory_data, 
                    channel_sku_map[['Channel SKU', 'Master SKU']], 
                    on='Channel SKU', 
                    how='left'
                )
                
                inventory_data['SKU_Mapped'] = inventory_data['Master SKU'].notna()
                
                unmapped_skus = inventory_data[inventory_data['SKU_Mapped'] == False]['Channel SKU'].unique()
                if len(unmapped_skus) > 0:
                    logger.info(f"Unmapped SKUs for {channel_name}: {unmapped_skus}")
            
            except Exception as mapping_error:
                logger.error(f"Error during SKU mapping for {file_name}: {mapping_error}")
                inventory_data['Master SKU'] = None
                inventory_data['SKU_Mapped'] = False
            
            all_inventory_data.append(inventory_data)
            
            logger.info(f"Processed {file_name} successfully")
        
        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")
    
    if all_inventory_data:
        final_inventory_data = pd.concat(all_inventory_data, ignore_index=True)
        
        output_path = os.path.join(app.config['PROCESSED_FOLDER'], 'Consolidated_Inventory.csv')
        final_inventory_data.to_csv(output_path, index=False)
        
        logger.info(f"Total files processed: {len(all_inventory_data)}")
        logger.info(f"Total records: {len(final_inventory_data)}")
        logger.info(f"Output saved to: {output_path}")
        
        return output_path
    else:
        logger.error("No files were successfully processed.")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        # Check if files were uploaded
        if 'inventory_files' not in request.files or 'sku_mapping' not in request.files:
            return jsonify({"error": "No file part"}), 400
        
        inventory_files = request.files.getlist('inventory_files')
        sku_mapping = request.files['sku_mapping']
        
        if not inventory_files or not sku_mapping:
            return jsonify({"error": "No selected file"}), 400
        
        # Save SKU mapping file
        sku_map_filename = secure_filename(sku_mapping.filename)
        sku_map_path = os.path.join(app.config['UPLOAD_FOLDER'], sku_map_filename)
        sku_mapping.save(sku_map_path)
        
        # Save inventory files
        for file in inventory_files:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
        
        # Process files
        result_path = process_inventory_files(app.config['UPLOAD_FOLDER'], sku_map_path)
        
        if result_path:
            return jsonify({
                "message": "Files processed successfully", 
                "download_link": "/download"
            }), 200
        else:
            return jsonify({"error": "Processing failed"}), 500
    
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/download')
def download_file():
    try:
        return send_file(
            os.path.join(app.config['PROCESSED_FOLDER'], 'Consolidated_Inventory.csv'),
            as_attachment=True
        )
    except Exception as e:
        logger.error(f"Download error: {e}")
        return jsonify({"error": "File not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)