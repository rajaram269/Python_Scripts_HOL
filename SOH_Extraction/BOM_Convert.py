import csv
import json

def convert_bom_csv_to_json_array(input_file, output_file):
    parent_skus = {}

    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Print actual headers to debug
        print("Actual CSV headers:", reader.fieldnames)
        
        # Verify required columns exist
        required_columns = ['ParentSKUCode', 'ChildSKUCode', 'BundleQuantity']
        for col in required_columns:
            if col not in reader.fieldnames:
                raise ValueError(f"Column '{col}' not found in CSV headers. Actual headers: {reader.fieldnames}")

        for row in reader:
            parent_code = row['ParentSKUCode']  # Ensure this matches your CSV's header
            if parent_code not in parent_skus:
                parent_skus[parent_code] = {
                    'ParentSKUCode': parent_code,
                    'ParentSKUName': row['ParentSKUName'],
                    'ParentSKUBarcode': row['ParentSKUBarcode'],
                    'ChildSKUs': []
                }
            child = {
                'ChildSKUCode': row['ChildSKUCode'],
                #'ChildSKUName': row['ChildSKUName'],
                #'ChildSKUBarcode': row['ChildSKUBarcode'],
                'BundleQuantity': float(row['BundleQuantity'])
            }
            parent_skus[parent_code]['ChildSKUs'].append(child)

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['ParentSKUCode', 'ParentSKUName', 'ParentSKUBarcode', 'ChildSKUs']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for parent in parent_skus.values():
            parent_row = {
                'ParentSKUCode': parent['ParentSKUCode'],
                'ParentSKUName': parent['ParentSKUName'],
                'ParentSKUBarcode': parent['ParentSKUBarcode'],
                'ChildSKUs': json.dumps(parent['ChildSKUs'], separators=(',', ':'), ensure_ascii=False)
            }
            writer.writerow(parent_row)

# Example usage:
convert_bom_csv_to_json_array('d:/BOM.csv', 'd:/bom_output.csv')