import os
import json
import csv
import glob


def find_all_json_files(root_dir):
    json_dir = os.path.join(root_dir, '**', '*.json')
    return glob.glob(json_dir, recursive=True)

def flatten_json(json_file, prefix=''):
    flattened = {}
    
    for key, value in json_file.items():
        new_key = f"{prefix}{key}" if prefix else key
        
        if isinstance(value, dict) and "type" in value and "coordinates" in value:
            flattened[new_key + "_type"] = value["type"]
            if isinstance(value["coordinates"], list) and len(value["coordinates"]) == 2:
                flattened[new_key + "_longitude"] = value["coordinates"][0]
                flattened[new_key + "_latitude"] = value["coordinates"][1]
            else:
                flattened[new_key + "_coordinates"] = str(value["coordinates"])
        elif isinstance(value, dict):
            nested_flattened = flatten_json(value, new_key + "_")
            flattened.update(nested_flattened)
        elif isinstance(value, list):
            if all(not isinstance(item, (dict, list)) for item in value):
                flattened[new_key] = str(value)
            else:
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        nested_flattened = flatten_json(item, f"{new_key}_{i}_")
                        flattened.update(nested_flattened)
                    else:
                        flattened[f"{new_key}_{i}"] = item
        else:
            flattened[new_key] = value
            
    return flattened

def convert_json_to_csv(json_file_path):
    csv_file_path = os.path.splitext(json_file_path)[0] + '.csv'
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        if isinstance(data, dict):
            flattened_data = [flatten_json(data)]
        elif isinstance(data, list):
            flattened_data = [flatten_json(item) for item in data]
        else:
            print(f"Unexpected JSON structure in {json_file_path}")
            return False
        
        all_keys = set()
        for item in flattened_data:
            all_keys.update(item.keys())
        
        headers = sorted(list(all_keys))
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(flattened_data)
        
        print(f"Successfully converted {json_file_path} to {csv_file_path}")
        return True
    
    except Exception as e:
        print(f"Error processing {json_file_path}: {str(e)}")
        return False



def main():
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    json_files = find_all_json_files(data_dir)
    
    if not json_files:
        print("No JSON files found in the data directory.")
        return
    
    print(f"Found {len(json_files)} JSON files.")
    
    successful_conversions = 0
    for json_file in json_files:
        if convert_json_to_csv(json_file):
            successful_conversions += 1
    
    print(f"Successfully converted {successful_conversions} out of {len(json_files)} files.")


if __name__ == "__main__":
    main()
