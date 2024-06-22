import json
import os

def save_json_file(json_data, file_path):
    # Check if the file already exists
    if os.path.exists(file_path):
        print(f'Skipping save: JSON file already exists at {file_path}')
        return
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write the JSON data to the file
    with open(file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=2)
    
    print(f'JSON file saved to: {file_path}')

if __name__ == "__main__":
    # JSON data to be saved
    json_data = {
        "username":"dushyantsupare",
        "key":"0669635d9d5b0730bf6acceb257888b8"
    }

    # File path where JSON file will be saved
    file_path = '/home/runner/.kaggle/kaggle.json'  # Change this path to your desired location

    # Save the JSON file
    save_json_file(json_data, file_path)
