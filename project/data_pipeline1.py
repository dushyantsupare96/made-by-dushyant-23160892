import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# Call the Kaggle API through the existing library
kaggleApiInit = KaggleApi()
kaggleApiInit.authenticate()

# Mention the kaggle dataset
#Another dataset doesnt have a working URl so not calling it
DATASET_FROM_KAGGLE = 'berkeleyearth/climate-change-earth-surface-temperature-data'  

# As the dataset is a zip file and we need only one file from that
WORKING_CSV_FILE = 'GlobalLandTemperaturesByCountry.csv'

# A directory needs to be created for data, that is csv and sqllite file
DATA_DIRECTORY = os.path.expanduser('./data')
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)

# Dataset Fetching would be done by this function from kaggle 
def get_dataset(dataset_url):
    local_directory_name = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1]) # This will make a directory by the name of the dataset in local environment
    kaggleApiInit.dataset_download_files(dataset_url, path=local_directory_name, unzip=True)
    return local_directory_name

# Initial Data cleaning and transformation steps
def transform_data_and_clean(file_path):
    # Dataframe initialization
    df = pd.read_csv(file_path)
    # Dropping missing values
    df.dropna(inplace=True)  
    # Dropping duplicate rows
    df.drop_duplicates(inplace=True)  
    # Correcting the date format
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'].dt.year > 2008]  # Keeping the data only after 2008
    return df

# Take this dataframe into sqllite file
def create_sqlite_from_dataframe(dataframe, db_filename):
    db_path = os.path.join(DATA_DIRECTORY, db_filename)
    conn = sqlite3.connect(db_path)
    dataframe.to_sql('data', conn, if_exists='replace', index=False)
    conn.close()

def main():
    data_directory = get_dataset(DATASET_FROM_KAGGLE)
    working_csv_file = os.path.join(data_directory, WORKING_CSV_FILE)
    if os.path.exists(working_csv_file):
        transform_data = transform_data_and_clean(working_csv_file)
        database_filename = os.path.splitext(WORKING_CSV_FILE)[0] + '.sqlite'
        create_sqlite_from_dataframe(transform_data, database_filename)
        print(f"Data from {WORKING_CSV_FILE} processed and saved in {database_filename}")
    else:
        print(f"{WORKING_CSV_FILE} not found in the downloaded dataset")

if __name__ == "__main__":
    main()