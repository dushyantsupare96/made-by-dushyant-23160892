import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# A directory needs to be created for data, that is csv and sqllite file
DATA_DIRECTORY = os.path.expanduser('./data')
if not os.path.exists(DATA_DIRECTORY):
    os.makedirs(DATA_DIRECTORY)
PROJECT_DIRECTORY = os.path.expanduser('./project')
# Dataset Fetching would be done by this function from kaggle 
def get_dataset(dataset_url:str, kaggleApiInit : KaggleApi) -> str:
    local_directory_name = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1]) # This will make a directory by the name of the dataset in local environment
    if os.getenv('TEST_ENVIRONMENT') != 'true':
        kaggleApiInit.dataset_download_files(dataset_url, path=local_directory_name, unzip=True)
    return local_directory_name

# Initial Data cleaning and transformation steps
def transform_data_and_clean_from_kaggle(file_path):
    # Dataframe initialization
    df = pd.read_csv(file_path)
    # Dropping missing values
    df.dropna(inplace=True)  
    # Dropping duplicate rows
    df.drop_duplicates(inplace=True)  
    # Discarding noisy data
    df = df[df['Country'] != "Åland"]
    #rounding off the temperature to two digits for uniformity
    df['AverageTemperature'] = df['AverageTemperature'].round(2)
    df['AverageTemperatureUncertainty'] = df['AverageTemperatureUncertainty'].round(2)
    #selecting the entries after 2008, discarding data before that
    if 'dt' in df.columns:
        df['dt'] = pd.to_datetime(df['dt'])
        df = df[df['dt'].dt.year >= 2008]  # Keeping the data only after 2008
    return df
    
def transform_data_and_clean_from_csv(file_path):
    # Dataframe initialization
    df = pd.read_csv(file_path, sep=",")
    # Dropping missing values
    df.dropna(inplace=True)
    # Dropping duplicate rows
    df.drop_duplicates(inplace=True)
    # Dropping the 'Code' column if it exists
    # Code column is not needed
    if 'Code' in df.columns:
        df = df.drop(columns=['Code'])
    print ("inside csv transformation")
    print(df.head())
    return df
# Take this dataframe into sqllite file
def create_sqlite_from_dataframe(dataframe, db_filename):
    db_path = os.path.join(DATA_DIRECTORY, db_filename)
    conn = sqlite3.connect(db_path)
    dataframe.to_sql('data', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()

def main():
    #Another dataset doesnt have a working URl so not calling it
    DATASET_FROM_KAGGLE = 'berkeleyearth/climate-change-earth-surface-temperature-data'  

    # As the dataset is a zip file and we need only one file from that
    WORKING_CSV_FILE = 'GlobalLandTemperaturesByCountry.csv'
    kaggleApiInit = KaggleApi()
    kaggleApiInit.authenticate()
    data_directory = get_dataset(DATASET_FROM_KAGGLE,kaggleApiInit)
    working_csv_file = os.path.join(data_directory, WORKING_CSV_FILE)
    dataset_directory_direct_file = './internally-displaced-persons-from-disasters.csv'
    if os.path.exists(dataset_directory_direct_file):
        transform_data = transform_data_and_clean_from_csv(dataset_directory_direct_file)
        database_filename = os.path.splitext("internally-displaced-persons-from-disasters")[0] + '.sqlite'
        create_sqlite_from_dataframe(transform_data, database_filename)
        print(f"Data from {WORKING_CSV_FILE} processed and saved in {database_filename}")
    else:
        print(f"{WORKING_CSV_FILE} not found in the downloaded dataset")
   
    if os.path.exists(working_csv_file):
        transform_data = transform_data_and_clean_from_kaggle(working_csv_file)
        database_filename = os.path.splitext(WORKING_CSV_FILE)[0] + '.sqlite'
        create_sqlite_from_dataframe(transform_data, database_filename)
        print(f"Data from {WORKING_CSV_FILE} processed and saved in {database_filename}")
    else:
        print(f"{WORKING_CSV_FILE} not found in the downloaded dataset")

if __name__ == "__main__":
    main()