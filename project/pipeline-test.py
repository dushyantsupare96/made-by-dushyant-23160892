import os
import sqlite3
import pytest
import pandas as pd
from unittest import mock
from pipeline import transform_data_and_clean_from_kaggle, transform_data_and_clean_from_csv, create_sqlite_from_dataframe, DATA_DIRECTORY

# Mock KaggleApi import and Kaggle functionality
with mock.patch.dict('sys.modules', {'kaggle.api.kaggle_api_extended': mock.Mock()}):
    from pipeline import transform_data_and_clean_from_kaggle, transform_data_and_clean_from_csv, create_sqlite_from_dataframe, DATA_DIRECTORY

@pytest.fixture
def setup_mock_datasets():
    """Fixture to setup and teardown mock datasets."""
    test_kaggle_csv = os.path.join(DATA_DIRECTORY, 'test_kaggle_sample.csv')
    test_displacement_csv = os.path.join(DATA_DIRECTORY, 'test_displacement_sample.csv')

    test_kaggle_data = pd.DataFrame({
        'dt': ['2009-01-01', '2010-02-01', '2011-03-01'],
        'AverageTemperature': [6.2, 7.3, 8.4],
        'AverageTemperatureUncertainty': [0.4, 0.5, 0.6],
        'Country': ['Testland', 'Testland', 'Testland']
    })
    test_kaggle_data.to_csv(test_kaggle_csv, index=False)
    
    test_displacement_data = pd.DataFrame({
        'Entity': ['TestCountry', 'TestCountry', 'TestCountry'],
        'Code': ['TC', 'TC', 'TC'],
        'Year': [2008, 2009, 2010],
        'Internally displaced persons': [1000, 1000, 1000]
    })
    test_displacement_data.to_csv(test_displacement_csv, index=False)
    
    yield test_kaggle_csv, test_displacement_csv

    # Teardown: Cleanup created files
    os.remove(test_kaggle_csv)
    os.remove(test_displacement_csv)
    berkeley_db_path = os.path.join(DATA_DIRECTORY, 'GlobalLandTemperaturesByCountry.sqlite')
    kaggle_db_path = os.path.join(DATA_DIRECTORY, 'internally-displaced-persons-from-disasters.sqlite')
    if os.path.exists(berkeley_db_path):
        os.remove(berkeley_db_path)
    if os.path.exists(kaggle_db_path):
        os.remove(kaggle_db_path)

@mock.patch('pipeline.transform_data_and_clean_from_kaggle')
def test_transform_and_clean_kaggle_data(mock_transform, setup_mock_datasets):
    test_kaggle_csv, _ = setup_mock_datasets

    # Define what the mock should return
    mock_transform.return_value = pd.DataFrame({
        'dt': ['2009-01-01', '2010-02-01', '2011-03-01'],
        'AverageTemperature': [6.2, 7.3, 8.4],
        'AverageTemperatureUncertainty': [0.4, 0.5, 0.6],
        'Country': ['Mockland', 'Mockland', 'Mockland']
    })
    
    # Call the function
    transformed_data = transform_data_and_clean_from_kaggle(test_kaggle_csv)
    
    # Verify the transformation
    assert not transformed_data.isnull().values.any(), "Transformed Kaggle data contains null values"
    assert not transformed_data.duplicated().values.any(), "Transformed Kaggle data contains duplicate rows"
    assert all(transformed_data['Country'] == 'Mockland'), "Transformed Kaggle data did not mock 'Country' correctly"

@mock.patch('pipeline.transform_data_and_clean_from_csv')
def test_transform_and_clean_displacement_data(mock_transform, setup_mock_datasets):
    _, test_displacement_csv = setup_mock_datasets

    # Define what the mock should return
    mock_transform.return_value = pd.DataFrame({
        'Entity': ['MockCountry', 'MockCountry', 'MockCountry'],
        'Code': ['TC', 'TC', 'TC'],
        'Year': [2008, 2009, 2010],
        'Internally displaced persons': [2000, 2000, 2000]
    })
    
    # Call the function
    transformed_data = transform_data_and_clean_from_csv(test_displacement_csv)
    
    # Verify the transformation
    assert not transformed_data.isnull().values.any(), "Transformed displacement data contains null values"
    assert not transformed_data.duplicated().values.any(), "Transformed displacement data contains duplicate rows"
    assert all(transformed_data['Entity'] == 'MockCountry'), "Transformed displacement data did not mock 'Entity' correctly"

@mock.patch('pipeline.create_sqlite_from_dataframe')
def test_create_sqlite_from_dataframe(mock_create_db, setup_mock_datasets):
    _, test_displacement_csv = setup_mock_datasets

    # Prepare the transformed data
    transformed_data = pd.DataFrame({
        'Entity': ['MockCountry', 'MockCountry', 'MockCountry'],
        'Code': ['TC', 'TC', 'TC'],
        'Year': [2008, 2009, 2010],
        'Internally displaced persons': [2000, 2000, 2000]
    })

    # Define what the mock should do
    def mock_db_effect(df, filename):
        db_path = os.path.join(DATA_DIRECTORY, filename)
        with sqlite3.connect(db_path) as conn:
            df.to_sql('data', conn, if_exists='replace', index=False)
    mock_create_db.side_effect = mock_db_effect

    # Call the function to create SQLite database
    db_filename = 'test_displacement.sqlite'
    create_sqlite_from_dataframe(transformed_data, db_filename)

    # Verify SQLite database
    db_path = os.path.join(DATA_DIRECTORY, db_filename)
    assert os.path.exists(db_path), f"SQLite database file {db_path} does not exist"

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        count = cursor.fetchone()[0]
        assert count == len(transformed_data), f"SQLite database table does not contain expected number of rows, found {count}"

    # Cleanup
    os.remove(db_path)

if __name__ == "__main__":
    pytest.main()