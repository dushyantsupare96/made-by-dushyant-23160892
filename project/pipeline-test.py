import os
import sqlite3
import pytest
import pandas as pd
from pipeline import transform_data_and_clean_from_kaggle, transform_data_and_clean_from_csv, create_sqlite_from_dataframe, DATA_DIRECTORY

@pytest.fixture
def setup_mock_datasets():
    # Setup test sample CSVs
    test_kaggle_csv = os.path.join(DATA_DIRECTORY, 'test_kaggle_sample.csv')
    test_displacement_csv = os.path.join(DATA_DIRECTORY, 'test_displacement_sample.csv')
    print("inside mock")
    test_kaggle_data = pd.DataFrame({
        'dt': ['2009-01-01', '2010-02-01', '2011-03-01'],
        'AverageTemperature': [6.2, 7.3, 8.4],
        'AverageTemperatureUncertainty': [0.4, 0.5, 0.6],
        'Country': ['Testland', 'Testland', 'Testland']
    })
    test_kaggle_data.to_csv(test_kaggle_csv, index=False)
    
    test_displacement_data = pd.DataFrame({
        'Entity': ['TestCountry', 'TestCountry', 'TestCountry', 'TestCountry', 'TestCountry', 'TestCountry'],
        'Code': ['TC', 'TC', 'TC', 'TC', 'TC', 'TC'],
        'Year': [2008, 2009, 2010, 2011, 2012, 2013],
        'Internally displaced persons, new displacement associated with disasters (number of cases)': [1000, 1000, 1000, 1000, 1000, 1000]
    })
    test_displacement_data.to_csv(test_displacement_csv, index=False)
    
    yield test_kaggle_csv, test_displacement_csv

    # Teardown: Cleanup created files
    if os.path.exists(test_kaggle_csv):
        os.remove(test_kaggle_csv)
    if os.path.exists(test_displacement_csv):
        os.remove(test_displacement_csv)
    berkeley_db_path = os.path.join(DATA_DIRECTORY, 'GlobalLandTemperaturesByCountry.sqlite')
    kaggle_db_path = os.path.join(DATA_DIRECTORY, 'internally-displaced-persons-from-disasters.sqlite')
    if os.path.exists(berkeley_db_path):
        os.remove(berkeley_db_path)
    if os.path.exists(kaggle_db_path):
        os.remove(kaggle_db_path)

def test_transform_and_clean_kaggle_data(setup_mock_datasets):
    test_kaggle_csv, _ = setup_mock_datasets

    # Test the transformation function for Kaggle data
    transformed_data = transform_data_and_clean_from_kaggle(test_kaggle_csv)
    
    # Verify the transformation
    assert not transformed_data.isnull().values.any(), "Transformed Kaggle data contains null values"
    assert not transformed_data.duplicated().any(), "Transformed Kaggle data contains duplicate rows"
    assert all(transformed_data['Country'] != 'Åland'), "Transformed Kaggle data contains discarded noisy data"
    assert all(transformed_data['dt'].dt.year >= 2008), "Transformed Kaggle data contains entries before 2008"

def test_transform_and_clean_displacement_data(setup_mock_datasets):
    _, test_displacement_csv = setup_mock_datasets

    # Test the transformation function for displacement data
    transformed_data = transform_data_and_clean_from_csv(test_displacement_csv)
    
    # Verify the transformation
    assert not transformed_data.isnull().values.any(), "Transformed displacement data contains null values"
    assert not transformed_data.duplicated().any(), "Transformed displacement data contains duplicate rows"
    assert 'Code' not in transformed_data.columns, "Transformed displacement data still contains 'Code' column"

def test_create_sqlite_from_dataframe(setup_mock_datasets):
    _, test_displacement_csv = setup_mock_datasets

    # Test SQLite creation
    transformed_data = transform_data_and_clean_from_csv(test_displacement_csv)
    db_filename = 'test_displacement.sqlite'
    create_sqlite_from_dataframe(transformed_data, db_filename)

    # Verify SQLite database
    db_path = os.path.join(DATA_DIRECTORY, db_filename)
    assert os.path.exists(db_path), f"SQLite database file {db_path} does not exist"

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        count = cursor.fetchone()[0]
        assert count > 0, "SQLite database table is empty"

    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
