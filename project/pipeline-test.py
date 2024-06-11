import os
import sqlite3
import pytest
import pandas as pd
from pipeline import main, DATA_DIRECTORY

@pytest.fixture
def setup_environment():
    # Setup test sample CSV
    test_kaggle_csv = os.path.join(DATA_DIRECTORY, 'test_kaggle_sample.csv')
    test_displacement_csv = os.path.join(DATA_DIRECTORY, 'test_displacement_sample.csv')
    test_data = pd.DataFrame({
        'dt': ['2009-01-01', '2010-02-01', '2011-03-01'],
        'AverageTemperature': [6.2, 7.3, 8.4],
        'AverageTemperatureUncertainty': [0.4, 0.5, 0.6],
        'Country': ['Testland', 'Testland', 'Testland']
    })
    test_data.to_csv(test_kaggle_csv, index=False)
    test_displacement_data = pd.DataFrame({
        'Entity': ['TestCountry', 'TestCountry', 'TestCountry', 'TestCountry', 'TestCountry', 'TestCountry'],
        'Code': ['TC', 'TC', 'TC', 'TC', 'TC', 'TC'],
        'Year': [2008, 2009, 2010, 2011, 2012, 2013],
        'Internally displaced persons, new displacement associated with disasters (number of cases)': [1000, 1000, 1000, 1000, 1000, 1000]
    })
    test_data.to_csv(test_displacement_csv, index=False)
    yield

    # Teardown: Cleanup created files
    berkeley_db_path = os.path.join(DATA_DIRECTORY, 'GlobalLandTemperaturesByCountry.sqlite')
    kaggle_db_path = os.path.join(DATA_DIRECTORY, 'internally-displaced-persons-from-disasters.sqlite')
    if os.path.exists(test_kaggle_csv):
        os.remove(test_kaggle_csv)
    if os.path.exists(test_displacement_csv):
        os.remove(test_displacement_csv)
    if os.path.exists(berkeley_db_path):
        os.remove(berkeley_db_path)
    if os.path.exists(kaggle_db_path):
        os.remove(kaggle_db_path)

def test_pipeline(setup_environment):
    # Run the pipeline in test mode
    main()

    # Paths to the SQLite files
    berkeley_db_path = os.path.join(DATA_DIRECTORY, 'GlobalLandTemperaturesByCountry.sqlite')
    kaggle_db_path = os.path.join(DATA_DIRECTORY, 'internally-displaced-persons-from-disasters.sqlite')

    # Check if the SQLite files are created
    assert os.path.exists(berkeley_db_path), f"Berkeley database file {berkeley_db_path} does not exist."
    assert os.path.exists(kaggle_db_path), f"Kaggle database file {kaggle_db_path} does not exist."

    # Verify the tables contain data
    with sqlite3.connect(berkeley_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        count_berkeley = cursor.fetchone()[0]
        assert count_berkeley > 0, "Berkeley database table is empty."

    with sqlite3.connect(kaggle_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM data")
        count_kaggle = cursor.fetchone()[0]
        assert count_kaggle > 0, "Kaggle database table is empty."
