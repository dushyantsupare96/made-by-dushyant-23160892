import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from kaggle.api.kaggle_api_extended import KaggleApi
import sqlite3

from pipeline import (
    DATA_DIRECTORY,
    get_dataset,
    transform_data_and_clean_from_kaggle,
    transform_data_and_clean_from_csv,
    create_sqlite_from_dataframe,
    main
)

def create_kaggle_sample_dataframe():
    data = {
        'Country': ['CountryA', 'CountryB', 'CountryC'],
        'AverageTemperature': [23.45, 19.32, 21.56],
        'AverageTemperatureUncertainty': [0.8, 1.2, 0.9],
        'dt': ['2010-01-01', '2010-01-01', '2010-01-01']
    }
    df = pd.DataFrame(data)
    return df

def create_displacement_sample_dataframe():
    data = {
        'Entity': ['CountryA', 'CountryB', 'CountryC'],
        'Code': ['CTA', 'CTB', 'CTC'],
        'Year': [2008, 2009, 2010],
        'Internally displaced persons, new displacement associated with disasters (number of cases)': [3400, 2500, 4100]
    }
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def kaggle_sample_csv_file(tmp_path):
    df = create_kaggle_sample_dataframe()
    csv_file = tmp_path / "kaggle_sample.csv"
    df.to_csv(csv_file, index=False)
    return csv_file

@pytest.fixture
def displacement_sample_csv_file(tmp_path):
    df = create_displacement_sample_dataframe()
    csv_file = tmp_path / "displacement_sample.csv"
    df.to_csv(csv_file, index=False)
    return csv_file

@pytest.fixture
def sample_sqlite_file(tmp_path):
    sqlite_file = tmp_path / "sample.sqlite"
    return sqlite_file

@pytest.fixture
def kaggle_api_mock():
    return MagicMock(KaggleApi)

@pytest.fixture
def makedirs_mock():
    with patch('os.makedirs') as mock:
        yield mock

@pytest.fixture
def path_exists_mock():
    with patch('os.path.exists', return_value=True) as mock:
        yield mock

@pytest.fixture
def kaggle_read_csv_mock():
    with patch('pandas.read_csv', return_value=create_kaggle_sample_dataframe()) as mock:
        yield mock

@pytest.fixture
def displacement_read_csv_mock():
    with patch('pandas.read_csv', return_value=create_displacement_sample_dataframe()) as mock:
        yield mock

@pytest.fixture
def sqlite_mock():
    with patch('sqlite3.connect') as mock:
        yield mock

def test_get_dataset(kaggle_api_mock, makedirs_mock):
    dataset_url = 'berkeleyearth/climate-change-earth-surface-temperature-data'
    expected_path = os.path.join(DATA_DIRECTORY, dataset_url.split('/')[-1])

    with patch('os.getenv', return_value='false'):
        result = get_dataset(dataset_url, kaggle_api_mock)

    kaggle_api_mock.dataset_download_files.assert_called_once_with(dataset_url, path=expected_path, unzip=True)
    assert result == expected_path

def test_transform_data_and_clean_from_kaggle(kaggle_read_csv_mock, kaggle_sample_csv_file):
    df = transform_data_and_clean_from_kaggle(kaggle_sample_csv_file)

    assert not df.empty
    assert 'Country' in df.columns
    assert df['AverageTemperature'].dtype == float
    assert df['AverageTemperatureUncertainty'].dtype == float

def test_transform_data_and_clean_from_csv(displacement_read_csv_mock, displacement_sample_csv_file):
    df = transform_data_and_clean_from_csv(displacement_sample_csv_file)

    assert not df.empty
    assert 'Country' in df.columns
    assert 'Displacement' in df.columns

@pytest.mark.filterwarnings("ignore::UserWarning")
def test_create_sqlite_from_dataframe(sqlite_mock, sample_sqlite_file):
    df = create_kaggle_sample_dataframe()
    db_filename = sample_sqlite_file.name
    
    create_sqlite_from_dataframe(df, db_filename)
    
    sqlite_mock.assert_called_once_with(os.path.join(DATA_DIRECTORY, db_filename))
    conn = sqlite_mock.return_value
    
    conn.cursor.return_value.execute.assert_called()
    
    # Check that commit was called 
    assert conn.commit.call_count == 3
    conn.close.assert_called_once()

# Test the main function with both datasets
def test_main(kaggle_api_mock, path_exists_mock, kaggle_read_csv_mock, displacement_read_csv_mock, sqlite_mock, makedirs_mock):
    with patch('pipeline.get_dataset', return_value='mock_directory'):
        with patch('pipeline.transform_data_and_clean_from_kaggle', return_value=create_kaggle_sample_dataframe()):
            with patch('pipeline.transform_data_and_clean_from_csv', return_value=create_displacement_sample_dataframe()):
                with patch('pipeline.create_sqlite_from_dataframe') as create_sqlite_mock:
                    main()
                    create_sqlite_mock.assert_called()
