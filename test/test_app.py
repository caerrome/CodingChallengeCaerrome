import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os, sys
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, project_dir)

from jobs.app import app

BATCH_CONNECTION_VALUES = 500

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture(scope = "session")
def spark_session():
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc).builder.getOrCreate()
    yield spark
    spark.stop()

def test_upload_csv(client):
    response = client.post('/upload-csv', data={'csv_files': ('test_data/data/jobs.csv', 'file_content')}, content_type='multipart/form-data')
    assert response.status_code == 200

def test_insert_hired_employees(client):
    response = client.post('/insert-hired_employees', data=b'[[1, "Test", "20230101", 1, 1]]', content_type='multipart/raw')
    assert response.status_code == BATCH_CONNECTION_VALUES
    
def test_insert_departments(client):
    response = client.post('/insert-departments', data=b'[[1, "Test"]]', content_type='multipart/raw')
    assert response.status_code == BATCH_CONNECTION_VALUES

def test_insert_jobs(client):
    response = client.post('/insert-jobs', data=b'[[1, "Test"]]', content_type='multipart/raw')
    assert response.status_code == BATCH_CONNECTION_VALUES

def test_get_number_quarter(client):
    response = client.get('/get-number_quarter')
    assert response.status_code == 200
    assert response.content_type == 'application/json'

def test_get_number_hired(client):
    response = client.get('/get-number_hired')
    assert response.status_code == 200
    assert response.content_type == 'application/json'

if __name__ == '__main__':
    pytest.main()