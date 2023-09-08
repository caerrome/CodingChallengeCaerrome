import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os, sys
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, project_dir)

from jobs.app import app
from src.functions import *

BATCH_CONNECTION_VALUES = 500

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture
def app_flask():
    app_val = app  # Replace with the actual method to create your Flask app
    return app_val

@pytest.fixture(scope = "session")
def spark_session():
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc).builder.getOrCreate()
    yield spark
    spark.stop()

def test_turn_into_type():
    assert turn_into_type("integer") == IntegerType()
    assert turn_into_type("string") == StringType()
    assert turn_into_type("test") == StringType()
   
def test_create_struct_schema():
    dict_schema = {"test":[{"name": "test_name", "type": "atring"}, {"name": "test_number", "type": "integer"}]}
    keyname = "test"
    expected_output = StructType([
        StructField("test_name", StringType(), True),
        StructField("test_number", IntegerType(), True)
    ])
    assert create_struct_schema(dict_schema, keyname) == expected_output
   
def test_get_schema():
    expected_output = StructType([
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True)
    ])
    assert get_schema("jobs") == expected_output
    assert get_schema("test") == None
   
def test_upload_csv_to_db(app_flask, spark_session):
    with app_flask.app_context():
        file_path = root_dir + "\\test_data\\data\\" + "jobs.csv"
        assert upload_csv_to_db(spark_session, file_path, "error")[1] == 400
        assert upload_csv_to_db(spark_session, file_path, "jobs") == None
   
def test_write_to_database(spark_session):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True)
    ])
    df = spark_session.createDataFrame(data=[[1, "test"]], schema=schema)
    assert write_to_database(df, "jobs", "overwrite") == None

def test_insert_batch(app_flask, spark_session):
    with app_flask.app_context():
        assert insert_batch(spark_session, "jobs")[1] == BATCH_CONNECTION_VALUES
        assert insert_batch(spark_session, "errr")[1] == 500

def test_read_from_database(app_flask):
    with app_flask.app_context():
        assert isinstance(read_from_database("SELECT * from jobs"), str) == False
        assert read_from_database("SEECT 1")[1] == 500

def test_check_and_upload_historic_tables(app_flask, spark_session):
    with app_flask.app_context():
        request_files = {"jobs": {"filename" : "json/jobs.csv"}}
        assert check_and_upload_historic_tables(spark_session, "jobs", request_files)[1] == 400

def test_get_sql_query():
    expected_sql_query = ""
    assert get_sql_query("comando_1") == expected_sql_query
    assert get_sql_query("error") == None
   
def test_get_sql_query(app_flask):
    with app_flask.app_context():
        assert isinstance(get_endpoint_by_command("number_quarter"), str) == False
        assert get_endpoint_by_command("error")[1] == 400
        
if __name__ == '__main__':
    pytest.main()