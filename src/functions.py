from pyspark.sql.types import StructType , StructField, StringType, IntegerType
from flask import jsonify, request
import pandas as pd
import json
import pymysql
import os

root_dir = os.getcwd()

def turn_into_type(input):
    '''
    Function to turn a string type into a sql.type
    + input: string with the type used in the schema
    * Returns expected sql type fot the input or StringType() as default
    '''
    switcher = {
        "integer": IntegerType(),
        "string": StringType()
    }
    return switcher.get(input, StringType())

def create_struct_schema(dictionary, keyname):
    '''
    Function to create a StructType to create a valid schema
    + dictionary: dictionary with the tables schemas information
    + keyname: name of the table to be created a valid schema
    * Returns a StructType with the schema of the table
    '''
    output_schema = []
    actual_dict = dictionary[keyname]
    for i in actual_dict:
        field = StructField(i["name"], turn_into_type(i["type"]), True)
        output_schema.append(field)
    return StructType(output_schema)

def get_schema(file_name):
    '''
    Function to create a schema based on the input table
    + file_name: name of the table
    * Returns a StructType schema of the input name or a not valid value if the schema does not exist
    '''
    with open("json/schema.json") as json_file:
        dictionary_dt_tables = json.load(json_file)

    if file_name in dictionary_dt_tables.keys():
        return create_struct_schema(dictionary_dt_tables, file_name)
    else:
        print("Not valid input filename")
        return None
    
def upload_csv_to_db(spark, csv_file_path, db_table_name):
    '''
    Function to upload a historical csv file into a SQL database
    + spark: spark session
    + csv_file_path: local path of the csv to be uploaded
    + db_table_name: name of the table in the database
    '''
    schema = get_schema(db_table_name)
    if schema == None:
        return jsonify({"error": "No valid table for the database"}), 400
    else:
        df = spark.read.csv(csv_file_path, sep=",", header=False, schema=schema)
        write_to_database(df, db_table_name, "overwrite")

def write_to_database(data_frame, table_name, type):
    '''
    Function to write a data frame into a SQL database
    + data_frame: data frame to be uploaded
    + table_name: name of the table to be uploaded
    + type: mode type for the write
    '''
    with open("json/db_conf.json") as json_file:
        data_db = json.load(json_file)

    connection = pymysql.connect(
        host=data_db["host"],
        user=data_db["username"],
        password=data_db["password"],
        database=data_db["database_name"]
    )

    try:
        cursor = connection.cursor()
        data_frame.write.format("jdbc").option("url", data_db["url"]) \
            .option("driver", data_db["driver"]) \
            .option("dbtable", table_name) \
            .option("user", data_db["username"]) \
            .option("password", data_db["password"]) \
            .mode(type) \
            .save()
        cursor.close()
        connection.commit()
    except Exception as e:
        print(f"Error uploading to MySQL: {str(e)}")
    finally:
        connection.close()

def insert_batch(spark, file_name):
    '''
    Function to upload batchs as list of information into a SQL database
    + spark: spark session
    + file_name: name of the table
    '''
    try:
        data = request.get_json()
        schema = get_schema(file_name)

        if schema == None:
            return jsonify({"error": "No valid table for the database"}), 400
        else:
            if not data or not isinstance(data, list):
                return jsonify({"error": "Invalid data format"}), 400

            df = spark.createDataFrame(data=data, schema=schema)
            write_to_database(df, file_name, "append")

            return jsonify({"message": "Batch transactions inserted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
def read_from_database(query):
    '''
    Function to read the information of a database based on a SQL query
    + query: sql query to be used
    '''
    with open("json/db_conf.json") as json_file:
        data_db = json.load(json_file)

    connection = pymysql.connect(
        host=data_db["host"],
        user=data_db["username"],
        password=data_db["password"],
        database=data_db["database_name"]
    )

    try: 
        df = pd.read_sql(query, connection)
        data_json = df.to_json(orient="records")

        return jsonify({"data": data_json})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        connection.close()

def check_and_upload_historic_tables(spark, file_name, request_files):
    '''
    Function to check and upload a valid historical csv file into a SQL database
    + spark: spark session
    + file_name: name of the table
    + request_files: files to be uploaded on the request
    '''
    if file_name not in ["hired_employees", "departments", "jobs"]:
        return jsonify({"error": "No file part"}), 400

    file = request_files[file_name]
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        file_path = root_dir + "\\test_data\\temp\\" + file.filename
        file.save(file_path)

        upload_csv_to_db(spark, file_path, file_name)

        return jsonify({"message": "CSV data uploaded successfully"}), 200
    
def get_sql_query(command):
    '''
    Function to get a SQL query on the parameters file
    + command: key related with the sql code
    '''
    try:
        with open("json/sql_queries.json") as json_file:
            query = json.load(json_file)[command]
    except:
        print("The file or command does not exist")
        query = None
    finally:
        return query
    
def get_endpoint_by_command(command):
    '''
    Function to get an SQL endpoint with a specified command
    + command: key related with the sql code
    '''
    query = get_sql_query(command)
    if query == None:
        return jsonify({"error": "The parameters file or command does not exist"}), 400
    else:
        return read_from_database(query)