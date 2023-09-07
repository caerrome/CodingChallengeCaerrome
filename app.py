from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType , StructField, StringType, IntegerType
import os
import json
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import pandas as pd
import pymysql

sc = SparkContext.getOrCreate()
spark = SparkSession(sc).builder.master("local").appName("Coding Challenge Caerrome").getOrCreate()


app = Flask(__name__)
root_dir = os.getcwd()

def get_schema(file_name):
    # Schema for employees
    schema_employees = StructType([ \
            StructField("id", IntegerType(), True), \
            StructField("name", StringType(), True), \
            StructField("datetime", StringType(), True), \
            StructField("department_id", IntegerType(), True), \
            StructField("job_id", IntegerType(), True)
        ])

    # Schema for departments
    schema_departments = StructType([ \
            StructField("id", IntegerType(), True), \
            StructField("department", StringType(), True)
        ])

    # Schema for Jobs
    schema_jobs = StructType([ \
            StructField("id", IntegerType(),True), \
            StructField("job", StringType(),True)
        ])
    
    switcher = {
        "hired_employees": schema_employees,
        "departments": schema_departments,
        "jobs": schema_jobs
    }
    schema = switcher.get(file_name, None)

    return schema

# Function to upload CSV data to a database
def upload_csv_to_db(csv_file_path, db_table_name):
    schema = get_schema(db_table_name)
    if schema == None:
        return jsonify({"error": "No valid table for the database"}), 400
    else:
        df = spark.read.csv(csv_file_path, sep=",", header=False, schema=schema)
        #Check it does not apply the overwrite in tables of previous sections [NEED CHECK]
        write_to_database(df, db_table_name, "overwrite")

#Code to check and upload valid tables for the database based in the inputs
def check_and_upload_historic_tables(file_name, request_files):
    if file_name not in ["hired_employees", "departments", "jobs"]:
        return jsonify({"error": "No file part"}), 400

    file = request_files[file_name]
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        # Save the uploaded CSV file locally
        file_path = root_dir + "\\temp\\" + file.filename
        file.save(file_path)

        # Upload the CSV data to the database table
        upload_csv_to_db(file_path, file_name)

        return jsonify({"message": "CSV data uploaded successfully"}), 200

# API endpoint to receive and upload historical data
@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    for actual_file in request.files:
        try:
            check_and_upload_historic_tables(actual_file, request.files)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    return jsonify({"message": "CSV data uploaded successfully"}), 200

def insert_batch(file_name):
    try:
        data = request.get_json()
        schema = get_schema(file_name)

        if not data or not isinstance(data, list):
            return jsonify({"error": "Invalid data format"}), 400

        # Convert the JSON data to a DataFrame and insert in batches
        df = spark.createDataFrame(data=data, schema=schema)
        write_to_database(df, file_name, "append")

        return jsonify({"message": "Batch transactions inserted successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API endpoint to insert in batch new data
@app.route('/insert-hired_employees', methods=['POST'])
def insert_hired_employees():
    return insert_batch("hired_employees")

# API endpoint to insert in batch new data
@app.route('/insert-departments', methods=['POST'])
def insert_departments():
    return insert_batch("departments")

# API endpoint to insert in batch new data
@app.route('/insert-jobs', methods=['POST'])
def insert_jobs():
    return insert_batch("jobs")

def write_to_database(data_frame, table_name, type):
    with open("conf.json") as json_file:
        data_db = json.load(json_file)

    connection = pymysql.connect(
        host="localhost",
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

# API endpoint to insert in batch new data
@app.route('/get-number_quarter', methods=['GET'])
def get_number_quarter():
    with open("get_commands.json") as json_file:
        query = json.load(json_file)["number_quarter"]
    return read_from_database(query)

# API endpoint to insert in batch new data
@app.route('/get-number_hired', methods=['GET'])
def get_number_hired():
    with open("get_commands.json") as json_file:
        query = json.load(json_file)["number_hired"]
    return read_from_database(query)

def read_from_database(query):
    with open("conf.json") as json_file:
        data_db = json.load(json_file)

    connection = pymysql.connect(
        host="localhost",
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
# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)