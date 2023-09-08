from pyspark.sql import SparkSession
from pyspark import SparkContext
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
import os, sys

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, project_dir)

from src.functions import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc).builder.master("local").appName("Coding Challenge Caerrome").getOrCreate()
app = Flask(__name__)

# API endpoint to receive and upload historical data
@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    for actual_file in request.files:
        try:
            check_and_upload_historic_tables(spark, actual_file, request.files)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    return jsonify({"message": "CSV data uploaded successfully"}), 200

# API endpoint to insert hired_employees in batch new data
@app.route('/insert-hired_employees', methods=['POST'])
def insert_hired_employees():
    return insert_batch(spark, "hired_employees")

# API endpoint to insert departments in batch new data
@app.route('/insert-departments', methods=['POST'])
def insert_departments():
    return insert_batch(spark, "departments")

# API endpoint to insert jobs in batch new data
@app.route('/insert-jobs', methods=['POST'])
def insert_jobs():
    return insert_batch(spark, "jobs")

# API endpoint to get number_quarter sql command data
@app.route('/get-number_quarter', methods=['GET'])
def get_number_quarter():
    return get_endpoint_by_command("number_quarter")

# API endpoint to get number_hired sql command data
@app.route('/get-number_hired', methods=['GET'])
def get_number_hired():
    return get_endpoint_by_command("number_hired")

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)