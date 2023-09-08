# CodingChallengeCaerrome
- This code contains a REST API with the next request:
    - 'POST'
        - '/upload-csv' Upload from one to multiple historical tables in csv to a SQL Database
        - '/insert-hired_employees' Upload batch of information to append new rows to the table hired_employee on a SQL Database
        - '/insert-departments' Upload batch of information to append new rows to the table departments on a SQL Database
        - '/insert-jobs' Upload batch of information to append new rows to the table jobs on a SQL Database
    - 'GET'
        - '/get-number_quarter' Returns the  Number of employees hired for each job and department in 2021 divided by quarter. Ordered alphabetically by department and job.
        - '/get-number_hired' Returns List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending)

- Important Files
    - 'db_conf.json' Configuation parameters to connect to the SQL database
    - 'schema.json' Schema file with the structure of the tables to be usen in de database
    - 'sql_queries.json' SQL queries to be applied on the GET request, it actually have the two requirements of the challenge

- The structure of the tables in the database, the configuration of the database and the queries to be used are dinamyc using the json files, so the code could be adapted to more uses and new request with new inputs tables or output information request.

- Execute app.py

Software and libraries used:
- Python 3.9.6
- Flask 2.3.3
- pandas 1.4.2
- pymysql 1.1.0
- pytest 7.3.1
- mysql-connector-j-8.1.0
- Spark 3.1.2
- Postman 10.17.4
- MySQL WorkBench 8.0
- Docker Engine 24.0.5