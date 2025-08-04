# import mysql.connector
# from classes import IngestionInputClass, InputTypeInputClass
# import json
# import subprocess

# # Connect to server
# cnx = mysql.connector.connect(
#     host="localhost",
#     port=3306,
#     user="root",
#     password="rootpass")

# # Get a cursor
# cur = cnx.cursor()

# # Execute a query
# cur.execute("SELECT * from demo.ingestion")

# # Fetch one result
# row = cur.fetchone()

# input_type_input_json_data = json.loads(row[6])
# input_type_input_json_data = input_type_input_json_data[0]
# print(input_type_input_json_data)

# # input_type_input = InputTypeInputClass(
# #     input_type=row[5],
# #     merge_columns=input_type_input_json_data.get('merge_columns', []),
# #     cdc_columns=input_type_input_json_data.get('cdc_columns', []),
# #     delete_insert_columns=input_type_input_json_data.get('delete_insert_columns', [])
# # )

# # ingestion_input = IngestionInputClass(
# #     organization=row[0],
# #     source_system=row[1],
# #     input_db_name=row[2],
# #     input_table_name=row[3],
# #     s3_bucket_name=row[4],
# #     input_type=row[5],
# #     input_type_input=input_type_input,
# #     input_columns=json.loads(row[7])[0],
# #     encrypted_columns=row[8],
# #     partition_columns=row[9]
# # )

# command = [
#     "python",
#     "ingestion.py",
#     "--organization", row[0],
#     "--source_system", row[1],
#     "--input_db_name", row[2],
#     "--input_table_name", row[3],
#     "--s3_bucket_name", row[4],
#     "--input_type", row[5],
#     "--input_type_input", row[6],
#     "--input_columns", f"{row[7]}",
#     "--encrypted_columns", 'None' if row[8] == None else row[8],
#     "--partition_columns", 'None' if row[9] == None else row[9]
# ]

# print("Running command:", " ".join(command))

# # subprocess.run(["python", "ingestion.py",
# #                     "--organization", ingestion_input.organization,
# #                     "--source_system", ingestion_input.source_system,
# #                     "--input_db_name", ingestion_input.input_db_name,
# #                     "--input_table_name", ingestion_input.input_table_name,
# #                     "--s3_bucket_name", ingestion_input.s3_bucket_name,
# #                     "--input_type", ingestion_input.input_type,
# #                     "--input_type_input", json.dumps(ingestion_input.input_type_input.model_dump()),
# #                     "--input_columns", json.dumps(ingestion_input.input_columns),
# #                     "--encrypted_columns", json.dumps(ingestion_input.encrypted_columns),
# #                     "--partition_columns", json.dumps(ingestion_input.partition_columns)
# #                     ])
# # Close the cursor and connection
# cur.close()
# cnx.close()

    
    
    
# from connectors.oracle import oracle_connector
# oracle_connector = oracle_connector()

# query = "SELECT * FROM DIP.employees"
# result = oracle_connector.execute_query(query)
# if result:
#     for row in result:
#         print(row)


import json
from pyspark.sql import SparkSession
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.oracle.database.jdbc:ojdbc8:19.3.0.0") \
    .enableHiveSupport() \
    .getOrCreate()
    
# set logging level to WARN
spark.sparkContext.setLogLevel("OFF")

# connect to oracle database from spark and read data
jdbc_url = "jdbc:oracle:thin:@localhost:1521/XEPDB1?internal_logon=SYSDBA"
properties = {
    "user": "SYS",
    "password": "MySecurePassword1",
    "driver": "oracle.jdbc.OracleDriver",
}

df = spark.read.jdbc(url=jdbc_url, table="DIP.employees", properties=properties)
df.show()

