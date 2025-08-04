import argparse
from classes import IngestionInputClass, InputTypeInputClass
from connectors.oracle import oracle_connector
import json
from pyspark.sql import SparkSession
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class Ingestion:
    
    def __init__(self):
        self.arguments = self.parse_arguments()
        self.input_type_input = InputTypeInputClass(
            input_type=self.arguments.input_type,
            merge_columns=json.loads(self.arguments.input_type_input)[0].get('merge_columns', []),
            cdc_columns=json.loads(self.arguments.input_type_input)[0].get('cdc_columns', []),
            delete_insert_columns=json.loads(self.arguments.input_type_input)[0].get('delete_insert_columns', [])
        )
        
        self.ingestion_input = IngestionInputClass(
            organization=self.arguments.organization,
            source_system=self.arguments.source_system,
            input_db_name=self.arguments.input_db_name,
            input_table_name=self.arguments.input_table_name,
            s3_bucket_name=self.arguments.s3_bucket_name,
            input_type=self.arguments.input_type,
            input_type_input=self.input_type_input,
            input_columns= json.loads(self.arguments.input_columns)[0],
            encrypted_columns= [] if  self.arguments.encrypted_columns == 'None' else json.loads(self.arguments.encrypted_columns),
            partition_columns= [] if self.arguments.partition_columns == 'None' else json.loads(self.arguments.partition_columns)
        )
        


        self.spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.oracle.database.jdbc:ojdbc8:19.3.0.0") \
            .enableHiveSupport() \
            .getOrCreate()
            
        self.sc = self.spark.sparkContext
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "test")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "test")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "localhost")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        
        self.spark.sparkContext.setLogLevel("OFF")
            
    def parse_arguments(self):
        parser = argparse.ArgumentParser(description="Ingestion script arguments")
        parser.add_argument('--organization', type=str, required=True, help='Input orginization name')
        parser.add_argument('--source_system', type=str, required=True,choices=['oracle'],default='oracle', help='Input the source system')
        parser.add_argument('--input_db_name', type=str, required=True, help='Input database name')
        parser.add_argument('--input_table_name', type=str, required=True, help='Input table name')
        parser.add_argument('--s3_bucket_name', type=str, choices=['json', 'csv', 'demo'], default='json', help='Output format')
        parser.add_argument('--input_type', type=str, required=True, choices=['cdc', 'fullload'],default='fullload', help='Define the Table In put type')
        parser.add_argument('--input_type_input', type=str, required=True, help='Input parameter for the input type')
        parser.add_argument('--input_columns', type=str, required=True, help='Input columns for the table and thier dtypes')
        parser.add_argument('--encrypted_columns', type=str, required=True, help='Encrypted Columns Map')
        parser.add_argument('--partition_columns', type=str, required=True, help='Partition Columns Map')
        return parser.parse_args()
        
    def oracle_connector_handler(self):
        
        #NOTE: For now oracle connections are hardcoded. Need to change this later to use the arguments
        
        oracle = oracle_connector(
                 username = 'SYS', 
                 password = 'MySecurePassword1', 
                 host = 'localhost', 
                 port = 1521, 
                 service_name= 'XEPDB1',
                 table_name = self.ingestion_input.input_db_name + '.' + self.ingestion_input.input_table_name,
                 spark=self.spark)
        
        oracle.handler(self.ingestion_input)
    
    
if __name__ == "__main__":
    ingestion = Ingestion()
    print("Parsed arguments:")
    print(ingestion.arguments)
    print("Input Type Input:")
    print(ingestion.input_type_input)
    ingestion.oracle_connector_handler()
    print("Oracle connector handler executed successfully.")
    