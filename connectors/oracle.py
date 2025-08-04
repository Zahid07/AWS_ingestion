
from classes import IngestionInputClass
from pyspark.sql.functions import to_date


class oracle_connector:
    """
    This class handles the connection to an Oracle database and provides methods for executing queries.
    """
    def __init__(self, 
                 username = 'SYS', 
                 password = 'MySecurePassword1', 
                 host = 'localhost', 
                 port = 1521, 
                 service_name= 'XEPDB1',
                 table_name = 'DIP.employees',
                 spark=None):
        """
        Initializes the Oracle connector with the provided credentials and connection details.
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.table_name = table_name
        self.spark = spark
        self.jdbc_url = f"jdbc:oracle:thin:@{self.host}:{self.port}/{self.service_name}?internal_logon=SYSDBA"
        self.properties = {
            "user": self.username,
            "password": self.password,
            "driver": "oracle.jdbc.OracleDriver",
        }
        self.write_parquet_path = f"s3a://dp-testing-pyspark/employees"
        
        
    def fullload_handler(self, table_name, db_name):
        """
        Placeholder for merge operation logic.
        """
        
        # connect using trino to the given table name and the db name
        # delete the data using trino
        # insert the data into the table using spark with the view created
        
        pass
    
    
    def cdc_handler(self, table_name, db_name, merge_columns, cdc_columns):
        """
        Placeholder for CDC operation logic.
        """
        
        # fetch new data based on the timestamp from the cdc table
        # move data to a staging table 
        # now merge the data with the main table using the merge columns
        
        pass
    
    def generate_merge_query(self, table_name, merge_columns):
        """
        Generates a SQL query for merging data into the specified table.
        """
        pass
   
   
    
    def handler(self, ingestion_input: IngestionInputClass ):
        """
        Handles the ingestion process for the Oracle database.
        """
        
        print(f"Handling ingestion for table: {self.table_name} with input: {ingestion_input.input_type}")
        
        df = self.spark.read.jdbc(url=self.jdbc_url, table=self.table_name, properties=self.properties)
        df.createOrReplaceTempView(f"{ingestion_input.input_table_name}_view")
        
        
        
        
        
        
    
        
        # # df.show()
       
        # df = df.withColumn("dob", to_date("dob"))
        # df.write \
        #     .mode("append") \
        #     .option("compression", "gzip") \
        #     .parquet(write_parquet_path)
        
        pass
        
    