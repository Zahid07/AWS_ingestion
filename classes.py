from pydantic import BaseModel, Field
from typing import List, Dict


class InputTypeInputClass(BaseModel):
    """
    Represents the input for the input type.
    """
    input_type: str = Field(..., description="Input type for the table")
    merge_columns: List[str]  = Field(None, description="Columns for merge operation")
    cdc_columns: List[str] = Field(None, description="Columns for CDC operation")
    delete_insert_columns: List[str] = Field(None, description="Columns for delete-insert operation")
    

class IngestionInputClass(BaseModel):
    """
    Represents the input for data ingestion.
    """
    organization: str = Field(..., description="Input organization name")
    source_system: str = Field('oracle', description="Input the source system", choices=['oracle'])
    input_db_name: str = Field(..., description="Input database name")
    input_table_name: str = Field(..., description="Input table name")
    s3_bucket_name: str = Field('json', description="Output format", choices=['json', 'csv', 'demo'])
    input_type: str = Field('ignore-insert', description="Define the Table Input type", choices=['merge', 'cdc', 'ignore-insert', 'delete-insert'])
    input_type_input: InputTypeInputClass = Field(..., description="Input parameter for the input type")
    input_columns: Dict = Field(..., description="Input columns for the table")
    encrypted_columns: List[str] | None = Field(..., description="Encrypted Columns Map")
    partition_columns: List[str] | None = Field(..., description="Partition Columns Map")