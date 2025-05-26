from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from os import getenv
import pandas as pd
import boto3
import io
import logging

# -----------------
# Environment variables

AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL = getenv("AWS_ENDPOINT_URL")

app = FastAPI(
    title="Utility for analyzing S3 buckets and objects and generating summaries",
    version="1.0.0",
    description="Accesses an S3 instance buckets and objects"
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"]
)

# -------------------------------
# Pydantic models
# -------------------------------

# class S3Connection(BaseModel):
#    access_key_id: str
#    secret_access_key: str
#    url : str
#    region_name: str = 'us-east-1'

class S3ObjectInput(BaseModel):
    bucket_name: str = Field(..., description="The name of the S3 bucket.")
    object_key: str = Field(..., description="The key (name) of the S3 object.")

class S3BucketInput(BaseModel):
    bucket_name: str = Field(..., description="The name of the S3 bucket.")
    object_prefix: Optional[str] = Field(..., description="The prefix (directory) of the S3 objects.")

# -------------------------------
# Routes
# -------------------------------
@app.post(
    "/analyze_s3_object",
    summary="Analyze an S3 Object"
)
def analyze_s3_object(data: S3ObjectInput):
    """
    Analyze a file in an S3 bucket and generate a summary.
    """
    # Create an S3 client using the provided credentials
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        region_name='us-east-1',
        verify=True
    ) 

    try:
        # Check if the key has a valid extension (csv, xlsx, parquet)
        file_extension = get_file_extension(data.object_key)
        if file_extension is None:
            raise ValueError("The file does not have an allowed extension (csv, xlsx or parquet).")

        # Get the object from S3
        response = s3_client.get_object(Bucket=data.bucket_name, Key=data.object_key)

        # Convert the value to a pandas dataframe
        dataframe = object_to_dataframe(response['Body'], file_extension)
        return {
            "status": 200,
            "records": dataframe.head(50).to_dict('records')
        }
    except Exception as e:
        return {
            "message": f"The file could not be processed with error: {e}"
        }

@app.post(
    "/get_s3_object_stats",
    summary="Get statistics about a S3 object."
)
def get_s3_object_stats(data: S3ObjectInput):
    """
    Get statistics about a S3 object, including number of rows and columns and null value count and data type per column. 
    File size, informations about connection and other S3 headers are also provided.
    """
    # Create an S3 client using the provided credentials
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        region_name='us-east-1',
        verify=True
    ) 

    try:
        # Check if the key has a valid extension (csv, xlsx, parquet)
        file_extension = get_file_extension(data.object_key)
        if file_extension is None:
            raise ValueError("The file does not have an allowed extension (csv, xlsx or parquet).")

        # Check if the key has a valid extension (csv, xlsx, parquet)
        tags = s3_client.get_object_tagging(Bucket=data.bucket_name, Key=data.object_key)

        # Get the object from S3
        response = s3_client.get_object(Bucket=data.bucket_name, Key=data.object_key)

        # Convert the value to a pandas dataframe
        dataframe = object_to_dataframe(response['Body'], file_extension)
        
        # Get statistics about the data
        stats = get_object_statistics(dataframe)
        return {
            "status": 200,
            "metadata": tags,
            "statistics": stats
        }
    except Exception as e:
        return {
            "status": 400,
            "message": f"The file could not be processed with error: {e}"
        }


@app.post(
    "/get_s3_bucket_stats",
    summary="Get statistics about a S3 bucket."
)
def get_s3_object_stats(data: S3BucketInput):
    """
    Get statistics about a S3 bucket, including object count and the first objects in that bucket.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        region_name='us-east-1',
        verify=True
    ) 

    try:
        # Get the object from S3
        if data.object_prefix:
            response = s3_client.list_objects_v2(Bucket=data.bucket_name, Prefix=data.object_prefix)
        else:
            response = s3_client.list_objects_v2(Bucket=data.bucket_name)

        # Get statistics about the data
        if len(response.get("Contents",[])) > 0:
            objects = [
                {"Key": obj["Key"], "Size": obj["Size"]}
                for obj in response["Contents"]
            ]
        else:
            objects = []
        return {
            "status": 200,
            "bucket_name": response['Name'],
            "num_objects": response["KeyCount"],
            "objects": objects
        }
    except Exception as e:
        return {
            "status": 400,
            "message": f"The file could not be processed with error: {e}"
        }


# ------------------------------------------------
# Utilitary functions
# ------------------------------------------------

def get_file_extension(object_key):
    """
    Get the extension of a file from its object key.
    """
    # Split the object key into parts using '/' as the delimiter
    extension = object_key.split('.')[-1]
    if extension is not None:
        return extension
    return None

def object_to_dataframe(data, file_extension):
    """
    Convert an S3 object to a pandas dataframe.
    """

    if file_extension == "csv":
        # Read the CSV data into a pandas DataFrame
        df = pd.read_csv(io.BytesIO(data.read()), sep=';')
        return df
    elif file_extension == "parquet":
        df = pd.read_parquet(io.BytesIO(data.read()))
        return df
    else:
        raise ValueError(f"Unsupported file extension '{file_extension}'")

def get_object_statistics(data : pd.DataFrame):
    """
    Get the statistics of an S3 object.
    """

    columns = list(data.columns)
    column_types = {}
    stats = {
        "rows": {
            "count": len(data.index),
        },
        "columns": [
            {
                "name": col,
                "type": str(data[col].dtype),
                "null_value_count": int(data[col].isnull().sum())
            } for col in columns
        ]
    }
    return stats
