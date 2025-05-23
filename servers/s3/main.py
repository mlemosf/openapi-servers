from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Literal
from dateutil import parser as dateutil_parser
from os import getenv
import pandas as pd
import pytz
import boto3
import io

#-----------------
# Environment variables

AWS_ACCESS_KEY_ID = getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL = getenv("AWS_ENDPOINT_URL")

app = FastAPI(
    title="Utility for analyzing S3 files and generating summaries",
    version="1.0.0",
    description="Accesses an S3 instance and analyzes a file."
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# -------------------------------
# Pydantic models
# -------------------------------

#class S3Connection(BaseModel):
#    access_key_id: str
#    secret_access_key: str
#    url : str
#    region_name: str = 'us-east-1'

class S3ObjectInput(BaseModel):
    bucket_name: str = Field(..., description="The name of the S3 bucket.")
    object_key: str = Field(..., description="The key (name) of the S3 object.")

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
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        region_name='us-east-1',
        verify=False
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
            "records": dataframe
        }
    except Exception as e:
        return {
            "message": f"The file could not be processed with error: {e}"
        }

@app.post(
    "/get_s3_object_metadata",
    summary="Get metadata about a file in s3"
)
def get_s3_object_metadata(data: S3ObjectInput):
    """
    Return object metadata
    """
    # Create an S3 client using the provided credentials
    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        region_name='us-east-1',
        verify=False
    ) 

    try:
        # Check if the key has a valid extension (csv, xlsx, parquet)
        tags = s3_client.get_object_tagging(Bucket=data.bucket_name, Key=data.object_key)['TagSet']
        return {
            "status": 200,
            "metadata": {
                "tags": tags
            }
            
        }
    except Exception as e:
        return {
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
        return df.head(50).to_dict('records')
    elif file_extension == "parquet":
        df = pd.read_parquet(io.BytesIO(data.read()))
        return df.head(50).to_dict('records')
    else:
        raise ValueError(f"Unsupported file extension '{file_extension}'")