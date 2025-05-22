from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware


from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Literal
import pytz
from dateutil import parser as dateutil_parser

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

class S3Connection(BaseModel):
    access_key_id: str
    secret_access_key: str
    url : str
    region_name: str = 'us-east-1'

class S3Object(BaseModel):
    bucket_name: str = Field(..., description="The name of the S3 bucket.")
    object_key: str = Field(..., description="The key (name) of the S3 object.")

# -------------------------------
# Routes
# -------------------------------
@app.get("/set_s3_connection", summary="Set connection to an S3 instance")
def set_s3_connection(s3_conn:S3Connection):
    """
    Sets the connection to an S3 instance.
    """

    