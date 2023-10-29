# -*- coding: utf-8 -*-
from google.cloud import storage
import os
import sys
import subprocess

# staging="../csv_staging/"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/cvims_etl/prod/refresh/config/application_default_credentials.json"
bucket = "renault-dgc-kmr-69973-transit-ope"
# zip_to_archive="/coredrive/campaigns_stg/zip_csv"
import pandas as pd


def upload_blob(bucket_name, source_file_name, destination_blob_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

if __name__ == '__main__':
    source_file_path = sys.argv[1]
    source_file_path_csv = sys.argv[2]
    to_ingest   =  sys.argv[3]
    destination_blob_name = "dump_rb_cmp/"+source_file_path.split("/")[-1]
    # Add exception for that
    upload_blob(bucket, source_file_path,   destination_blob_name)

