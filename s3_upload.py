#!/usr/bin/python

import sys, os, csv, logging
import boto3
from botocore.exceptions import ClientError

def upload_file(s3_client, filename, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use filename
    if object_name is None:
        object_name = os.path.basename(filename)

    # Upload the file
    try:
        response = s3_client.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True    

### VARIABLES ##################################################################
logging.basicConfig(level=logging.INFO)    # logging level
bucket = "car-archi-objects"               # S3 bucket
s3_client = boto3.client('s3')             # S3 client object

### INPUT VALIDATION ###########################################################
# Receive and validate input arguments from command line
# Arguments:
#     inlist_path: filepath to CSV containing VTT local paths, S3 URIs for S3 upload 

n = len(sys.argv)
if n != 2:
    print(f"Expected 2 arguments. Received {n}")
    print("Exiting")
    exit()
inlist_path = sys.argv[1]

if not(os.path.exists(inlist_path)):
    print("Filepath for inlist not found: ", inlist_path)
    print("Exiting")
    exit()

################################################################################

### S3 UPLOAD ###############################################
with open(inlist_path, newline='') as inlist_obj:
    in_reader = csv.reader(inlist_obj, delimiter=',')

    # Validate structure of input CSV
    n_col = len(next(in_reader))
    if n_col != 3:
        print("Unexpected number of columns in inlist file: ", inlist_path)
        print(f"Found {n_col} columns. Expected 3")
        print("Exiting")
        exit()
    num_rows = sum(1 for row in inlist_obj)+ 1
    inlist_obj.seek(0)
    i=0

    # Batch-upload loop
    for row in in_reader:
        i += 1
        # Retrieve fields from input CSV
        f_path, f_name, f_s3uri = row[0], row[1], row[2]

        # Build S3 URI for VTT
        f_key = ("/".join(f_s3uri.split('/')[3:])).split('.')[0] + ".vtt"
        fpath_obj = "_".join((f_path.split("\\")[-1]).split('_')[0:2])
        fname_obj = "_".join(f_name.split('_')[0:2])
        s3_obj = "_".join((f_key.split("/")[2]).split('_')[0:2])

        print(f"Row {i} of {num_rows}: Attempting upload for ", f_name)

        ### Per-row input validation
        # Check if local filepath exists
        if not os.path.exists(f_path):
            print(f"Invalid path for {f_name}. Skipping upload")
            print(f_path)
            continue
        # Check if S3 object folder matches filename
        elif not fpath_obj == s3_obj or not fname_obj == s3_obj:
            print(f"Conflicting S3 object folders provided. Skipping upload")
            continue
        # Check if all paths and S3 URIs end in .vtt 
        elif not f_path.endswith(".vtt") or not f_name.endswith(".vtt") \
            or not f_key.endswith(".vtt"):
            print("Path for non-VTT file provided. Skipping upload")
            continue

        # Attempt S3 upload
        if not upload_file(s3_client, f_path, bucket, f_key):
              logging.info(f"Failed to upload {f_name}")
              i += 1
              continue
        print(f"Successfully uploaded {f_name}\n")
