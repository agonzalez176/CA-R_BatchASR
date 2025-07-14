#!/usr/bin/python

import sys, os, csv, time, logging
import boto3, argparse
from datetime import datetime
from enum import Enum
from botocore.exceptions import ClientError

class result_state(Enum):
    ERROR = 0
    SUCCESS = 1

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("inlist", help="Local path to input CSV", type=str)
    parser.add_argument("log", help="Local path to log CSV", type=str)

    args = parser.parse_args()
    return args

def upload_file(s3_client, filename, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use filename
    print("object-name: ", object_name)
    if object_name is None:
        object_name = os.path.basename(filename)

    # Upload the file
    try:
        response = s3_client.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        time.sleep(1)
        return False
    time.sleep(1)
    return True

def update_log(log_writer, fpath, fname, uri, msg, result):
    now = datetime.now()
    print(msg)
    try:
        log_writer.writerow([fpath, fname, uri, now.strftime("%Y/%m/%d %H:%M:%S"),msg, result])
        print("Wrote to log")
    except:
        print("Unable to write to log")
    print("\n")

# Utility function for exiting the script
def exit_msg(msg, msg_arg):
    print(msg, msg_arg)
    print("Exiting")
    exit()

### VARIABLES ##################################################################
logging.basicConfig(level=logging.INFO)    # logging level
bucket = "car-archi-objects"               # S3 bucket
s3_client = boto3.client('s3')             # S3 client object

### INPUT VALIDATION ###########################################################
# Receive and validate input arguments from command line
# Arguments:
#     inlist: filepath to CSV containing VTT local paths, S3 URIs for S3 upload 
#     log: filepath for CSV log of upload jobs
def main():
    args = get_args()
    if not(os.path.exists(args.inlist)):
        exit_msg("Filepath for inlist not found: ", args.inlist)
    elif not (os.path.exists(args.log)):
        print("Output log ", args.log, ". Creating now")
        try:
            with open(args.log, "w", newline='') as log_obj:
                log_writer = csv.writer(log_obj, delimiter=',')
                log_writer.writerow(["Filepath", "Filename", "S3 URI"])
            print("Made output log at path: ", args.log)
        except:
            exit_msg("Unable to create output log at path: ", args.log)
    print("Validated args\n")
    ################################################################################

    ### S3 UPLOAD ###############################################
    with open(args.inlist, newline='') as inlist_obj:
        in_reader = csv.reader(inlist_obj, delimiter=',')

        # Validate structure of input CSV
        n_col = len(next(in_reader))
        if n_col != 3:
            print("Unexpected number of columns in inlist file: ", args.inlist)
            print(f"Found {n_col} columns. Expected 3")
            print("Exiting")
            exit()
        num_rows = sum(1 for row in inlist_obj)
        inlist_obj.seek(0); next(in_reader)
        i=0

        with open(args.log, "a", newline='') as log_obj:
            log_writer = csv.writer(log_obj, delimiter=',')

            # Batch-upload loop
            for row in in_reader:
                i += 1
                # Retrieve fields from input CSV
                f_path, f_name, f_s3uri = row[0], row[1], row[2]

                # Build S3 URI for VTT
                f_key = ("/".join(f_s3uri.split('/')[3:]))
                fpath_obj = "_".join((f_path.split("\\")[-1]).split('_')[0:2])
                fname_obj = "_".join(f_name.split('_')[0:2])
                s3_obj = "_".join((f_key.split("/")[2]).split('_')[0:2])

                print(f"Row {i} of {num_rows}: Attempting upload for ", f_name)

                ### Per-row input validation
                # Check if local filepath exists
                if not os.path.exists(f_path):
                    update_log(log_writer, f_path, f_name, f_s3uri, "Invalid filepath",
                        result_state.ERROR.name)
                    continue
                # Check if S3 object folder matches filename
                elif not fpath_obj == s3_obj or not fname_obj == s3_obj:
                    update_log(log_writer, f_path, f_name, f_s3uri, 
                        "S3 object doesn't match filepath or filename",
                        result_state.ERROR.name)
                    continue
                # Check if all paths and S3 URIs end in .vtt 
                elif not f_path.endswith(".vtt") or not f_name.endswith(".vtt") \
                    or not f_key.endswith(".vtt"):
                    update_log(log_writer, f_path, f_name, f_s3uri,
                        "Path for non-VTT file provided. Skipping upload",
                        result_state.ERROR.name)
                    continue

                # Attempt S3 upload
                if not upload_file(s3_client, f_path, bucket, f_key):
                    update_log(log_writer, f_path, f_name, f_s3uri,
                        "Failed to upload", result_state.ERROR.name)
                    continue
                update_log(log_writer, f_path, f_name, f_s3uri,
                    "Successful upload", result_state.SUCCESS.name)

if __name__=="__main__":
    main()