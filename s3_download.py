#!/usr/bin/python

import sys, os, csv, logging, shutil
from pathlib import Path
import argparse
import boto3


storage_threshold=0.1

class S3DownloadLogger(object):
        def __init__(self, filesize, filename):
                self._filename = filename
                self._size = filesize
                self._seen_so_far = 0
                self._seen_percentages = dict.fromkeys(range(0,100,10), False)

        def __call__(self, bytes_amount):
                self._seen_so_far += bytes_amount
                percentage = round((self._seen_so_far / self._size) * 100)
                if percentage in self._seen_percentages.keys() and not self._seen_percentages[percentage]:
                        self._seen_percentages[percentage] = True
                        logging.info(f"Downloaded {self._seen_so_far} of {self._size} bytes: {percentage}")

def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("inlist", default="01_inlist.csv", help="Local filepath to input CSV", type=str)
        parser.add_argument("outdir", default="01_s3_downloads", help="Local filepath to download destination folder",
                    type=str)
        parser.add_argument("outlist", default="01_outlist.csv", help="Local filepath to download results CSV",
                    type=str)
        args = parser.parse_args()
        return args
        

def s3_download(client, bucket, file, outpath, outlist_f):
        ''' Downloads a file from S3, writes results to CSV at outlist_f

        : param client: S3 object, s3 client object
        : param bucket: String, S3 bucket for download source
        : cell        : List of Strings, cell contents from input CSV
        : outpath     : String, local path for destination downloads from S3
        : outlist_f   : CSV reader object for output CSV of download results
        '''
        file_key = file.split("//")[-1]
        file_name = file.split("/")[-1]
        file_outpath = outpath + "/" + file_name
        s3_uri = "s3://" + bucket + "/" + file_key

        print("file_key: ", file_key)

        s3_obj = s3.head_object(
                Bucket=bucket,
                Key=file_key)

        logging.info(f"Starting download for '{file_key}'")
        download_logger =  S3DownloadLogger(s3_obj['ContentLength'], file_key)

        # Check if available space to download the file
        out_vol = Path(outpath).absolute().drive
        out_vol_stats = shutil.disk_usage(out_vol)

        if out_vol_stats.free - s3_obj['ContentLength'] < out_vol_stats.total * storage_threshold:
                print("Downloading file would exceed recommended storage threshold.")
                print("Exiting")
                exit()

        try:
                client.download_file(bucket, file_key, file_outpath,
                                     Callback=download_logger)
                logging.info(f"Downloaded {file_key}")
                outlist_f.writerow([file_outpath, file_name, s3_uri])   
        except:
                logging.info(f"Failed to download {file_key}")
        print("\n")

def download_loop(client, bucket, cell, outpath, outlist_f):
        ''' Loop warpper for s3_download().
            Iterates over 1 cell from input CSV,
            Downloads files from S3 for each object in file

        : param client: S3 object, s3 client object
        : param bucket: String, S3 bucket for download source
        : cell        : List of Strings, cell contents from input CSV
        : outpath     : String, local path for destination downloads from S3
        : outlist_f   : CSV reader object for output CSV of download results
        '''
        if len(cell) > 0:
                files = cell.split(";")
                print("        Number of files: ", len(files))

                num = 1
                for f in files:
                        print(f"File {num} of {len(files)}")
                        s3_download(client, bucket, f, outpath, outlist_f)
                        num += 1
        else: print("        NONE")

### VARIABLES ##################################################################
logging.basicConfig(level=logging.INFO)           # logging level
bucket = "car-archi-objects"                      # S3 bucket
input_fields={"obj_object_identifier": None,      # dict of fields from input CSV
        "obj_audio_files": None,
        "obj_moving_image_files": None}
s3 = boto3.client('s3')                           # S3 object
i = 0
### INPUT VALIDATION ###########################################################
# Args:
        # input: CSV of target files
        # output: Local destination
        # output: CSV of download results
args = get_args()
        
if not (os.path.exists(args.inlist)):
        print("Filepath for inlist not found: ", args.inlist, "Exiting.")
        exit()

if not (os.path.exists(args.outdir)):
        print("Output directory not found: ", args.outdir, "Creating now.")
        try:
                os.mkdir(args.outdir)
                print("Made output directory: ", args.outdir)
        except:
                print("Unable to create output directory. Exiting.")
                exit()

if not (os.path.exists(args.outlist)):
        print("Filepath for outlist not found: ", args.outlist, "Creating now.")
        try:
                f = open(args.outlist, "w", newline='')
                f.close()
                print("Created results file at outlist path: ", args.outlist)
        except:
                print("Unable to create results file: ", args.outlist, ". Exiting.")
                exit()
                
###############################################################################

### S3 DOWNLOAD BATCH-PROCESS LOOP ############################################
with open(args.inlist, newline='') as inlist_obj:
        in_reader = csv.reader(inlist_obj, delimiter=',')

        # Get indices of input fields
        header = next(in_reader)
        for c in header:
                for key, value in input_fields.items():
                        if value == None and c == key:
                                input_fields.update({key: i})
                i += 1
        num_rows = sum(1 for row in inlist_obj)
        inlist_obj.seek(0)
        next(in_reader)

        with open(args.outlist, "a", newline='') as outlist_obj:
                out_writer = csv.writer(outlist_obj, delimiter=',')
                i = 1
                # Batch-process loop
                for row in in_reader:
                        # Retrieve info from input CSV fields
                        obj_id = row[input_fields.get("obj_object_identifier")]
                        audio_cell = row[input_fields.get("obj_audio_files")]
                        vid_cell = row[input_fields.get("obj_moving_image_files")]

                        print(f"Row {i} of {num_rows}")
                        print("Object: ", obj_id)
                        
                        if audio_cell == None and vid_cell == None:
                                print("No audio or video objects.")
                                print("Skipping file.\n")
                                continue

                        print("Audio Files:")
                        download_loop(s3, bucket, audio_cell, args.outdir, out_writer)
                        
                        print("Video Files:")
                        download_loop(s3, bucket, vid_cell, args.outdir, out_writer)
                        i += 1
