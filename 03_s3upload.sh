#!/bin/bash

# Script 3 in California Revealed (CA-R) workflow
# for producing VTT audio transcripts using Whisper

# Uploads VTTs, specified in input .csv, to S3

### INPUTS ################################################

# Stem for test directory. Used to make absolute filepaths
test_dir="/mnt/d/Sept2024_to_June2025_CA-R_Internship/Workflow Scripts"

# Source list of VTT locations on local storage
# Output from car_whispervtt_02_autowhisper.sh
# Expected entries as: [VTT filepath],[VTT filename], [Source S3 folder]
#src_file=$test_dir"/02_outlist.csv"
src_file=$test_dir"/03_inlist.csv"

# Input list of destination S3 folders for upload
# Can use input list from Step 01
# Expected entries as: [local folder],[local file], [s3 folderpath]
dst_file=$test_dir"/02_inlist_s3.csv"


###########################################################

echo "###### START 03: VTT UPLOAD TO S3 ###########"


### INPUT VALIDATION ######################################

# Validation for src_file
# Check if src_file exists
if !([ -f "$src_file" ]); then
	echo "VTT list file does not exist."
	echo "Expecting .csv at path: ${src_file}"
	echo "Exiting."
	exit 66
# Check if src_file has entries
elif [ $(wc -l <"$src_file") -eq 0 ]; then
	echo "VTT list file contains 0 entries."
	echo "VTT list file location: ${src_file}."
	echo "Exiting."
	exit 66
# Check if src_file has 3 columns
elif !([ "$(head -n1 "$src_file" | grep -o ',' | wc -l)" -eq 2 ]); then
	echo "VTT list file has improper amount of columns."\
	"$(head -n1 "$src_file" | grep -o ',' | wc -l) columns found."\
	"2 columns expected."
	echo "VTT list file location: ${src_file}."
	echo "Exiting."
	exit 66
fi

echo "Passed checks for input files"

# Clear s3_dsts
if [ -f s3_dsts.csv ]; then
	rm s3_dsts.csv
fi
touch s3_dsts.csv

###########################################################


### S3 UPLOAD #############################################
 
tr -d '\r' < "$src_file" > s3_dsts.csv 

# Batch-upload VTTs to S3 using info from s3_dsts 
while IFS= read line
do 
	echo ""
	i=$((i+1))
	vtt_localPath=$(echo $line | cut -d ',' -f 1)
	vtt_name=$(echo $line | cut -d ',' -f 2)
	s3_path=$(echo $line | cut -d ',' -f 3)

	# Skip if row is malformed. 
	# Indicates missing or inaccurate VTT name or S3 path
	#if ([ -z "$vtt_localPath" -o "$vtt_localPath" == " " ]) ||\
	#	!([ -f "$vtt_localPath" ]); then
	if ([ -z "$vtt_localPath" -o "$vtt_localPath" == " " ]); then
		echo "Skipping S3 upload for row ${i}"
		echo "No VTT path provided."
		echo ""
		continue 
	elif !([ -f "$vtt_localPath" ]); then
		echo "Skipping S3 upload for row ${i}"
		echo "VTT not found at: ${vtt_localPath}"
		continue
	elif ([ -z "$vtt_name" -o "$vtt_name" == " " ]); then
		echo "Skipping S3 upload for row ${i]}"
		echo "Missing or incorrect VTT filename: ${vtt_name}"
		continue
	elif ([ -z "$s3_path" -o "$s3_path" == " " ]); then
		echo "Skipping S3 upload for row ${i}"
		echo "Missing S3 upload location."
		continue
	fi

	s3_folder=${s3_path%/*}
	# # Do we want to verify if we have the right S3 folder,
	# # Or trust that the S3 data is valid?
	# # Remember: this is the same S3 paths used in Script 01
	
	# Try S3 upload, remove VTT if successful
	{
		aws s3 cp "${vtt_localPath}" "${s3_folder}/${vtt_name}" &&
		echo "Successfuly uploaded ${vtt_name} to ${s3_folder}" &&
		sleep 1 
	} || {
		echo "Failed to upload ${vtt_name} to ${s3_folder}"
	}

done < s3_dsts.csv

echo "Finished script execution."
echo "###### END 03: VTT UPLOAD TO S3 ###########"
