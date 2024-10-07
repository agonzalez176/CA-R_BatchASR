#!/bin/bash

# Script 2 in California Revealed (CA-R) workflow for 
# VTT audio transcript using Whisper.

# Runs Whisper on a .csv of input filepaths to produce VTTs

### CONSTANTS #############################################

# Stem for test directory. Used to make absolute filepaths
test_dir="/mnt/d/Sept2024_to_June2025_CA-R_Internship/Workflow Scripts"

# Input list of audio file locations on local storage
# Output from car_whispervtt_01_s3download.sh
in_file=$test_dir"/02_inlist.csv"
#in_file=$test_dir"/01_outlist.csv"
#in_file=$test_dir"/02_inlist_valid_single.csv"

# Output directory for VTT Transcripts
out_dir=$test_dir"/02_vtt_transcripts"

# Output csv of filepaths for VTTs
vtt_locs=$test_dir"/02_outlist.csv"


### WHISPER PARAMETERS ####################################

w_lang="en"

# w_model="large-v3"
#w_model="large"
#w_model="medium"
w_model="small"
# w_model="base"
#w_model="tiny"

w_task="transcribe"

###########################################################


echo "###### START 02: WHISPER VTT TRANSCRIPTION ###########"

### ARGUMENT VERIFICATION #################################

if !([ -f "$in_file" ]); then
	echo "in_file "$in_file" does not exist"
	exit 66
# Check that in_file has at least 1 entry
elif [ "$(wc -l <"$in_file")" -eq 0 ]; then
	echo "in_file "$in_file" contains 0 files to download"
	exit 66
# Check if infile has 3 columns
# Script 02 uses infile cols 1,2. Col 3 is used in Script 03
elif !([ "$(head -n1 "$in_file" | grep -o ',' | wc -l)" -eq 2 ]); then
	echo "in_file has Improper amount of columns. 2 columns expected."
	echo "Number of columns found: " $(head -n1 "$in_file" | grep -o ',' | wc -l)
	exit 66
elif !([ "$(head -n1 "$in_file" | cut -d ',' -f 1) == "Filepath"" ]); then
	echo "in_file is missing Filename column." 
	echo "Exiting"
	exit
elif !([ "$(head -n1 "$in_file" | cut -d ',' -f 2) == "Filename"" ]); then
	echo "in_file is missing Filename column."
	echo "Exiting."
	exit
elif !([ "$(head -n1 "$in_file" | cut -d ',' -f 1) == "S3 URI"" ]); then
	echo "in_file is missing S3 URI column."
	echo "Exiting."
	exit
fi


# Verify that out_dir exists
if !([ -d "$out_dir" ]); then
	mkdir "$out_dir"
fi

# Clear vtt_locs
if [ -f "$vtt_locs" ]; then
	rm "$vtt_locs"
fi
touch "$vtt_locs"

###########################################################


### WHISPER TRANSCRIPTION #################################
echo "Start Whisper transcription"

if [ -f temp.csv ]; then
	rm temp.csv
fi
touch temp.csv




# Run Whisper on filepaths from in_file
while IFS= read line
do

	echo ""
	echo "Try Whisper transcription for: ${line}"
	filepath=$(echo $line | cut -d ',' -f 1)
	filename=$(echo $line | cut -d ',' -f 2)
	
	if [  "$filepath" == "Filepath" ]; then
		echo "Skipping header row."
		continue
	fi


	# Skip transcription for missing files
	if !([ -f "$filepath" ]); then
		echo "Skipping transcription. Missing file: ${filename}"
		continue
	fi

	# Skip transcription for missing filename.
	# Technically isn't necessary, but skips needing to pull 
	# filename from full filepath
	if [ -z $filename -o $filename == " "  ]; then
		echo "Skipping translation. Missing filename for: ${filepath}"
		continue
	fi

	{
 	whisper "$filepath" --output_dir "$out_dir" \
 		--output_format vtt \
 		--lang $w_lang \
 		--model $w_model \
 		--task $w_task \
 		--fp16 False \
 		--condition_on_previous_text False \
 		--verbose False &&
	
 	#	Strip extension from filename to match Whisper output
		fname=$(echo "${filename%.*}")
	 	echo "${out_dir}/${fname}.vtt,${fname}.vtt" >> temp.csv
	 	echo "Successful transcription for: ${filename}"
	 } || {
	 	echo "Failed to make transcript for: "$line
	 }
done < "$in_file"

# We only write to vtt_locs after completing the loop
tr -d '\r' < temp.csv > "$vtt_locs"
rm temp.csv


### VTT VERIFICATION ######################################
# Check if VTTs were created successfully
echo ""
echo "Verifying VTTs"
while IFS= read line
do
	filepath=$(echo $line | cut -d ',' -f 1)
	filename=$(echo $line | cut -d ',' -f 2)

	if !([ -f "$filepath" ]); then
		echo "Failed to make VTT for: ${filename}"
	fi
	# Check for proper VTT formatting?

done < "$vtt_locs"

echo "Finished script execution."
echo "###### END 02: WHISPER VTT TRANSCRIPTION ###########"
