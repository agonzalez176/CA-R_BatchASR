#!/bin/bash

# Process File Assignment Audit Export report from 
# CA-R Repository to expected input format to 
# 01_s3download.sh


# Read input csv line by line
# For each line:
#	Extract contents of cols 13, 14
# 	Tokenize cols 13, 14 to get individual S3 links
# 	Insert bucket name 'car-archi-objects' into each S3 link to get full URI

### SETUP #####################################################################
in_file="01_inlist.csv"
out_dir="01_S3_Downloads"
out_dload_locs="01_outlist.csv"

#Names for desired object ID, audio file, moving image file 
#fields in input CSV
obj_id_field="obj_object_identifier"
audio_fs_field="obj_audio_files"
vid_fs_field="obj_moving_image_files"


# Verify paths for input/output files, download destination folder  
if !([  -f "$in_file" ]); then
	echo "Input CSV does not exist"
	echo "Input CSV filepath: ${in_file}"
	exit 66
elif !([  -d "$out_dir" ]); then
	echo "Download destination folder does not exist"
	echo "Destination folder path: ${out_dir}"
	exit 66 
elif !([  -f "$out_dload_locs" ]); then
	echo "Output CSV does not exist"
	echo "Output CSV folder path: ${out_dload_locs}"
	exit 66 
fi


touch in_list.csv
echo "$in_file" > in_list.csv
tr -d '\r' < "$in_file" > in_list.csv

if [ -f out_temp.csv ]; then
	rm out_temp.csv
fi
touch out_temp.csv

###############################################################################



### S3 DOWNLOAD LOOP ##########################################################

# Get column positions for object_identifier, audio files, 
# and video files from header row

obj_id_pos=$(head -n1 in_list.csv | tr ',' '\n' | grep -nx $obj_id_field | cut -d ':' -f1)
audio_fs_pos=$(head -n1 in_list.csv | tr ',' '\n' | grep -nx $audio_fs_field | cut -d ':' -f1)
vid_fs_pos=$(head -n1 in_list.csv | tr ',' '\n' | grep -nx $vid_fs_field | cut -d ':' -f1)

if [ -z "$obj_id_pos" -o "$obj_id_pos" == " " ]; then
	echo "No object field ${obj_id_field}"
	echo "Exiting."
	exit 
elif [ -z "$audio_fs_pos" -o "$audio_fs_pos" == " " ]; then
	echo "No audio file field ${audio_fs_field}"
	echo "Exiting."
	exit
elif [ -z "$vid_fs_pos" -o "$vid_fs_pos" == " " ]; then
	echo "No moving image file field ${vid_fs_field}"
	echo "Exiting."
	exit
fi

echo "obj_id_pos: ${obj_id_pos}"
echo "audio_fs_pos: ${audio_fs_pos}"
echo "vid_fs_pos: ${vid_fs_pos}"

# Run S3 cp commands for each object's associated audio, video files 
while IFS= read line
do

	# Skip header row
	obj_id=$(echo $line | cut -d ',' -f $obj_id_pos)
	echo "${obj_id}"
	if [ "$obj_id" == "obj_object_identifier" ]; then
		echo "Skipped header row"
		continue
	fi

	# Build audio, video arrays
	audio_fs=$(echo $line | cut -d ',' -f $audio_fs_pos)
	vid_fs=$(echo $line | cut -d ',' -f $vid_fs_pos)

	echo "Object: ${obj_id}"

	# Extract audio, video files from export CSV
	readarray -d ";" -t audio_arr <<< "$audio_fs"
	readarray -d ";" -t vid_arr <<< "$vid_fs"

	# Skip if object has no associated audio file
	if [ -z "${audio_fs}" -o "$audio_fs" == " " ]; then
		:
	else
		# Audio Loop
		echo "Number of audio files: ${#audio_arr[*]}"
		#echo "conditional test: $((${#audio_arr[*]} - 1))"

		# For each object, run S3 cp command for each associated audio file
		for ((  n=0; n < ${#audio_arr[*]}; n++ ))
		do
			# Remove trailing line feed char from last audio file
			if [ $n -eq $((${#audio_arr[*]} - 1)) ]; then
				audio_arr[$n]=${audio_arr[$n]%?}
			fi
			echo "${audio_arr[$n]}"
			# Run S3 cp commnad
			fname_audio=${audio_arr[n]##*/}
 			s3uri_audio="s3://car-archi-objects${audio_arr[n]#*/}"
 			#echo "aws s3 cp --dryrun ${s3uri_audio} ${out_dir}/${fname_audio}"	
 			aws s3 cp --dryrun "${s3uri_audio}" "${out_dir}/${fname_audio}"
	 		
	 		# Write to outlist
	 		# [local filepath],[filename],[s3 link]
 			echo "${out_dir}/${fname_audio},${fname_audio},${s3uri_audio}" >> out_temp.csv
		done	
	fi 

	echo ""

	# Skip if object has no associated video file
	if [ -z "$vid_fs" -o "$vid_fs" == " " ]; then
		:
	else
		# Video Loop
		echo "Number of video files for ${obj_id}: ${#vid_arr[*]}"

		# For each object, run S3 cp command for each associated video file
		for ((  n=0; n < ${#vid_arr[*]}; n++ ))
		do
			# Remove trailing line feed char from last video file
			if [ $n -eq $((${#vid_arr[*]} - 1)) ]; then
				vid_arr[$n]=${vid_arr[$n]%?}
			fi
			echo "${vid_arr[$n]}"
			# Run S3 cp command
			fname_vid=${vid_arr[n]##*/}
		 	s3uri_vid="s3://car-archi-objects${vid_arr[n]#*/}"
		 	#echo "aws s3 cp ${s3uri_vid} ${out_dir}/${fname_vid}"
		 	aws s3 cp --dryrun "$s3uri_vid" "${out_dir}/${fname_vid}"

		 	# Write to outlist
		 	echo "${out_dir}/${fname_vid},${fname_vid},${s3uri_vid}" >> out_temp.csv

		done
	fi
	echo ""
	echo "########"
done < in_list.csv

tr -d '\r' < out_temp.csv > "$out_dload_locs"
rm out_temp.csv
