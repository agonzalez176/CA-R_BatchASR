#!/usr/bin/python

from datetime import datetime
from enum import Enum

import sys, os, csv, time, gc
import argparse

import faster_whisper, whisperx, whisperx.utils, torch
from pymediainfo import MediaInfo

class result_state(Enum):
    ERROR = 0
    SUCCESS = 1

av_file_exts = ["wav","mp3","m4a","mov","mp4","webm","m4v","mpeg4"]

# Read in input/output locations from command-line
# Args:
#   input CSV
#   output: VTT destination folder
#   output: CSV of VTT locs
#   whisper config settings
def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("inlist", help="Local filepath to input CSV",
                            type=str)
        parser.add_argument("outdir", default="02_vtt_transcripts",
                            help="Local filepath to download destination",
                            type=str)
        parser.add_argument("outlist", default="02_outlist.csv",
                            help="Local filepath to download results CSV",
                            type=str)
##        parser.add_argument("w_settings", default="whisper_args.txt",
##                            help="Text file containing settings for WhisperX",
##                            type=str)
        args = parser.parse_args()
        return args


# Adds strongly-recommended metadata to WebVTT file header,
# following FADGI recommendations outined in 
# 'Guidelines for Embedding Metadata in WebVTT Files'
# June 7, 2024 version
#
# Needs: filepath of VTT, use to create file object
def write_fadgi_block(fpath, fname):
    if not fpath:
        print("write_fadgi_block: Empty filepath")
        exit()
    elif fpath.split(".")[-1] != "vtt":
        print("write_fadgi_block: Expected VTT file")
        exit()
    elif os.path.exists(fpath):
        print("write_fadgi_block: Filepath does not exist for: ", fpath)
        exit()

    fname_tokens = fname.split("_", 2)
    if len(fname_tokens) != 3:
        print("write_fadgi_block: Improperly formatted VTT name: ", fname)
        exit()
    f_id = fname_tokens[0] + "_" + fname_tokens[1]

    with open(fpath, 'w') as f:
        f.readline()
        f.write("Type: caption")
        f.write("Language: eng")
        f.write("Responsible party: US, California Revealed")
        f.write("Media Identifier: ", f_id)
        f.write("Originating File: ", fname)
        f.write("File Creator: California Revealed")
        f.write("File Creation Date: ", datetime.today().strftime('%Y-%m-%d'))
        f.write("Origin History: Created in response to CA-R's Web Accessibility Audit")


# Utility function for updating output log
def update_log(csv_writer, fpath, fname, msg, t_start, end_state):
    now = datetime.now()
    t_end = time.perf_counter()
    elapsed_time = t_end - t_start

    print(msg)
    print("Elapsed time: ", elapsed_time)

    #try:
    csv_writer.writerow([fpath, fname, elapsed_time,
        msg, now.strftime("%Y/%m/%d %H:%M:%S"), end_state])
    print("Wrote to results output file")
    #except:
    #    print("Unable to write results to output file"
    print("\n")

# Utility function for exiting the script
def exit_msg(msg, msg_arg):
    print(msg, msg_arg)
    print("Exiting")
    exit()


def main():
    ### INPUT VALIDATION ######################################################
    print("Let's start!")
    args = get_args()

    if not (os.path.exists(args.inlist)):
        exit_msg("Filepath for inlist not found: ", args.inlist)
    elif not (os.path.exists(args.outdir)):
        print("Output directory ", args.outdir, 
            " not found. Creating now")
        try:
            os.mkdir(args.outdir)
            print("Made output directory ", args.outdir)
        except:
            exit_msg("Unable to create output directory at path: ", 
                args.outdir)
    elif not (os.path.exists(args.outlist)):
        print("Output log ", args.outlist, " not found. Creating now")
        try:
            f = open(args.outlist, "w", newline='')
            f_writer = csv.writer(f, delimiter=',')
            f_writer.writerow(["Filepath", "Filename", "Elapsed Time", "Message", "Completion Time", "Endstate"])
            f.close()
            print("Created output log at path: ", args.outlist)

        except:
            exit_msg("Failed to create output log at path: ",
             args.outlist)
    print("Validated args")

    ###############################################################################

    ### ASR SET-UP ################################################################
    device = "cuda"
    batch_size = 6
    compute_type = "float16"

    gc.collect(); torch.cuda.empty_cache()
    model = whisperx.load_model("distil-large-v3",
                                 device, 
                                 compute_type=compute_type)
    print("Device: ", model.device)
    print("Set up model")


    ### BATCH-PROCESS LOOP ########################################################
    with open(args.inlist, newline='') as inlist_obj:
        in_reader = csv.reader(inlist_obj, delimiter=',')

        # Check for properly formatted input CSV
        n_col = len(next(in_reader))
        if n_col != 3:
            print("Unexpected number of columns in inlist file: ", 
                args.inlist)
            print("Found ", n_col, " columns. Expected 3. Exiting")
            inlist_obj.close()
            exit()
        n_rows = sum(1 for row in inlist_obj)
        inlist_obj.seek(0); next(in_reader)


        with open(args.outlist, "a", newline='') as outlist_obj:
            out_writer = csv.writer(outlist_obj, delimiter=',')

            i = 0
            prev_result, prev_file = "", ""

            for row in in_reader:
                t_start = time.perf_counter()
                i+= 1
                # Read from current row in inlist
                av_fpath, av_fname = row[0], row[1]

                # Build filename for output transcript
                out_fname = (os.path.splitext(av_fname))[0] 
                av_f_ext = ((os.path.splitext(av_fname))[1])[1:]
                out_fname += ".vtt"
                out_fpath = args.outdir + "/" + out_fname

                print(f"Row {i} of {n_rows}")
                print("Attempting WhisperX transcription for: ", av_fname)

                # Check if target file exists
                if not (os.path.exists(av_fpath)):
                    print("Bad path: ", av_fpath)
                    # Write to output log
                    update_log(out_writer, fpath=av_fpath, fname=av_fname,
                        msg="Target file does not exist. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Check if target file is an A/V file
                if not(av_f_ext in av_file_exts):
                    update_log(csv_writer=out_writer, fpath=av_fpath, 
                        fname=av_fname,
                        msg="Not a sound or video file. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Check if the file has already been transcribed
                if (os.path.exists(out_fpath)):
                    update_log(csv_writer=out_writer, fpath=av_fpath, 
                        fname=av_fname,
                        msg="This file has already been transcribed. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Check if the file has at least one audio track
                file_mi = MediaInfo.parse(av_fpath)
                if (len(file_mi.audio_tracks) == 0):
                    update_log(csv_writer=out_writer, fpath=av_fpath, 
                        fname=av_fname,
                        msg="No audio tracks to transcribe. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Attempt transcription, write results to VTT file
                try:
                    audio = whisperx.load_audio(av_fpath)
                    result = model.transcribe(
                        audio, 
                        batch_size=batch_size,
                        language="en",
                        task="transcribe",
                        print_progress=True)

                    # Check duplicate results here
                    if result == prev_result:
                        err_msg = "Duplicate VTT of " + prev_file
                        update_log(csv_writer=out_writer, fpath=out_fpath,
                            fname=av_fname, msg=err_msg, 
                            t_start=t_start, 
                            end_state=result_state.ERROR.name)
                        prev_file = av_fname
                        continue
                    else:
                        prev_result, prev_file = result, av_fname
                    # Move txt writer here, for successful outputs
                    try:
                        txt_writer = whisperx.utils.get_writer("vtt", args.outdir)
                        txt_writer(result, av_fname, 
                            {
                                "highlight_words": None,
                                "max_line_count": None,
                                "max_line_width": None   
                            })
                        print("Wrote to VTT: ", out_fname)
                    except:
                        print("Unable to write to VTT: ", out_fname)
                except:
                    print("Unable to transcribe ", av_fname)

                update_log(csv_writer=out_writer, fpath=out_fpath, 
                    fname=out_fname,
                    msg="Transcription successful",
                    t_start=t_start, end_state=result_state.SUCCESS.name)

                #delete model if low on GPU resources
                gc.collect(); torch.cuda.empty_cache()

                print("\n")

    print("Transcript file locations written to: " + args.outlist)

if __name__=="__main__":
    main()