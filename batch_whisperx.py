#!/usr/bin/python

import sys, os, csv
import time
import argparse
import gc

import faster_whisper
import whisperx, whisperx.utils
import torch



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

### INPUT VALIDATION ##########################################################
def main():
    args = get_args()

    if not (os.path.exists(args.inlist)):
        print("Filepath for inlist not found: ", args.inlist)
        print("Exiting")
        exit()

    if not (os.path.exists(args.outdir)):
        print("Output directory ", args.outdir, " not found. Creating now")
        try:
            os.mkdir(args.outdir)
            print("Made output directory ", args.outdir)
        except:
            print("Unable to create output directory at path: ", args.outdir)
            print("Exiting")
            exit()

    if not (os.path.exists(args.outlist)):
        print("Results output file not found at path: ", args.outlist)
        print("Creating now")

        try:
            f = open(args.outlist, "w", newline='')
            f.close()
            print("Created results output file at desired path: ", args.outlist)
        except:
            print("Unable to create results output file at path: ", args.outlist)
            exit()
    print("Validated args")

    ###############################################################################

    ### ASR SET-UP ################################################################
    device = "cuda"
    batch_size = 6
    compute_type = "float16"

    gc.collect(); torch.cuda.empty_cache()
    model = whisperx.load_model("distil-large-v3",
                                 device, 
                                 compute_type=compute_type,
                                 language="en")
    print("Device: ", model.device)
    print("Set up model")



    ### BATCH-PROCESS LOOP ########################################################
    with open(args.inlist, newline='') as inlist_obj:
        in_reader = csv.reader(inlist_obj, delimiter=',')

        # Check for properly formatted input CSV
        n_col = len(next(in_reader))
        if n_col != 3:
            print("Unexpected number of columns in inlist file: ", args.inlist)
            print("Found ", n_col, " columns. Expected 3. Exiting")
            inlist_obj.close(); outlist_obj.close()
            exit()
        n_rows = sum(1 for row in inlist_obj)
        inlist_obj.seek(0)


        with open(args.outlist, "a", newline='') as outlist_obj:
            out_writer = csv.writer(outlist_obj, delimiter=',')

            i = 0
            for row in in_reader:
                t_start = time.perf_counter()
                i+= 1
                # Read from current row in inlist
                av_fpath, av_fname = row[0], row[1]

                # Build filename for output transcript
                out_fname = (os.path.splitext(av_fname))[0]
                out_fname += ".vtt"
                out_fpath = args.outdir + "/" + out_fname

                print(f"Row {i} of {n_rows}")
                print("Attempting WhisperX transcription for: ", av_fname)

                # Check if target file exists
                if not (os.path.exists(av_fpath)):
                    print("Filepath does not exist for: ", av_fname)
                    print("Filepath: ", av_fpath)
                    print("Skipping transcription for: ", av_fname)

                    t_end = time.perf_counter()
                    print("Elapsed Time: ", t_end - t_start)
                    continue
                
                if (os.path.exists(out_fpath)):
                    print("A transcript already exists for: ", av_fname)
                    print("Transcript location: ", out_fpath, "\n")
                    print("Skipping transcription")

                    t_end = time.perf_counter()
                    print("Elapsed Time: ", t_end - t_start)
                    continue
     
                try:
                    audio = whisperx.load_audio(av_fpath)
                    result = model.transcribe(
                        audio, 
                        batch_size=batch_size,
                        language="en",
                        task="transcribe",
                        print_progress=True)

                except:
                    print("Unable to transcribe ", av_fname)

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

                try:
                    out_writer.writerow([out_fpath, out_fname])
                    print("Wrote to results output file")
                except:
                    print("Unable to write results to output file")

                #delete model if low on GPU resources
                gc.collect(); torch.cuda.empty_cache()

                t_end = time.perf_counter()
                print("Elapsed Time: ", t_end - t_start)
                print("\n")

    print("Transcript file locations written to: " + args.outlist)

if __name__=="__main__":
    main()