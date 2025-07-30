#!/usr/bin/python

from datetime import datetime
from enum import Enum

import sys, os, csv, time, gc
import argparse

import whisper, torch
from whisper.utils import get_writer
from pymediainfo import MediaInfo
import iso639
from iso3166_2 import *

class result_state(Enum):
    ERROR = 0
    SUCCESS = 1

# Constants for transcription
av_file_exts = ["wav","mp3","m4a","mov","mp4","webm","m4v","mpeg4"]
default_model = "large-v3"

# Constants for embedding metadata
fadgi_types = ["subtitle", "caption", "audio description",   # Vocab values for FADGI type
                "chapters", "metadata"]  
fadgi_party1 = "US, California Revealed"                     # Required value for FADGI Responsible Party
fadgi_fileCreator = "OpenAI Whisper"                         # Required value for WebVTT creator

iso = ISO3166_2()

# Read in input/output locations from command-line
# Args:
#	input CSV
# 	output: VTT destination folder
#	output: CSV of VTT locs
#	whisper config settings
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
        parser.add_argument("--w_settings", default=None,
                           help="Text file containing settings for Whisper",
                           type=str, required=False)
        args = parser.parse_args()
        return args


# Utility function for updating output log
def update_log(csv_writer, fpath, fname, msg, t_start, end_state):
    now = datetime.now()
    t_end = time.perf_counter()
    elapsed_time = t_end - t_start

    print(msg)
    print("Elapsed time: ", elapsed_time, "\n")

    try:
        csv_writer.writerow([fpath, fname, elapsed_time,
            msg, now.strftime("%Y/%m/%d %H:%M:%S"), end_state])
        print("Wrote to results output file")
    except:
        print("Unable to write results to output file")


# Utility function for exiting the script
def exit_msg(msg, msg_arg):
    print(msg, msg_arg)
    print("Exiting")
    exit()


def reset_mdata(mdata):
    mdata = {
        "type": fadgi_types[1],
        "lang": "",
        "party1": fadgi_party1,
        "party2": "",
        "mi": "",
        "mi_type": "",
        "og_file": "",
        "f_creator": fadgi_fileCreator,
        "fc_date": "",
        "title": "",
        "og_history": "",
        "local_key1": "",
        "local_value1": "",
        "local_key2": "",
        "local_value2": ""
    }
    return mdata

def validate_mdata(mdata):
    # Is Type compliant with FADGI type vocab?
    if not mdata["type"] in fadgi_types:
        return "Provided WebVTT type does not comply with FADGI vocabulary"

    # Do party fields have [Country], [Name] structure?
    try:
        p1_tokens = mdata["party1"].split(',')
        p1_country, p1_name = p1_tokens[0], p1_tokens[1]
    except:
        return "Responsible Party 1 does not follow [Country], [Partner Name] formatting"
    try:
        p2_tokens = mdata["party2"].split(',')
        p2_country, p2_name = p2_tokens[0], p2_tokens[1]
    except:
        return "Responsible Party 2 does not follow [Country], [Partner Name] formatting"

    # Do party fields have ISO-compliant country codes?    
    try: iso[p1_country]
    except:
        return "Country code for Responsible Party 1 does not comply with ISO 3166-2"

    try: iso[p2_country]
    except:
        return "Country code for Responsible Party 2 does not comply with ISO 3166-2"

    # Does media identifier follow CA-R object_identifier format?
    mi_tokens = mdata["mi"].split('_')
    if len(mi_tokens) != 2:
        return "Object Identifier does not have correct number of underscores"
    try:
        obj_number = int(mi_tokens[1])
    except:
        return "Object Identifier does not contain a number in expected position"

    # Does MI Type is local?
    if mdata["mi_type"] != "local":
        return "Media Identifier Type for CA-R object identifier is not local"

    # Are both key and value present for local pairs?
    keys = (k for k in mdata.keys() if 'local_key' in k)
    values = (v for v in mdata.keys() if 'local_value' in v)
    for k, v in zip(keys, values):
        if bool(mdata[k]) != bool(mdata[v]):
            return "Key and Value Fields must be used together if using Local Usage Elements"
    return ""

# Adds strongly-recommended metadata to WebVTT file header,
# following FADGI recommendations outined in 
# 'Guidelines for Embedding Metadata in WebVTT Files'
# June 7, 2024 version
#
# Needs: filepath of VTT, use to create file object
def write_fadgi_block(fpath, mdata):
    # Validate provided filepath for webVTT metadata embedding
    if not fpath:
        print("write_fadgi_block: Empty filepath")
        return False
    elif not os.path.exists(fpath):
        print("write_fadgi_block: Filepath does not exist for: ", fpath)
        return False
    elif fpath.split(".")[-1] != "vtt":
        print("write_fadgi_block: Expected VTT file")
        return False

    with open(fpath, "r") as f_reader:
        lines = f_reader.readlines()

    # Comments show example output for WebVTT embedded metadata
    # [brackets] indicate values from corresponding field names in AV Data Baseline reports
    with open(fpath, "w") as f_writer:
        f_writer.write(lines[0])
        lines.pop(0)

        f_writer.write("\n" + "Type: " + mdata["type"] + "\n")      # Type: caption
        f_writer.write("Language: " + mdata["lang"] + "\n")         # Type: language specified or detected during transcription 
        f_writer.write("Responsible Party: " + mdata["party1"]       
            + "; " +  mdata["party2"] + "\n")                       # Responsible party: US, California Revealed; US, [Partner Name]
        f_writer.write("Media Identifier: " + mdata["mi"]            
            + ", " + mdata["mi_type"] + "\n")                       # Media Identifier: [obj_object_identifier], local
        f_writer.write("Originating File: " 
            + mdata["og_file"] + "\n")                              # Originating File: [obj_object_identifier]_t1_access.mp3
        f_writer.write("File Creator: " 
            + mdata["f_creator"] + "\n")                            # File Creator: OpenAI Whisper         
        f_writer.write("File Creation Date: " 
            + mdata["fc_date"] + "\n")                              # File Creation Date: 2025-06-27
        f_writer.write("Title: " + mdata["title"] + "\n")           # Title: [label]                       
        f_writer.write("Origin History: " 
            + mdata["og_history"] + "\n")                           # Origin History: Created in response to 2024 website accessibility audit

        if (mdata["local_key1"] != "" and mdata["local_value1"] != ""):
            f_writer.write(mdata["local_key1"] + ": " 
                + mdata["local_value1"] + "\n")

        if (mdata["local_key2"] != "" and mdata["local_value2"] != ""):
            f_writer.write(mdata["local_key2"] + ": " 
                + mdata["local_value2"] + "\n")

        for l in lines:
            f_writer.write(l)

    return True

def main():
    # INPUT VALIDATION
    args = get_args()
    w_default = False
    # Validate input args
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
            f.close()
            print("Created output log at path: ", args.outlist)
        except:
            exit_msg("Failed to create output log at path: ",
             args.outlist)
    elif ((args.w_settings is None) or (not (os.path.exists(args.w_settings)))):
        print("Whisper settings file ", args.w_settings, " not found.")
        print("Using default settings instead")
        w_default = True
    print("Validated args")

    # Set up Whisper
    torch.cuda.init()
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    print("device: ", device)

    # Read and validate from whisper_settings
    # May need to revisit if we're just hard-coding
    w_settings = {}
    line_tokens = []

    if (not w_default):
        try:
            with open(args.w_settings) as ws:
                for line in ws.readlines():
                    line_tokens = line.split("=")
                    key = line_tokens[0]
                    value = line_tokens[1].strip()

                    print(key, "=", value)
                    
                    if ((value == 'True') or (value == 'False')):
                        w_settings.update({key: eval(value)})
                    else:
                        try:
                            v_int = int(value)
                            v_float = float(value)

                            if v_int == v_float:
                                w_settings.update({key: v_int})
                            else:
                                w_settings.update({key: v_float})
                        except:
                            w_settings.update({key: value})
        except:
            exit_msg("Bad whisper_settings line:", line_tokens)

    # Build DecodingOptions dict from w_settings
    decode_options = {
        'task': w_settings.get("task", "transcribe"),
        'language': w_settings.get("language", None),
        'sample_len': w_settings.get("sample_len", None),
        'best_of': w_settings.get("best_of", None),
        'beam_size': w_settings.get("beam_size", None),
        'patience': w_settings.get("patience", None),
        'length_penalty': w_settings.get("length_penalty", None),
        'prompt': w_settings.get("prompt", None),
        'prefix': w_settings.get("prefix", None),
        'suppress_tokens': w_settings.get("suppress_tokens", "-1"),
        'suppress_blank': w_settings.get("suppress_blank", True),
        'without_timestamps': w_settings.get("without_timestamps", False),
        'max_initial_timestamp': w_settings.get("max_initial_timestamp", 1.0),
        'fp16': w_settings.get("fp16", True)}

    print(decode_options)

    try:
        print("Loading model from w_settings: ", w_settings["model"])
        model = whisper.load_model(w_settings.pop("model"), 
            w_settings.pop("device"))
    except:
        print("Loading default model: ", default_model)
        model = whisper.load_model(default_model, device)
    print("Device: ", model.device)

    obj_mdata = {}

    # Batch-process loop
    with open(args.inlist, newline='') as inlist_obj:
        in_reader = csv.reader(inlist_obj, delimiter=',')

        n_rows = sum(1 for row in inlist_obj)
        inlist_obj.seek(0); next(in_reader)

        obj_mdata = reset_mdata(obj_mdata)

        with open(args.outlist, "a", newline='') as outlist_obj:
            out_writer = csv.writer(outlist_obj, delimiter=',')
            i=0
            prev_result, prev_file = "",""

            for row in in_reader:
                print(f"Row {i} of {n_rows}")

                t_start = time.perf_counter()
                i+=1

                # Build filename for output transcript            
                av_fpath, av_fname = row[0], row[1]
                av_f_ext = ((os.path.splitext(av_fname))[1])[1:]
                out_fname = (os.path.splitext(av_fname))[0] + ".vtt"
                out_fpath = args.outdir + "/" + out_fname
                
                # Parse FADGI metadata values from intake sheet to dict
                obj_mdata["party2"] = row[3]
                obj_mdata["mi"] = row[4]
                obj_mdata["mi_type"] = row[5]
                obj_mdata["og_file"] = av_fname
                obj_mdata["title"] = row[6]
                obj_mdata["og_history"] = row[7]
                obj_mdata["local_key1"] = row[8]
                obj_mdata["local_value1"] = row[9]
                obj_mdata["local_key2"] = row[10]
                obj_mdata["local_value2"] = row[11]

                mdata_check = validate_mdata(obj_mdata)
                if mdata_check:
                    update_log(out_writer, fpath=av_fpath, fname=av_fname,
                        msg=mdata_check,
                        t_start=t_start, end_state=result_state.ERROR.name)
                    continue

                print("Attempting Whisper transcription for: ", av_fname)

                # Check if target file exists
                if not (os.path.exists(av_fpath)):
                    print("Filepath does not exist for: ", av_fname)
                    update_log(out_writer, fpath=av_fpath,fname=av_fname,
                        msg="Target filepath does not exist. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Check if target file is an A/V file
                if not(av_f_ext in av_file_exts):
                    update_log(out_writer, fpath=av_fpath, fname=av_fname,
                        msg="Not a supported A/V file. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Check that filesize > 0B
                if os.path.getsize(av_fpath) == 0:
                    update_log(out_writer, fpath=av_fpath, fname=av_fname,
                        msg="Blank file. Skipping file.",
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

                # Check that at least 1 audio track is not blank
                skip = 0
                for at in file_mi.audio_tracks:
                    if at.duration > 0: break
                    else: skip+=1
                if skip == len(file_mi.audio_tracks):
                    update_log(out_writer, av_fpath, av_fname,
                        msg="All audio tracks blank. Skipping file.",
                        t_start=t_start, end_state=result_state.ERROR.name)
                    prev_file=av_fname
                    continue

                print("Passed pre-transcription file checks")

                try:
                    #Try ASR transcription                    
                    with torch.cuda.device(device):
                        result = model.transcribe(
                            av_fpath,
                            verbose=w_settings.get("verbose", False),
                            temperature=w_settings.get("temperature", (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)),
                            logprob_threshold=w_settings.get("logprob_threshold", -1.0),
                            no_speech_threshold=w_settings.get("no_speech_threshold", 0.6),
                            condition_on_previous_text=w_settings.get("condition_on_previous_text", False),
                            initial_prompt=w_settings.get("initial_prompt", None),
                            word_timestamps=w_settings.get("word_timestamps", False),
                            clip_timestamps=w_settings.get("clip_timestamps", "0"),
                            hallucination_silence_threshold=w_settings.get("hallucination_silence_threshold", None),
                            **decode_options)  
                except:
                    print("Transcription failed for: ", av_fname)  
                    update_log(out_writer, av_fpath, av_fname,
                        msg="Transcription failed", t_start=t_start,
                        end_state=result_state.ERROR.name)
                    continue

                obj_mdata["fc_date"] = datetime.today().strftime('%Y-%m-%d')
                gc.collect(); torch.cuda.empty_cache()   

                # Skip writing to VTT if blank transcript (no speech)
                if result["text"] == "":
                    update_log(out_writer, out_fpath, av_fname,
                        msg="Blank transcript", t_start=t_start,
                        end_state = result_state.ERROR.name)
                    prev_file = av_fname
                    continue

                # Skip writing to VTT if transcript duplicates prev file
                if result["text"] == prev_result:
                    update_log(out_writer, out_fpath, av_fname,
                        msg="Duplicate VTT of " + prev_file,
                        t_start=t_start,
                        end_state = result_state.ERROR.name)
                    prev_file = av_fname
                    continue
                else:
                    prev_result, prev_file = result, av_fname  

                # Validate langauge of transcription output
                if not (iso639.is_language(result["language"], "pt1")):
                    print("Non-ISO 639-3 language code provided")
                    update_log(out_writer, out_fpath, out_fname,
                        msg="Non-ISO 639-3 language code provided",
                        t_start=t_start,
                        end_state=result_state.ERROR.name)
                    continue
                
                #Convert Whisper's ISO 639-2 lang code to FADGI's 639-3 code
                lang = iso639.Lang(result["language"])
                obj_mdata["lang"] = lang.pt3
                print("Passed checks on transcription output")

                #Write transcription output to new VTT file
                try:
                    with open(out_fpath, "x+") as out_f:
                        vtt_writer = get_writer("vtt", out_fpath)
                        vtt_writer.write_result(result, out_f)
                except:
                    print("Failed to write to VTT file.")
                    update_log(out_writer, out_fpath, out_fname,
                        msg="Failed to write VTT", t_start=t_start,
                        end_state=result_state.ERROR.name)
                    continue
                    print("Wrote transcription output to WebVTT file")

                print("Attempting metadata embed")
                if (write_fadgi_block(out_fpath, obj_mdata) == False):
                    print("Failed to embed metadata")
                    update_log(out_writer, out_fpath, out_fname,
                        msg="Failed to embed metadata to VTT",
                        t_start=t_start,
                        end_state=result_state.ERROR.name)
                    continue
                print("Embedded metadata")

                # Write results to log file
                print("Successfully created transcript: ", out_fname)
                update_log(out_writer, out_fpath, out_fname,
                    msg="Successful transcription",
                    t_start=t_start,
                    end_state=result_state.SUCCESS.name)

            print("\n")
    
    print("Transcript file locations written to: " + args.outdir)

if __name__=="__main__":
    main()