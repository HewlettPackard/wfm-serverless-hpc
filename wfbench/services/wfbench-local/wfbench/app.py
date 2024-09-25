#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021-2023 The WfCommons Team.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

import os
import sys
import re
import time
import subprocess
import pathlib
from flask import Flask, request, jsonify

from wfbench import *

import argparse
import signal
import sys
from io import StringIO
from filelock import FileLock

import json
import pandas as pd

from typing import List, Optional

app = Flask(__name__)

def update_args_from_json(args, json_data):
    for key, value in json_data.items():
        if key == 'cpu-work':
            value = int(value)
        
        if key == 'percent-cpu':
            value = float(value)

        setattr(args, key.replace("-", "_"), value)
        
    return args

@app.route('/wfbench', methods=['POST'])
def wfbench():

    print(request.json)
    parser = get_parser()
    args, other = parser.parse_known_args()
    # Assuming you have received a JSON string as a POST request
    # For demonstration purposes, let's pretend this is the JSON received in the POST request
    
    json_from_request = request.json
    #print(json_from_request)

    # Parse the JSON data
    #json_data = json.loads(json_from_request)

    # Update argparse object with JSON data
    update_args_from_json(args, json_from_request)

    # Now you can use the args object as usual
    print(args)

    core = None
    if args.path_lock and args.path_cores:
        path_locked = pathlib.Path(args.path_lock)
        path_cores = pathlib.Path(args.path_cores)
        core = lock_core(path_locked, path_cores)

    print(f"[WfBench] Starting {args.name} Benchmark\n")

    mem_bytes = args.mem * 1024 * 1024 if args.mem else None

    if args.out and args.inputs and args.workdir:
        io_read_benchmark_user_input_data_size(args.inputs, args.workdir, memory_limit=mem_bytes)
    
    if args.gpu_work:
        print("[WfBench] Starting GPU Benchmark...")
        available_gpus = get_available_gpus() #checking for available GPUs

        if not available_gpus:
            print("No GPU available")
        else:
            device = available_gpus[0]
            print(f"Running on GPU {device}")
            gpu_benchmark(args.gpu_work, device, time=args.time)
    
    if args.cpu_work:
        print("[WfBench] Starting CPU and Memory Benchmarks...")
        if core:
            print(f"[WfBench]  {args.name} acquired core {core}")

        cpu_procs = cpu_mem_benchmark(cpu_threads=int(10 * args.percent_cpu),
                                    mem_threads=int(10 - 10 * args.percent_cpu),
                                    cpu_work=sys.maxsize if args.time else int(args.cpu_work),
                                    core=core,
                                    total_mem=args.mem)
        
        if args.time:
            time.sleep(int(args.time))
            for proc in cpu_procs:
                print("Killing the process because there is arg.time")
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            for proc in cpu_procs:
                print("Waiting for the process because there is no arg.time")
                stdout, stderr = proc.communicate()
                print("stdout, stderr", stdout, stderr)
                proc.wait()
        
        mem_kill = subprocess.Popen(["killall", "stress-ng"])
        mem_kill.wait()
        print("[WfBench] Completed CPU and Memory Benchmarks!\n")

    if args.out and args.inputs and args.workdir:
        #outputs = json.loads(str(args.out).replace("'", '"'))
        print("Args out", args.out)
        outputs = json.loads(json.dumps(args.out))
        print(outputs, type(outputs))
        io_write_benchmark_user_input_data_size(outputs, args.workdir, memory_limit=mem_bytes)

    if core:
        unlock_core(path_locked, path_cores, core)

    print("WfBench Benchmark completed!")    
    return jsonify({'statusCode': 200})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))