# Copyright 2024 Hewlett Packard Enterprise Development LP.

import os
import subprocess
import json
import yaml
import sys
from time import time
import time
import pathlib
import threading
import csv
import yaml
import networkx as nx
from collections import deque, defaultdict


def verify_expected_data(expected_output, workflow_data_locality, workflow_id):
    # To wait for the function completion. For now, I'm doing it checking if the expected outputs are written.
    print(">>> Checking if the required outputs are already there")
    output_ready = False
    cmd_result = os.listdir(workflow_data_locality + '/' + workflow_id)
    print("Listing expected outputs", expected_output, cmd_result, set(expected_output) <= set(cmd_result))
    if set(expected_output) <= set(cmd_result):
        output_ready = True
    print("All requirements ready? ", output_ready)
    return output_ready

def remove_function(workflow_name, function_name):
    print("Deleting ", workflow_name, function_name)
    cmd = "kubectl delete -f service.yaml" # --grace-period=0 --force"
    print(cmd)
    subprocess.run(cmd.split(" "), cwd="../services/wfbench/wfbench")
    os.system("sleep 20s")
    return

def deploy_function(workflow_name, function_name):
    print("Deploying ", workflow_name, function_name)
    cmd = "kubectl apply -f service.yaml"
    print(cmd)
    subprocess.run(cmd.split(" "), cwd="../services/wfbench/wfbench")
    os.system("sleep 10s")
    return

def list_inputs_and_outputs(next_function_files):
    next_function_data_requirements = []
    next_function_output = []
    for files in next_function_files:
        if (files["link"] == "input" and files["name"] not in next_function_data_requirements):
            next_function_data_requirements.append(files["name"])
        if (files["link"] == "output" and files["name"] not in next_function_data_requirements):
            next_function_output.append(files["name"])
    return next_function_data_requirements, next_function_output

def invoke_function(functions, next_function_name, invoked_functions, functions_data_locality, workflow_data_locality, workflow_id, platform):

    # Retrieve the values for the next functions
    next_function = functions[next_function_name]
    next_parameters = next_function["command"]
    cmd_invokation = ""

    """
    Reproduce this command:
    # curl localhost:8080/wfbench -X POST -H "Content-Type: application/json" -d 
    #'{"name":"split_fasta_00000001", "cpu_threads":0, "percent_cpu":0.6, "cpu_work":100, 
    # "out":{"split_fasta_00000001_output.txt": 204082}, "inputs":["split_fasta_00000001_output.txt"]}'
    """

    # Prepare the parameters of the next_function and construct its command line
    if next_parameters is not None:
        parameter_list = ""
        for parameter, value in next_parameters["arguments"][0].items():
            if value is None:
                parameter_list = None
            else:
                if (isinstance(value, str)):
                    parameter_list += '"' + parameter + '":"' + str(value) + '", '
                elif (isinstance(value, dict) or isinstance(value, list)): 
                    parameter_list += '"' + parameter + '":' + str(json.dumps(value)) + ', '
                else:
                    parameter_list += '"' + parameter + '":' + str(value) + ', '
        parameter_list += '"workdir":"' + str(pathlib.Path(functions_data_locality) / workflow_id) + '"'
        print(" >>>> Prepared the parameters", parameter_list, parameter_list == None)

        if (platform == 'knative'):
            workflow_id = workflow_id
            function_api = next_parameters["api_url"]
            cmd_invokation = "curl " + function_api + " -X POST -H 'Content-Type: application/json' -d '{" + parameter_list + "}'"#& " 
        
        if (platform == 'local'):
            #curl http://localhost:80/wfbench -X POST -H 'Content-Type: application/json' -d '{"name":"split_fasta_00000001", "percent-cpu":0.9, "cpu-work":100, "out":{"split_fasta_00000001_output.txt": 10010}, "inputs":["split_fasta_00000001_input.txt"], "workdir":"../data/BlastRecipe-250-100"}'
            function_api = "http://localhost:80/wfbench"
            cmd_invokation = "curl " + function_api + " -X POST -H 'Content-Type: application/json' -d '{" + parameter_list + "}'"#& " 

        print("  >>> Command: ", cmd_invokation)

        invoked_functions.append(next_function_name)
    
    return cmd_invokation, invoked_functions

def execute_functions(cmds_invokation):
    if (len(cmds_invokation) != 0):
        print(" >>> Final Command: ", cmds_invokation)
        command_threads = []
        for cmd_command in cmds_invokation:
            command_thread = threading.Thread(target=run_thread, args=(cmd_command, ))
            command_threads.append(command_thread)
            command_thread.start()

        for command_thread in command_threads:
            command_thread.join()
    return

"""
Create a DAG from the functions json file, and return a list of topological sorted functions.
In the case of Cyclic graphs, we break it by removing one functions from the cycle and attach it outside.
"""
def create_dag_from_yaml(functions):
    # Create a directed graph
    G = nx.DiGraph()

    # Add nodes and edges from the JSON data
    for node, attributes in functions.items():
        G.add_node(node)
        for child in attributes['children']:
            G.add_edge(node, child)

    # Break cycles in the graph
    removed_edges = break_cycles(G)

    # Calculate levels of nodes
    levels = calculate_levels(G)

    # Group nodes by level
    sorted_nodes_by_level = group_nodes_by_level(levels)

    # Ensure execution of nodes involved in the removed edges
    for edge in removed_edges:
        if edge[1] not in [node for sublist in sorted_nodes_by_level for node in sublist]:
            sorted_nodes_by_level.append([edge[1]])

    print("Nodes sorted by level:", sorted_nodes_by_level)

    return G, sorted_nodes_by_level

def break_cycles(G):
    removed_edges = []
    while True:
        try:
            # Perform topological sort to check for cycles
            list(nx.topological_sort(G))
            break
        except nx.NetworkXUnfeasible:
            # If there's a cycle, find it and remove one edge
            cycle = nx.find_cycle(G, orientation='ignore')
            edge_to_remove = cycle[0][:2]  # Get only the start and end nodes
            G.remove_edge(*edge_to_remove)
            removed_edges.append(edge_to_remove)
    return removed_edges

def calculate_levels(G):
    # Initialize levels
    levels = {node: 0 for node in G.nodes()}
    
    # Queue for BFS
    queue = deque(node for node in G.nodes() if G.in_degree(node) == 0)
    
    while queue:
        node = queue.popleft()
        current_level = levels[node]
        for neighbor in G.successors(node):
            levels[neighbor] = max(levels[neighbor], current_level + 1)
            queue.append(neighbor)
    
    return levels

def group_nodes_by_level(levels):
    # Group nodes by level
    level_dict = defaultdict(list)
    for node, level in levels.items():
        level_dict[level].append(node)
    
    # Create a sorted list of lists
    sorted_levels = []
    for level in sorted(level_dict):
        sorted_levels.append(sorted(level_dict[level]))
    
    return sorted_levels

"""
Verify parents executin recursively
"""
def verify_parent_function_execution(functions, current_function, invoked_functions, functions_data_locality, workflow_id):
    
    if (len(current_function.get("parents")) == 0):
        if current_function["name"] not in invoked_functions:
            cmds_invokation, invoked_functions = invoke_function(functions, current_function["name"], invoked_functions, functions_data_locality, workflow_id)
            execute_functions([cmds_invokation])
            return invoked_functions
        else:
            return invoked_functions
            
    else:
        if (len(current_function.get("parents")) == 1):
            function_parent = current_function.get("parents")[0]
            if function_parent not in invoked_functions:
                invoked_functions = verify_parent_function_execution(functions, functions.get(function_parent), invoked_functions, functions_data_locality, workflow_id)
                return invoked_functions
            else:
                cmds_invokation, invoked_functions = invoke_function(functions, current_function["name"], invoked_functions, functions_data_locality, workflow_id)
                execute_functions([cmds_invokation])
                return invoked_functions
                         
        #elif (len(current_function.get("parents")) > 1):
        else:
            for function_parent in current_function.get("parents"):
                if function_parent not in invoked_functions:
                    invoked_functions = verify_parent_function_execution(functions, functions.get(function_parent), invoked_functions, functions_data_locality, workflow_id)
            cmds_invokation, invoked_functions = invoke_function(functions, current_function["name"], invoked_functions, functions_data_locality, workflow_id)
            execute_functions([cmds_invokation])
            return invoked_functions

def run_exp_dag(exp_description, workflow_id, number_of_cores, platform):
    # Initialize variables
    start_time_experiment = int(time.time() * 1000)
    function_timeout_limit = 600000 #10 minutes
    workflow_function_timeout = False
    elapsed_time = {}
    executed_functions = []
    invoked_functions = []

    # Read the experiment description
    with open(exp_description) as file:
        documents = json.load(file)

    # Read the parameters from the description
    workflow = documents['workflow']
    workflow_name = workflow['name']
    workflow_data_locality = workflow['workflow_manager_data_locality']
    functions_data_locality = workflow['workflow_data_locality']

    if (platform == 'knative'):
        deploy_function(workflow_name, workflow_name)

    """
    TODO To fix for permission (sudo and not sudo)
    # Initilize the logs
    os.makedirs(workflow_data_locality + "/" + workflow_id, exist_ok=True)
    with open(str(workflow_data_locality) + "/" + str(workflow_id) + "/log_" + str(workflow_id) + '.log', 'a') as file1:
        file1.write("Run Workflow - " + str(workflow_id) + "\n")
        file1.write("++profile/Start timestamp: " + str(start_time_experiment) + " milliseconds\n")
    """

    # Retrieve functions for invokation phase and execute each one of them
    functions = workflow['tasks']

    # Create the DAG from the functions read above
    dag, functions_as_a_dag = create_dag_from_yaml(functions)

    # Verify which function was already executed in the platform
    # Here I verify the output instead of the data_requirements because I want to check which function was executed(finished)
    
    #""" REMOVE THIS COMMENT AFTER TESTING LOCAL
    print("!! Pre-verification phase\n")
    for functions_by_level in functions_as_a_dag:
        for function_name in functions_by_level:
            if ("-benchmark-start" in function_name or "-benchmark-finish" in function_name):
                continue
            print(function_name)
            function = functions[function_name]
            function_files = function["files"]
            function_output = []
            function_input, function_output = list_inputs_and_outputs(function_files)
            print(function_output)
            expected_requirements_ready = verify_expected_data(function_output, workflow_data_locality, workflow_id)
            if expected_requirements_ready == True:
                invoked_functions.append(function_name)
        
    print(" !! Already invoked functions: ", invoked_functions)
    print(" !! Let's trigger the next functions\n")
    print("functions.keys(): ", functions.keys())
    #"""

    # Looping though all functions to execute the workflow
    for functions_by_level in functions_as_a_dag:
        cmds_invokation = []
        for function_name in functions_by_level:
            
            print("Function_name", function_name)
            function = functions[function_name]
            function_original_name = function_name
            function_original_name = function_name#function['spec']['original_name']
            next_functions = function['children']
            function_files = function["files"]
            print("Next functions:", next_functions)

            for next_function_name in next_functions:
                next_function = functions[next_function_name]
                print("Next function name: ", next_function_name)
                print("Next functions:", next_functions)

                #""" REMOVE THIS COMMENT AFTER TESTING LOCAL
                if len(next_functions) == 1 and next_functions[0].split('-')[-1] == 'finish':
                    next_function_files = next_function["files"]
                    next_function_data_requirements, next_function_output = list_inputs_and_outputs(next_function_files)
                    print("Next function input:", next_function_data_requirements)

                    verify_last_end_of_workflow =  verify_expected_data(next_function_data_requirements, workflow_data_locality, workflow_id)
                    while verify_last_end_of_workflow == False:
                        os.system("sleep 1s")
                    print("Workflow completed")
                    break
                #"""

                # Verify if the funcions was executed by checking the name of the function
                if (next_function_name in invoked_functions):
                    print("   >>> Function already executed ", next_function_name)
                    continue
                
                next_function_files = next_function["files"]
                next_function_data_requirements, next_function_outputs = list_inputs_and_outputs(next_function_files)
                print("next_function_input", next_function_data_requirements)
                print("next_function_output", next_function_outputs)

                #""" REMOVE THIS COMMENT AFTER TESTING LOCAL
                # Check if the previous functions completed, which means, to check if the data requirements for the current function is ready
                # Does that respecting a timeout
                expected_requirements_ready = False
                start_timeout_countdown = int(time.time() * 1000)
                while expected_requirements_ready == False:
                    expected_requirements_ready = verify_expected_data(next_function_data_requirements, workflow_data_locality, workflow_id)
                    if (expected_requirements_ready == False): #and function_name in invoked_functions):
                        os.system("sleep 1s")
                        #invoked_functions = verify_parent_function_execution(functions, functions[next_function_name], invoked_functions, functions_data_locality, workflow_id)
                #"""    
                """
                    current_timeout_countdown = int(time.time() * 1000)
                    if (current_timeout_countdown - start_timeout_countdown >= function_timeout_limit):
                        workflow_function_timeout = True
                        print("Timeout between functions' execution. ")
                        break
                """
                """
                TODO To fix for permission (sudo or not sudo)
                # If the timeout happened, register it in the logs
                if workflow_function_timeout:
                    with open(str(workflow_data_locality) + "/" + str(workflow_id) + "/log_" + str(workflow_id) + '.log', 'a') as file1:
                        file1.write("++profile/Step (timeout reached) at: " + str(current_timeout_countdown) + " \n")
                        file1.write("++profile/Step (timeout reached): " + str(current_timeout_countdown - start_timeout_countdown) + " milliseconds\n")
                    break
                """
                cmd_invokation, invoked_functions = invoke_function(functions, next_function_name, invoked_functions, functions_data_locality, workflow_data_locality, workflow_id, platform)
                cmds_invokation.append(cmd_invokation)
            # If the next functions still need to be executed, let's prepare the command line for them
            print(" >>> Invoked functions ", invoked_functions)
            print(" >>> CMDS invokation ", cmds_invokation)

            # TODO To update it to process per call, then to ensure that all functions finished correctly
            if (len(cmds_invokation) != 0):
                print("cmds_invokation", cmds_invokation)
                for cmd in cmds_invokation:
                    print("\n\CMD: ", cmd)
                execute_functions(cmds_invokation)
            cmds_invokation = []
                
    """
    TODO To fix for permission (sudo or not sudo)
    end_time_experiment=int(time.time() * 1000)
    elapsed_time=end_time_experiment - start_time_experiment
    with open(str(workflow_data_locality) + "/" + str(workflow_id) + "/log_" + str(workflow_id) + '.log', 'a') as file1:
        file1.write("++profile/Elapsed time: " + str(elapsed_time) + " milliseconds\n")
        file1.write("++profile/End timestamp: " + str(end_time_experiment) + " milliseconds\n")
    """
    if (platform == 'knative'):
        remove_function(workflow_name, function_original_name)

    return     

def print_parameters():
    str = "\nPlease, use one of the following parameters: \n\
    -h                 | help \n\
    -r <exp_file.yaml> <workflow_id> <number_of_cores> <platform> | run the experiment described in exp_file.yaml"
   
    print(str)
    return

def run_thread(cmd):
    print("Run: \n", cmd)
    subprocess.getoutput(cmd)

def thread_function(cmd):
    print("\n         >>>> Run: \n", cmd)
    subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

def main():

    argvs = sys.argv
    if (len(argvs) == 1):
        print_parameters()

    else:
        if(argvs[1] == "-h"):
           print_parameters()
        elif(argvs[1] == "-r"):
            if (len(argvs) < 5):
                print_parameters()
            else:
                print(" >>> Starting the thread to measure the resources")
                measurements_execution_time_file_name = "measurement_execution_time.csv"
                measurements_file_name = str(argvs[3])
                if (argvs[5] == 'knative'):
                    command_measurement = 'ssh -l <user_name> <machine_address> "pmdumptext -d \',\' -f \'%d/%m/%y %H:%M:%S\' -t 1sec kernel.all.cpu.user mem.util.used denki.rapl.rate[\"0-package-0\"] denki.rapl.rate[\"1-package-1\"] > wfbench/' + measurements_file_name + '.csv\"' 
                else:
                    command_measurement = "pmdumptext -d \',\' -f \'%d/%m/%y %H:%M:%S\' -t 1sec kernel.all.cpu.user mem.util.used denki.rapl.rate[\"0-package-0\"] denki.rapl.rate[\"1-package-1\"] > ../../wfbench/" + measurements_file_name + '.csv'
                print("Command for the thread\n", command_measurement)
                command_thread = threading.Thread(target=thread_function, args=(command_measurement, ))
                command_thread.start()
                os.system("sleep 2s")

                print("Creating and invoking the functions for exp: " + str(argvs[2]))
                entire_service_start_time=int(time.time() * 1000)
                
                run_exp_dag(argvs[2], argvs[3], argvs[4], argvs[5])
                
                entire_service_end_time=int(time.time() * 1000)
                task_time = entire_service_end_time - entire_service_start_time

                """ 
                TODO To solve problems of permission (sudo or not sudo)
                rows = zip([entire_service_start_time], [entire_service_end_time], [task_time])
                with open('./' + measurements_execution_time_file_name, "w") as f:
                    writer = csv.writer(f)
                    for row in rows:
                        writer.writerow(row)
                """

                print(" >>> Finilizing the thread to measure the resources")
                command_thread.join()

                if (argvs[5] == 'knative'):
                    copy_measurement = 'ssh <user_name> <machine_address> "cp -r wfbench/' + measurements_file_name + '.csv wfbench/' + measurements_file_name + '_official.csv\"' #  wfbench/measurements.txt\"'
                else:
                    copy_measurement = 'cp -r ../../wfbench/' + measurements_file_name + '.csv ../../wfbench/' + measurements_file_name + '_official.csv' #  wfbench/measurements.txt\"'
                command_thread = threading.Thread(target=thread_function, args=(copy_measurement, ))
                command_thread.start()
                command_thread.join()

                print(" >>> All threads finished")
    return

main()