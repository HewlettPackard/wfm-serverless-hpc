# Copyright 2024 Hewlett Packard Enterprise Development LP.

import pathlib
import json
from logging import Logger
from typing import Dict, Optional, Union

from .abstract_translator import Translator
from ...common import FileLink, Workflow
import yaml

class KnativeTranslator(Translator):
    """
    A WfFormat parser for creating Knative workflow applications.

    :param workflow: Workflow benchmark object or path to the workflow benchmark JSON instance.
    :type workflow: Union[Workflow, pathlib.Path],
    :param logger: The logger where to log information/warning or errors (optional).
    :type logger: Logger
    """


    def __init__(self,
                 workflow: Union[Workflow, pathlib.Path],
                 logger: Optional[Logger] = None) -> None:
        """Create an object of the translator."""
        super().__init__(workflow, logger)

        #self.root_task_names = []
        #self.task_parents = {}
        #self.task_children = {}
        self.script = "Knative here"
        self.parsed_tasks = []
        self.tasks_map = {}
        self.task_counter = 1

    def translate(
        self, 
        output_file_name: pathlib.Path,
        service_name,
        service_namespace,
        container_tag, 
        container_url, 
        volume_mount_name, 
        volume_mount_path, 
        cpu_request,
        memory_request,
        cpu_limit, 
        memory_limit, 
        volume_name, 
        pvc_name,
        workflow_data_locality,
        workflow_manager_data_locality,
        knative_function_url) -> None:
        
        self.script = "apiVersion: serving.knative.dev/v1\n" \
            "kind: Service\n" \
            "metadata:\n" \
            "  name: " + str(service_name) + "\n" \
            "  namespace: " + str(service_namespace) + "\n" \
            "spec:\n" \
            "  template:\n" \
            "    spec:\n" \
            "      containers:\n" \
            "      - name: " + str(container_tag) + "\n" \
            "        image: " + str(container_url) + "\n" \
            "        volumeMounts:\n" \
            "        - name: " + str(volume_mount_name) + "\n" \
            "          mountPath: " + str(volume_mount_path) + "\n" \
            "        resources:\n" \
            "          requests:\n" \
            "            cpu: " + str(cpu_request) + "\n" \
            "            memory: " + str(memory_request) + "\n" \
            "          limits:\n" \
            "            cpu: " + str(cpu_limit) + "\n" \
            "            memory: " + str(memory_limit) + "\n" \
            "      volumes:\n" \
            "      - name: " + str(volume_name) + "\n" \
            "        persistentVolumeClaim:\n" \
            "          claimName: " + str(pvc_name) + "\n" \
            "          readOnly: false\n" \
            "      serviceAccountName: knative-rbac\n"

        with open('service.yaml', 'w') as file:
            yaml.dump(yaml.safe_load(self.script), file)

        workflow = json.dumps(self.workflow.workflow_json)

        # Workflow level
        self.workflow.workflow_json["workflow"]["workflow_data_locality"] = workflow_data_locality
        self.workflow.workflow_json["workflow"]["workflow_manager_data_locality"] = workflow_manager_data_locality
        self.workflow.workflow_json["workflow"]["name"] = self.workflow.workflow_json["name"].lower() + "-knative"
        dict_tasks = {}
        tasks_without_parent = []
        for task in self.workflow.workflow_json["workflow"]["tasks"]:
            task_name = task["name"]
            task["command"]["api_url"] = knative_function_url

            arguments = task["command"]["arguments"]

            # Preparing the new_arguments format
            new_arguments = {}

            # Name is the first argument, and it has no key
            new_arguments["name"] = arguments[0]

            # If there are more arguments than the name
            if (len(arguments) > 2):
                for argument in arguments[1:]:
                    # arguments are in format '--key value'. Also covering '--key {value1: value2, ...}'
                    # here we convert them to proper dictionaries: "{key: value, key: {values}}"
                    # splitting the space between '--key value'. The name does not have a --key.
                    if (len(argument.split(' ', 1)) > 1):
                        argument_key =  argument.split(' ', 1)[0].split("--")[1]
                        argument_value = ""
                        value_str = argument.split(' ', 1)[1]  # Split at the first space
                        try:
                            # If it has a '{', so it is a the output dictionary. 
                            # To get all the outputs we actully go to the list of "files" because it is easier than converting the current dictionary from a string.
                            if value_str.startswith('{'):
                                output_arguments = {}
                                for file in task["files"]:
                                    if file["link"] == "output":
                                        output_arguments[file["name"]] = file["sizeInBytes"]
                                argument_key = "out"
                                argument_value = output_arguments   
                        
                            elif '.' in value_str:
                                argument_value = float(value_str)
                        
                            else:
                                argument_value = int(value_str)
                        except ValueError:
                            argument_value = value_str  # If conversion fails, keep it as string

                    # The inputs are listed in the end, without key, and can be several. So we also go to the list of "files" because it is easier.
                    else:
                        input_arguments = []
                        for file in task["files"]:
                            if file["link"] == "input":
                                input_arguments.append(file["name"])
                        argument_key = "inputs"
                        argument_value = input_arguments
                    
                    new_arguments[argument_key] = argument_value

            task["command"]["arguments"] = [new_arguments]
            dict_tasks[task_name] = task

            # Check if the task has parent, if not, they should be invoqued by the starting-task
            if (len(task["parents"]) == 0):
                tasks_without_parent.append(task_name)

        # Add header and tail on the workflow

        # Get the first task and make a copy to be the skeleton of our new first task, and update it
        #current_initial_task = list(dict_tasks.keys())[0]

        starting_task = {}
        starting_task_name = self.workflow.workflow_json["name"].lower() + "-start"
        starting_task[starting_task_name] = dict_tasks[list(dict_tasks.keys())[0]].copy()
        starting_task[starting_task_name]["name"] = starting_task_name
        starting_task[starting_task_name]["command"] = {}
        starting_task[starting_task_name]["parents"] = []
        starting_task[starting_task_name]["children"] = tasks_without_parent
        starting_task[starting_task_name]["files"] = []
        starting_task[starting_task_name]["id"] = "00000000" 
        starting_task[starting_task_name]["category"] = "initializing-workflow"
        
        # Prepare the final task
        final_task_name = self.workflow.workflow_json["name"].lower() + "-finish"

        # Search over the self.task_children for those that does not have children
        children_task_without_children = []
        for children_task in self.task_children:
            if not self.task_children[children_task]:
                children_task_without_children.append(children_task)

        # Update the children tasks that does not have children to point to the new final task
        # And use the same loop to put all files in the same list
        list_of_files_of_all_tasks = []
        for task in self.workflow.workflow_json["workflow"]["tasks"]:
            if (task["name"] in children_task_without_children):
                task["children"] = [final_task_name]
            if (task["name"] in tasks_without_parent):
                task["parents"] = [starting_task_name]                
            
            for file in task["files"]:
                if (file["link"] == "input" and file not in list_of_files_of_all_tasks):
                    list_of_files_of_all_tasks.append(file)

        # Write the new final task
        final_task = {}
        final_task[final_task_name] = dict_tasks[children_task_without_children[0]].copy()
        final_task[final_task_name]["name"] = final_task_name
        final_task[final_task_name]["command"] = {}
        final_task[final_task_name]["parents"] = children_task_without_children
        final_task[final_task_name]["files"] = list_of_files_of_all_tasks
        final_task[final_task_name]["id"] = str(len(self.workflow.workflow_json["workflow"]["tasks"]) + 2)
        final_task[final_task_name]["category"] = "ending-workflow"
        
        # Create a new dictionary and extend it to
        # (i) make sure that our new first task is the official first task
        # (ii) add our new final task at the end
        dict_tasks_with_head_and_tail = starting_task
        dict_tasks_with_head_and_tail.update(dict_tasks)
        dict_tasks_with_head_and_tail.update(final_task)
      
        # Update the official tasks entry      
        self.workflow.workflow_json["workflow"]["tasks"] = dict_tasks_with_head_and_tail

        with open(output_file_name, "w") as outfile: 
            json.dump(self.workflow.workflow_json, outfile, indent=4) 