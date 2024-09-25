# Copyright 2024 Hewlett Packard Enterprise Development LP.

import pathlib

from wfcommons import BlastRecipe
from wfcommons.wfbench import WorkflowBenchmark, KnativeTranslator

# create a workflow benchmark object to generate specifications based on a recipe
benchmark = WorkflowBenchmark(recipe=BlastRecipe, num_tasks=50)

# generate a specification based on performance characteristics
benchmark.create_benchmark(pathlib.Path("./"), cpu_work=100, data=10, percent_cpu=0.9)

# generate a Knative workflow
translator = KnativeTranslator(benchmark.workflow)

output_file_name = pathlib.Path("./benchmark-workflow-example-50.json")
service_name = "wfbench"
service_namespace = "knative-functions"
container_tag = "wfbench"
container_url = "docker.io/andersonandrei/wfbench-knative:wfbench"
volume_mount_name = "wfbench"
volume_mount_path = "/data"
cpu_request = 1
memory_request = "1024M"
cpu_limit = 1
memory_limit = "1024M"
volume_name = "wfbench"
pvc_name = "wfbench"
workflow_data_locality = "../data"
workflow_manager_data_locality = "<address_for_the_shared_drive>/wfbench/data"
knative_function_url = "http://wfbench.knative-functions.10.106.193.156.sslip.io/wfbench"

translator.translate(
        output_file_name,
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
        knative_function_url)