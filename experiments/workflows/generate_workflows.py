# Copyright 2024 Hewlett Packard Enterprise Development LP.

import pathlib

from wfcommons import BlastRecipe, BwaRecipe, CyclesRecipe, EpigenomicsRecipe
from wfcommons import GenomeRecipe, MontageRecipe, SeismologyRecipe, SoykbRecipe, SrasearchRecipe
from wfcommons.wfbench import WorkflowBenchmark, KnativeTranslator

if __name__:
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

    #generator = WorkflowGenerator(EpigenomicsRecipe.from_num_tasks(1000))
    recipes = {
        #"BlastRecipe": BlastRecipe}#, 
        #"BwaRecipe": BwaRecipe}#, 
        #"CyclesRecipe": CyclesRecipe}#, 
        #"EpigenomicsRecipe": EpigenomicsRecipe}#, 
        "GenomeRecipe": GenomeRecipe}#, 
        #"MontageRecipe": MontageRecipe}#, 
        #"SeismologyRecipe": SeismologyRecipe}#, 
        #"SoykbRecipe": SoykbRecipe}#, 
        #"SrasearchRecipe": SrasearchRecipe}
        #}
    for recipe_name, recipe_class in recipes.items():
        for size in [106]: #[500, 1000, 1500, 2000]:
            benchmark = WorkflowBenchmark(recipe=recipe_class, num_tasks=size)
            benchmark.create_benchmark(pathlib.Path('./'), cpu_work=100, data=10, percent_cpu=0.9)
            
            #workflow.write_json(pathlib.Path('./workflows_generated/' + f'epigenomics-workflow-{i}.json'))
            output_file_name = pathlib.Path('./workflows_generated/' + recipe_name + f'-workflow-{size}.json')
            translator = KnativeTranslator(benchmark.workflow)
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