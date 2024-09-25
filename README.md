
# Enabling HPC Scientific Workflows for Serverless 
Repository for the paper: Enabling HPC Scientific Workflows for Serverless 

Here is the description of each folder in this repository. 

## Experiments

### experiments/services/

<wfbench>: packs wfbench as a service to be deployed on Knative. It should be deployed as: "kubectl apply -f service.yaml". Once deployed, the service responde to any HTTP request as the following: ""
<wfbench-local>: prepares wfbench as a local container. It should be deployed by: "". Once deployed, the service responde to any HTTP request as the following: ""

### experiments/src/

<serverless-workflows-wfbench.py>: It is our prototype of WFM for serverless. 
<run_all_wfbench.sh> : A bash script that calls each experiment on our serverless setup.
<run_all_wfbench_local.sh> : A bash script that calls each experiment on our local container setup.

### experiments/wfbench/

<knative.py>: It is our contribution to WfCommons, the Knative Translator component. To be working we should:
- Clone WfCommons repository,
- Place knative.py at: "wfcommons/wfbench/translator/"
- Then we should run "pip install . --break-system-packages" from the WfCommon root directory.
- Then we can run the generate_workflow.py file that is on our repository at "/workflows".

### experiments/workflows/

<generate_workflows_example.py>: This script examplifies how to import our Knative Translator module and use it to generate the workflows we need.
<generate_workflows_example.py>: This script imports our Knative Translator module and use it to generate all workflows we need.

### experiments/results/

</workflow-executions/>: This folder contains the main results of our experiments: the measurements of cpu, memory, power and execution time per experiment.
They are grouped by paradigm, listed also on the paper. They are:
- knative-level            
- knative-scaling-1w-novm   
- local-container-96w
- knative-scaling-10w-novm  
- knative-sequential        
- local-container-96w-novm
- knative-scaling-1w        
- local-container-960w-novm  
- local-level

</workflows_descriptions/>: This folder contains, by group, the summary for the workflows we used. The groups are:
- "/functions_invocation" : the number of invocations per phase;
- "/functions_invocation_name" : the number of invocations per function_name.

## Analysis

### analysis/graphs_visualization/

<generate_visualization.py>: This script generates the visualization for the workflows generated on the /experiments/workflows. Here are the ones we filtered for the paper at "/pdf" and "/png".

### analysis/jupyter_notebook/

Here is the complete jupyter-notebook folder, which includes the code and the outputs of the analysis for the performance, resource usage, and workflow_description (data generated on the folder described above).