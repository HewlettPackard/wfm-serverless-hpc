# Copyright 2024 Hewlett Packard Enterprise Development LP.

import json
import networkx as nx
from collections import deque, defaultdict
import matplotlib.pyplot as plt
import os
import csv

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

def read_graph(generated_workflow):
    
    # Load the JSON file
    with open("./generated_workflows/" + str(generated_workflow) + "/" + str(generated_workflow) + ".json", "r") as file:
        documents = json.load(file)

    workflow = documents['workflow']
    # Retrieve functions for invokation phase and execute each one of them
    functions = workflow['tasks']

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

    return G, sorted_nodes_by_level, functions

def write_invocations_per_level(generated_workflow, sorted_nodes_by_level):
    
    print("write_invocations_per_level\n")
    # Register number of invocations per level
    #print("Nodes sorted by level:", sorted_nodes_by_level)
    enumerated_nodes_by_level = {}
    for index, nodes_by_level in enumerate(sorted_nodes_by_level):   
        enumerated_nodes_by_level[index] = len(nodes_by_level)
    #print("enumerated_nodes_by_level", enumerated_nodes_by_level)

    headers = ["workflow_level", "number_of_invocations"]
    with open("./functions_invocation/" + str(generated_workflow) + ".csv", "w") as outfile: 
        # Create a writer object
        writer = csv.writer(outfile)
        
        # Write the header
        writer.writerow(headers)
            # Write the data
        for key, value in enumerated_nodes_by_level.items():
            writer.writerow([key, value])

def write_functions_name(generated_workflow, sorted_nodes_by_level, functions):
    
    print("write_functions_name\n")
    # Register number of invocations per level
    #print("Nodes sorted by level:", sorted_nodes_by_level)
    
    functions_name = {}
    for function_name in functions:
        name = function_name.split("_")[0]
        if name not in functions_name.keys():
            functions_name[name] = 1
        else:
            functions_name[name] += 1
    #print("functions_name", functions_name)
    
    headers = ["function_name", "number_of_invocations"]
    with open("./functions_invocation_name/" + str(generated_workflow) + ".csv", "w") as outfile: 
        # Create a writer object
        writer = csv.writer(outfile)
        
        # Write the header
        writer.writerow(headers)
            # Write the data
        for key, value in functions_name.items():
            writer.writerow([key, value])

def generate_visualization(G, functions, generated_workflow):
    
    print("generate_visualization\n")
    # Specify nodes to label
    nodes_to_label = []
    for function_name in functions:
        if 'start' in function_name or 'finish' in function_name:
            nodes_to_label.append(function_name)
    #nodes_to_label = ['node1', 'node4']  # Adjust this list to specify which nodes to label
    #labels = {node: node for node in nodes_to_label}
    #labels = {node: f"{node}\n(multi-line)" for node in nodes_to_label}  # Customize the label with multi-line text
    labels = {node: f"{node}" for node in nodes_to_label}  # Customize the label with multi-line text
    # Set default node attributes
    default_node_size = 50
    default_node_color = 'skyblue'
    default_node_shape = 'o'

    # Set attributes for labeled nodes
    special_node_size = 100
    special_node_color = 'green'
    special_node_shape = 's'  # square shape

    # Draw default nodes
    default_nodes = [node for node in G.nodes() if node not in nodes_to_label]
    special_nodes = [node for node in nodes_to_label]

    # Visualization
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G)
    #nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=3000, edge_color='gray', linewidths=1, font_size=15)
    #nx.draw(G, pos, node_color='skyblue', node_size=50, edge_color='gray', linewidths=1)  # Adjusted node_size and removed with_labels
    #nx.draw(G, pos, labels=labels, node_color='skyblue', node_size=50, edge_color='gray', linewidths=1)
    nx.draw_networkx_nodes(G, pos, nodelist=default_nodes, node_size=default_node_size, node_color=default_node_color, node_shape=default_node_shape)
    nx.draw_networkx_nodes(G, pos, nodelist=special_nodes, node_size=special_node_size, node_color=special_node_color, node_shape=special_node_shape)
    nx.draw_networkx_edges(G, pos, edge_color='grey', width=0.5)
    #nx.draw_networkx_labels(G, pos, labels=labels, font_size=12, font_color='black')
    
    # Draw labels with bold and multi-line text
    for node, (x, y) in pos.items():
        if node in labels:
            plt.text(x, y, labels[node], fontsize=12, fontweight='bold', ha='center', va='center')

    plt.title('DAG Visualization')
    #plt.savefig("./graphs_visualization/" + str(generated_workflow) +'.png')  # Save the figure as a PNG file
    plt.savefig("./graphs_visualization/png/" + str(generated_workflow) + '.png', format='png', dpi=300)  # High-quality PNG
    plt.savefig("./graphs_visualization/pdf/" + str(generated_workflow) + '.pdf', format='pdf')  # PDF format
    #plt.savefig("./graphs_visualization/svg/" + str(generated_workflow) + '.svg', format='svg')  # SVG format
    #plt.show()    

def main():

    generated_workflows = os.listdir("./generated_workflows")
    #print(generated_workflows)

    for generated_workflow in generated_workflows:
        print(generated_workflow)
        if '-1000' in generated_workflow or '-10000' in generated_workflow:
            continue

        G, sorted_nodes_by_level, functions = read_graph(generated_workflow)

        #write_invocations_per_level(generated_workflow, sorted_nodes_by_level)
        
        #write_functions_name(generated_workflow, sorted_nodes_by_level, functions)

        generate_visualization(G, functions, generated_workflow)

main()