import json
import time

# Read the JSON file and convert it into a dictionary
numnodes = [100, 200, 300, 500, 700, 1000] 

def bellman_ford(graph, source):
    # Initialize distance to all vertices as infinite and distance to source as 0
    distance = {vertex: float('infinity') for vertex in graph}
    distance[source] = 0

    # Relax all edges |V| - 1 times
    for _ in range(len(graph) - 1):
        for node in graph:
            for neighbor in graph[node]['edges']:
                if distance[node] + graph[node]['edges'][neighbor] < distance[neighbor]:
                    distance[neighbor] = distance[node] + graph[node]['edges'][neighbor]

    # Check for negative-weight cycles
    for node in graph:
        for neighbor in graph[node]['edges']:
            if distance[node] + graph[node]['edges'][neighbor] < distance[neighbor]:
                print("Graph contains a negative-weight cycle")
                return None

    return distance
source_vertex = '1'  # Assuming you want to start from vertex with ID 1
start_time = time.time()
for i in numnodes:
    with open(f'Test\SampleNodes{i}.json', 'r') as file:
        graph = json.load(file)
    start_time = time.time()
    shortest_paths = bellman_ford(graph, source_vertex)
    end_time = time.time()
    # if shortest_paths:
        # print("Shortest distances from vertex", source_vertex)
        # for vertex in shortest_paths:
        #     print(f"Distance to vertex {vertex}: {shortest_paths[vertex]}")

    print(f"Number of nodes: {i}. Execution time: {end_time - start_time} seconds")