import random
import json

numnodes = [100, 200, 300, 500, 700, 1000] 

def keep(success_rate=0.7):
    random_value = random.random()
    if random_value <= success_rate:
        return True  # Success
    else:
        return False  # Failure

def generate(size):
    nodes = {}
    edges = {}
    for i in range(1, size):
        for j in range(i + 1, size + 1):
            if keep():
                pair = (i, j)
                edges[pair] = random.randint(1,15)

    for i in range(1, size+1):
        nodes[str(i)] = {}
        nodes[str(i)]["ID"] = i
        nodes[str(i)]["edges"] = {}
        for j in range(1, size+1):
            if j != i:
                pair = (i, j) if i < j else (j, i)
                if pair in edges:
                    nodes[str(i)]["edges"][str(j)] = edges[pair]
    json_file_path = f"Test/SampleNodes{size}.json"

    # Convert dictionary to JSON and write to file
    with open(json_file_path, 'w') as json_file:
        json.dump(nodes, json_file)
    # print(nodes)
for i in numnodes:
    # print(i)
    generate(i)