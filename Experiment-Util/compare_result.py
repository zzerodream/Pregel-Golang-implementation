# Import the json module
import json

# Open the JSON file in read mode
with open("solution.json", "r") as f:
    # Read the JSON data and convert it to a dictionary
    data = json.load(f)

# Print the dictionary
print(data)
computed = dict()
for i in range (10):
    with open(f"file{i+1}", "r") as f:
        # Read the JSON data and convert it to a dictionary
        part = json.load(f)
        computed = computed | part

print(computed)
wrong_count = 0
for key in data.keys():
    if key not in computed.keys() or int(computed[key])!=int(data[key]):
        wrong_count += 1
print("correct rate: ",(len(data)-wrong_count)*1.0/len(data))