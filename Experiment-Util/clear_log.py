for i in range (5):
    with open(f"master{i+1}.txt","w") as f:
        f.write("")
for i in range(10):
    with open(f"worker{i+1}.txt","w") as f:
        f.write("")