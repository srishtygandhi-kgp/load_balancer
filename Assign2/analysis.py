import os
import requests


performance = {"Write": {}, "Read": {}}
numOfRW = 10000


def performRW(numOfShards, numOfServers, numOfReplicas):
    global performance, numOfRW
    # start the system in the background
    os.system("docker compose up -d")
    # init the system
    shards = []
    for i in range(numOfShards):
        shards.append({"Stud_id_low": i*4096, "Shard_id": f"sh{i}", "Shard_size": 4096})
    servers = {}
    for i in range(numOfServers):
        servers[f"Server{i}"] = []
    j = 0
    for i in range(numOfShards):
        for k in range(numOfReplicas):
            servers[f"Server{j}"].append(f"sh{i}")
            j = (j+1) % numOfServers
    payload = {
        "N":3, 
        "schema": {
            "columns":["Stud_id","Stud_name","Stud_marks"], 
            "dtypes":["Number","String","String"]
            }, 
        "shards":[{"Stud_id_low":0, "Shard_id": "sh1", "Shard_size":4096}, {"Stud_id_low":4096, "Shard_id": "sh2", "Shard_size":4096}, {"Stud_id_low":8192, "Shard_id": "sh3", "Shard_size":4096}], 
        "servers":{"Server0":["sh1","sh2"], "Server1":["sh2","sh3"], "Server2":["sh1","sh3"]}
    }
    # print(servers)
    # print(payload)

    # Send payload to the load balancer
    response = requests.post("http://localhost:5000/init", json=payload)
    if response.status_code != 200:
        print("Error in init")
        print(response.text)
        exit(0)
    # os.system("docker ps")
    # perform writes
    readTime = 0
    writeTime = 0
    for i in range(numOfRW):
        payload = {"Stud_id":i, "Stud_name":f"Student{i}", "Stud_marks": i%100}
        response = requests.post("http://localhost:5000/write", json=payload)
        writeTime += response.elapsed.microseconds
        if response.status_code != 200:
            print("Error in writing")
    # perform reads
    for i in range(numOfRW):
        payload = {"Stud_id": {"low":i, "high":i+1}}
        response = requests.post(f"http://localhost:5000/read", json=payload)
        readTime += response.elapsed.microseconds
        if response.status_code != 200:
            print("Error in reading")
            print(response.text)
    readTime /= 1000000
    writeTime /= 1000000
    performance["Write"][f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"] = writeTime
    performance["Read"][f"{numOfShards} Shards, {numOfServers} Servers, {numOfReplicas} Replicas"] = readTime

    # stop the system
    # curl -X DELETE -H "Content-Type: application/json" -d '{"n" : 2, "servers" : ["Server4"]}' http://localhost:5000/rm
    payload = {"n":numOfServers, "servers":[]}
    response = requests.delete("http://localhost:5000/rm", json=payload)
    os.system("docker compose down")



performRW(4, 6, 3)
performRW(4, 6, 6)
performRW(6, 10, 8)


def printGraph():
    global performance
    print(performance)
    # print the graph
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.bar(performance["Write"].keys(), performance["Write"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Write Performance')
    # plt.xticks(rotation=90)
    plt.show()
    fig, ax = plt.subplots()
    ax.bar(performance["Read"].keys(), performance["Read"].values())
    ax.set_ylabel('Time (in seconds)')
    ax.set_xlabel('Configurations')
    ax.set_title('Read Performance')
    # plt.xticks(rotation=90)
    plt.show()


printGraph()