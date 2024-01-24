import requests
import concurrent.futures
import os
import matplotlib.pyplot as plt
import json
import sys


def cmd(n):
    hostnames = ["S" + str(i+1) for i in range(n)]
    curl_command = 'curl -X POST -H "Content-Type: application/json" -d \'{{"n": {}, "hostnames": {}}}\' http://localhost:5000/add'.format(n, json.dumps(hostnames))
    return curl_command


def send_request(url):
    try:
        response = requests.get(url)
        return response.json()["message"]
    except Exception as e:
        return str(e)


def main():
    base_url = "http://localhost:5000/home"
    num_requests = 10000
    data = {}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        urls = [base_url for _ in range(num_requests)]
        responses = list(executor.map(send_request, urls))

    # with open("server_responses.txt", "w") as file:
    for response in responses:
        server = response.split()[-1]
        if server in data:
            data[server] += 1
        else:
            data[server] = 1
            # file.write(response + "\n")
    
    # Extract keys and values from the dictionary
    labels = list(data.keys())
    values = list(data.values())

    # Create a bar plot
    plt.bar(labels, values, color='blue', alpha=0.7)

    # Add labels and title
    plt.xlabel('Servers')
    plt.ylabel('Frequency')
    plt.title('Requests per Server')

    # Show the plot
    plt.show()


if __name__ == "__main__":
    os.system(cmd(int(sys.argv[1])))
    main()
