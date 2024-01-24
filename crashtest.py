import requests
import concurrent.futures
import os
import json
import sys

# Analysis for the crash, and respawn

def validInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


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
    data = {'Hit': 0, 'Miss' : 0}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        urls = [base_url for _ in range(num_requests)]
        responses = list(executor.map(send_request, urls))

    for response in responses:
        server = response.split()[-1]
        if validInt(server):
            data['Hit'] += 1
        else:
            data['Miss'] += 1
    print(data['Hit'] / num_requests)

if __name__ == "__main__":
    os.system(cmd(int(sys.argv[1])))
    main()
