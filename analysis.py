import requests
import concurrent.futures

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

if __name__ == "__main__":
    main()
