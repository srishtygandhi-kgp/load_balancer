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

    with concurrent.futures.ThreadPoolExecutor() as executor:
        urls = [base_url for _ in range(num_requests)]
        responses = list(executor.map(send_request, urls))

    with open("server_responses.txt", "w") as file:
        for response in responses:
            file.write(response + "\n")

if __name__ == "__main__":
    main()
