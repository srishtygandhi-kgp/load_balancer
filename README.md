# load_balancer
CS60002: Distributed Systems 

## Assignment 1: Implementing a Customizable Load Balancer with consistent hashing
Contributors for this project are:
- [Yatindra Indoria](https://github.com/yatindra7) - 20CS30060
- [Srishty Gandhi](https://github.com/srishtygandhi-kgp) - 20CS30052
- [Atulya Sharma](https://github.com/r-avenous) - 20CS10012
- [Krishna Venkat Cherukuri](https://github.com/kv2002) - 20CS10019
  
---

## Usage
### Running the load balancer

- To build the Docker containers:
```bash
docker-compose build
```
- To start the Docker containers:
```bash
docker-compose up
```
- Adding new servers to the container:
```bash
curl -X POST -H "Content-Type: application/json" -d '{"n": 3, "hostnames": ["S1", "S2", "S3"]}' http://localhost:5000/add
```
- To get the status of replicas
  ```bash
  curl http://localhost:5000/rep
  ```
- To remove the servers:
  ```bash
  curl -X DELETE -H "Content-Type: application/json" -d '{"n": 2, "hostnames": ["S1", "S2"]}' http://localhost:5000/rm
  ```
- To route to a server
  ```bash
  curl http://localhost:5000/home
  ```

- Cleanup:
```bash
docker ps -a | grep './server' | awk '{print $1}' | xargs docker rm --force
docker-compose down
```


