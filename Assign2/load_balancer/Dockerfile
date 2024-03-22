FROM golang:latest

# # Install Docker
RUN apt-get update && apt-get install -y docker.io

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o load_balancer main.go

USER root

EXPOSE 5000

CMD ["./load_balancer"]




