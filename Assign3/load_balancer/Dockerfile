FROM golang:latest


WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o load_balancer main.go lb.go types.go

USER root

EXPOSE 5000

CMD ["./load_balancer"]




