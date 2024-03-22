FROM mysql:8.0.36-debian

WORKDIR /docker-entrypoint-initdb.d/
COPY . .

RUN apt-get update && apt-get install -y ca-certificates openssl

RUN apt-get install -y curl

RUN apt-get install -y golang-go

ENV MYSQL_ROOT_PASSWORD="abc" \
    MYSQL_DATABASE="assign2"

RUN go mod tidy

RUN go build -o server main.go

EXPOSE 5000
