FROM golang:latest

WORKDIR /app

COPY . .

RUN go build -o server main.go

EXPOSE 5000

CMD ["./server"]
