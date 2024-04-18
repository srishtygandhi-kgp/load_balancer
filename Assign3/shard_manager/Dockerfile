FROM shm_base_image

WORKDIR /app

COPY . .

RUN go build -o shard_manager main.go types.go

USER root

EXPOSE 5000

CMD ["./shard_manager"]
