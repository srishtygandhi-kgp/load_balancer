FROM server_base_image

WORKDIR /docker-entrypoint-initdb.d/
COPY . .

RUN go build -o server main.go types.go

USER root

EXPOSE 5000