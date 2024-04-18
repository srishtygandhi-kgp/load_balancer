FROM lb_base_image

WORKDIR /app

COPY . .


RUN go build -o load_balancer main.go lb.go types.go

USER root

EXPOSE 5000

CMD ["./load_balancer"]




