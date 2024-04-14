build:
	docker compose build
	docker build -t server_image ./server

run:
	docker compose up

stop:
	docker compose down
	docker ps -a | grep 'server' | awk '{print $1}' | xargs docker rm --force
	docker network rm assign2_net1
