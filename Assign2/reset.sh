docker ps -a | grep 'server' | awk '{print $1}' | xargs docker rm --force
docker network rm assign2_net1