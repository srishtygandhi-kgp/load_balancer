docker ps -a | grep -m 1 './server' | awk '{print $1}' | xargs docker rm --force
