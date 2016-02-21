#!/usr/bin/env bash


docker run --name some-mysql -e MYSQL_ROOT_PASSWORD=rc  -e MYSQL_USER=rc -e MYSQL_PASSWORD=rc -p 3306:3306 -d mysql:latest

echo "then just connect as usual with .."
echo "> mysql -u rc -h $DOCKER_IP -p "

# Connect
# mysql -u rc -h $DOCKER_IP -p

# Run the following
# create user 'rc'@'*'  identified by 'rc' ;
#