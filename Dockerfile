FROM gcc:latest

WORKDIR /app

COPY epoll.c .
