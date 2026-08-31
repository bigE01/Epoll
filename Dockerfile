FROM gcc:latest
WORKDIR /app
COPY Epoll.c .
COPY www/ ./www/
