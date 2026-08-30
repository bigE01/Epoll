#define MAX_LENGTH 1024 
#define _GNU_SOURCE

#include <sys/epoll.h>
#include <sys/socket.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <fcntl.h>

int MAX_FDS = 100;
int PORT = 8081;
int MAX_EVENTS = 10;
int BACKLOG = 5;

typedef struct{
    int fd;
    int done;
    char buffer[MAX_LENGTH];
    int used_len;
} connection;

void close_connection(int fd, connection *connections[MAX_FDS], int ep_fd)
{
    epoll_ctl(ep_fd, EPOLL_CTL_DEL, fd, NULL);
    free(connections[fd]);
    connections[fd] = NULL;
    close(fd);
}

connection *create_connection(int fd)
{
    connection (*conn) = malloc(sizeof(connection));
    if (conn == NULL)
    {
        perror("memmory allocation failled");
        return NULL;
    }
    conn -> used_len = 0;
    conn -> done = 0;
    conn -> fd = fd;

    return conn;
}

// void run_server()
// {
//     while(1)
//     {
//         int n = epoll_wait(ep_fd, event, MAX_EVENTS, -1);
//         for (int i = 0; i < n; i++)
//         {
//             int fd = event[i].data.fd;
//             if(fd < 0)
//             {
//                 perror("fd was un succesfull");
//             }
//             int client_fd = socket(AF_INET, SOCK_STREAM, 0);
//             if(fd == listeningfd)
//             {
//                 //new data
//                 accept();
//                 recv(client_fd);
//                 if (memmem(buffer,used_len,"\r\n\r\n",4) == true)
//                 {

//                 }
//             }
//             else
//             {
//             //existing connection has data
//             }
//         }
//     }
// }

int main(int argc, char *argv[])
{   
    //epoll_wait();
    struct epoll_event events[MAX_EVENTS];
    connection *connections[MAX_FDS]; 
    struct sockaddr_in sock_addr;
    sock_addr.sin_family = AF_INET;
    sock_addr.sin_addr.s_addr = INADDR_ANY;
    sock_addr.sin_port = htons(PORT);
    socklen_t sock_len = sizeof(sock_addr);
    //create a new socket usnig ipv4
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1)
    { perror("creating socket failled"); exit(EXIT_FAILURE);}
    //bind the socket to a port and ip adress
    int bind_sock = bind(socket_fd, (struct sockaddr *)&sock_addr, sock_len);
    if (bind_sock == -1)
    { perror("socket binding failled"); exit(EXIT_FAILURE);}
    //listening to the socket_fd and the backlog is the max amount of connection waitting for a response
    int sock_listener = listen(socket_fd, BACKLOG);
    if (sock_listener == -1)
    { perror("socket listening failled"); exit(EXIT_FAILURE);}    
    int ep_fd = epoll_create1(0);
    if (ep_fd == -1)
    { perror("epol create failled"); exit(EXIT_FAILURE);}   
    events[0].events = EPOLLIN;
    events[0].data.fd = socket_fd;
    if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, socket_fd, &events[0]) == -1)
    {perror("epoll_ctl failed for listening socket"); exit(EXIT_FAILURE);}
    while(1)
    {
        int n = epoll_wait(ep_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; i++)
        {
            int fd = events[i].data.fd;
            if(fd < 0)
            {
                perror("fd was un succesfull");
            }
            if(fd == socket_fd)
            {
                //new connection
                struct epoll_event new_event;
                //accepts the connection
                int client_fd = accept(socket_fd, NULL, NULL);
                //checks that the connection didnt fail
                if (client_fd == -1)
                { perror("accept failled"); continue;}
                fcntl(client_fd, F_SETFL, O_NONBLOCK);
                new_event.events = EPOLLIN;
                new_event.data.fd = client_fd;
                //creates a new connection with client fd
                connections[client_fd] = create_connection(client_fd);
                if (connections[client_fd] == NULL)
                { close(client_fd); continue;}
                //registers client fd with epoll_ctl
                if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, client_fd, &new_event) == -1)
                { perror(" epoll ctl failled"); continue;}
            }
            else
            {
                //existing connection has data
                size_t size = MAX_LENGTH - connections[fd]->used_len;
                //status is how many bytes are incoming, 0 means the client closed the connection properly any thing over 0 is how many bytes are incoming and -1 is or an error or nothing is incoming
                int status = recv(fd,connections[fd]->buffer + connections[fd]->used_len, size, 0);
                if (status == 0)
                {
                    //connection was closed by client properly
                    close_connection(fd, connections, ep_fd);
                    continue;
                }
                if(status < 0)
                {
                    //error occured while trying to recv
                    perror("error occured in recv");
                    //checking errno if there is a real erron or just no new data yet, checking EPOLLIN
                    if (errno == EAGAIN)
                    {
                        perror("resources not availble currently");
                        continue;
                    }
                    else
                    {
                        perror("an error has occured");
                        close_connection(fd, connections, ep_fd);
                        continue;
                    }
                }
                if (status > 0)
                {
                    //connection not closed yet
                    connections[fd] -> used_len += status;
                    void *found = memmem(connections[fd]->buffer, connections[fd]->used_len,"\r\n\r\n", 4);
                    if (found == NULL) 
                    {


                        
                    }
                    else
                    {
                        connections[fd] -> done = 1;
                        const char *response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                        int respond = send(fd, response, strlen(response), 0);
                        close_connection(fd, connections, ep_fd);
                        if (errno == EAGAIN) continue;
                    }

                }   
            }
        }
    }
    //int bind_result = bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    return 0;
}