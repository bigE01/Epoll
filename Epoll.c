#define MAX_LENGTH 1024 
#define _GNU_SOURCE
#define NUM_WORRKERS 4
#define QUEUE_SIZE 20
#define MAX_FDS 100

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
#include <pthread.h>
#include <signal.h>

volatile sig_atomic_t running = 1;
int PORT = 8081;
int MAX_EVENTS = 10;
int BACKLOG = 5;

typedef struct{
    int fd;
    int done;
    char buffer[MAX_LENGTH];
    int used_len;
} connection;

typedef struct{
    int workingFd;
    char dullPath[512];
} job;

typedef struct {
    job jobs[QUEUE_SIZE];
    int count;              // how many jobs are currently waiting
    int front;              // index of the next job to pop
    int back;                // index where the next pushed job goes
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
} job_queue;

job_queue queue;
connection *connections[MAX_FDS];
int ep_fd;

void handle_sigterm(int sig) {
    (void)sig;       // unused, silences a compiler warning
    running = 0;
}

void queue_init(job_queue *q)
{
    q->count = 0;
    q->front = 0;
    q->back = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

// Called by the epoll thread once a request is fully parsed.
int queue_push(job_queue *q, job new_job)
{
    pthread_mutex_lock(&q->lock);
    if (q->count == QUEUE_SIZE) 
    {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }
    q->jobs[q->back] = new_job;
    q->back = (q->back + 1) % QUEUE_SIZE;   // wrap around, circular buffer
    q->count++;

    pthread_cond_signal(&q->not_empty);      // wake up one sleeping worker, if any
    pthread_mutex_unlock(&q->lock);
    return 0;
}

// Called by a worker thread. Blocks (sleeps, no CPU spent) if the queue is empty.
job queue_pop(job_queue *q)
{
    pthread_mutex_lock(&q->lock);

    while (q->count == 0) {
        // Releases the lock while sleeping, reacquires it automatically upon waking.
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    job result = q->jobs[q->front];
    q->front = (q->front + 1) % QUEUE_SIZE;
    q->count--;

    pthread_mutex_unlock(&q->lock);
    return result;
}

// Keeps calling send() until all 'length' bytes have been sent,
// since a single send() call isn't guaranteed to send everything at once.
// Returns 0 on success, -1 if a real error occurs partway through.
int send_all(int fd, const void *data, size_t length)
{
    size_t total_sent = 0;
    const char *ptr = data;  // char* so we can do pointer arithmetic in bytes

    while (total_sent < length) {
        ssize_t sent = send(fd, ptr + total_sent, length - total_sent, 0);
        if (sent == -1) {
            if (errno == EAGAIN) {
                continue;  // socket temporarily can't accept more — retry
            }
            perror("send failed");
            return -1;  // real error
        }
        total_sent += sent;
    }
    return 0;
}

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

// pthread_create requires this exact signature: takes and returns void*.
void *worker_thread(void *arg)
{
    job_queue *q = (job_queue *)arg;

    while (1) {
        job current_job = queue_pop(q);
        int fd = current_job.workingFd;

        FILE *file = fopen(current_job.dullPath, "rb");
        if (file == NULL) {
            const char *response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            send_all(fd, response, strlen(response));
            close_connection(fd, connections, ep_fd);
            continue;
        }

        fseek(file, 0, SEEK_END);
        long file_size = ftell(file);
        fseek(file, 0, SEEK_SET);

        char *file_data = malloc(file_size);
        if (file_data == NULL) {
            fclose(file);
            close_connection(fd, connections, ep_fd);
            continue;
        }

        size_t bytes_read = fread(file_data, 1, file_size, file);
        if (bytes_read != (size_t)file_size) {
            free(file_data);
            fclose(file);
            close_connection(fd, connections, ep_fd);
            continue;
        }

        char header[256];
        snprintf(header, sizeof(header),
            "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n", file_size);
        send_all(fd, header, strlen(header));
        send_all(fd, file_data, file_size);

        free(file_data);
        fclose(file);
        close_connection(fd, connections, ep_fd);
    }
    return NULL;
}

int main(int argc, char *argv[])
{   
    struct sigaction sa;
    sa.sa_handler = handle_sigterm;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;   // deliberately NOT using SA_RESTART, so epoll_wait gets interrupted
    if (sigaction(SIGTERM, &sa, NULL) == -1) 
    {
        perror("sigaction failed");
        exit(EXIT_FAILURE);
    }
    struct epoll_event events[MAX_EVENTS];
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
    ep_fd = epoll_create1(0);
    if (ep_fd == -1)
    { perror("epol create failled"); exit(EXIT_FAILURE);}   
    events[0].events = EPOLLIN;
    events[0].data.fd = socket_fd;
    if (epoll_ctl(ep_fd, EPOLL_CTL_ADD, socket_fd, &events[0]) == -1)
    {perror("epoll_ctl failed for listening socket"); exit(EXIT_FAILURE);}
    queue_init(&queue);
    pthread_t workers[4];
    job new_job;
    for (int i = 0; i < NUM_WORRKERS; i++) {
    pthread_create(&workers[i], NULL, worker_thread, &queue);
    }
    while(running)
    {
        int n = epoll_wait(ep_fd, events, MAX_EVENTS, -1);
        if (n == -1) {
            if (errno == EINTR) {
                continue;   // interrupted by the signal — loop back, 'while(running)' catches the exit
            }
            perror("epoll_wait failed");
            break;
        }
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
                if (client_fd >= MAX_FDS)
                { perror("client_fd exceeds MAX_FDS, rejecting connection"); close(client_fd);
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
                    if (found != NULL) 
                    {
                        // getitng the url of the file recived
                        // space1 = position of the space right after the method (GET)
                        // space2 = position of the space right after the path
                        char *space1 = strchr(connections[fd]->buffer, ' ');
                        char *space2 = strchr(space1 + 1, ' ');
                        if (space1 == NULL || space2 == NULL) {
                            close_connection(fd, connections, ep_fd);
                            continue;
                        }

                        // Copy just the path substring into its own buffer
                        char path[256];
                        size_t path_len = space2 - space1 - 1;
                        if (path_len >= sizeof(path)) {
                            close_connection(fd, connections, ep_fd);
                            continue;
                        }
                        memcpy(path, space1 + 1, path_len);
                        path[path_len] = '\0';

                        // Reject path traversal attempts BEFORE building any real filesystem path
                        if (strstr(path, "..") != NULL) {
                            close_connection(fd, connections, ep_fd);
                            continue;
                        }

                        // Build the real filesystem path from the web root + requested path
                        char full_path[512];
                        snprintf(full_path, sizeof(full_path), "www%s", path);
                        printf("full_path: %s\n", full_path);
                        fflush(stdout);
                        new_job.workingFd = fd;
                        strncpy(new_job.dullPath, full_path, sizeof(new_job.dullPath));
                        if (queue_push(&queue, new_job) == -1) {
                            const char *response = "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n";
                            send_all(fd, response, strlen(response));
                            close_connection(fd, connections, ep_fd);
                        }
                    }
                }   
            }
        }
    }
    close(socket_fd);
    printf("Server shutting down cleanly\n");
    return 0;
}