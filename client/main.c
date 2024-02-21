//for struct addrinfo (for some reason this is necessary)
#define _GNU_SOURCE

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdbool.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

void xntcp_send(int sockfd, char* buf, int len) {
    ssize_t written = 0;
    ssize_t n;

    while (written < len) {
        if ((n = send(sockfd, buf + written, len - written, 0)) <= 0) {
            if (n < 0 && errno == EINTR) //interrupted but not error, so we need to try again
                n = 0;
            else {
                exit(1); //real error
            }
        }

        written += n;
    }
}

bool xntcp_recv(int sockfd, char* buf, int len) {
    ssize_t nread = 0;
    ssize_t n;
    while (nread < len) {
        if ((n = recv(sockfd, buf + nread, len - nread, 0)) < 0) {
            if (n < 0 && errno == EINTR)
                n = 0;
            else
                exit(1);
        } else if (n == 0) {
            //connection ended
            return false;
        }

        nread += n;
    }

    return true;
}

int xnclient_connect(const char* hostname, const char* port) {
    struct addrinfo hints;
    struct addrinfo* servinfo;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, port, &hints, &servinfo) != 0)
        return -1;

    //connect to first result possible
    struct addrinfo* p;
    int sockfd;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            continue;
        }

        break;
    }

    if (p == NULL)
        return -1;

    freeaddrinfo(servinfo);

    return sockfd;
}

void xnclient_disconnect(int sockfd) {
    close(sockfd);
}

int main(int argc, char **argv) {
    int fd = xnclient_connect("127.0.0.1", "3000");
    int result;
    xntcp_recv(fd, (char*)&result, sizeof(int));
    printf("result: %d\n", result);
    xnclient_disconnect(fd);
    return 0;
}
