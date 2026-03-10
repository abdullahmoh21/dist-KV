#include "event_loop/event_loop.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>

struct event_loop {
    int epfd;
    struct epoll_event *events;
    int max_events;
};

event_loop_t *event_loop_create(int max_events) {
    event_loop_t *loop = malloc(sizeof(event_loop_t));
    if (!loop) {
        return NULL;
    }

    loop->epfd = epoll_create1(0);
    if (loop->epfd == -1) {
        free(loop);
        return NULL;
    }

    loop->events = calloc(max_events, sizeof(struct epoll_event));
    if (!loop->events) {
        close(loop->epfd);
        free(loop);
        return NULL;
    }

    loop->max_events = max_events;
    return loop;
}

void event_loop_destroy(event_loop_t *loop) {
    if (!loop) {
        return;
    }
    
    if (loop->epfd >= 0) {
        close(loop->epfd);
    }
    
    free(loop->events);
    free(loop);
}

int event_loop_add(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    struct epoll_event ee;
    memset(&ee, 0, sizeof(ee));
    ee.data.fd = fd;
    ee.events = 0;

    if (mask & EVENT_READABLE) {
        ee.events |= EPOLLIN;
    }
    if (mask & EVENT_WRITABLE) {
        ee.events |= EPOLLOUT;
    }

    return epoll_ctl(loop->epfd, EPOLL_CTL_ADD, fd, &ee);
}

int event_loop_mod(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    struct epoll_event ee;
    memset(&ee, 0, sizeof(ee));
    ee.data.fd = fd;
    ee.events = 0;

    if (mask & EVENT_READABLE) {
        ee.events |= EPOLLIN;
    }
    if (mask & EVENT_WRITABLE) {
        ee.events |= EPOLLOUT;
    }

    return epoll_ctl(loop->epfd, EPOLL_CTL_MOD, fd, &ee);
}

int event_loop_del(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    // For epoll, we'll just remove the fd entirely
    // In a more sophisticated implementation, you might want to 
    // keep track of the current mask and only remove specific events
    (void)mask; // Unused for now
    return epoll_ctl(loop->epfd, EPOLL_CTL_DEL, fd, NULL);
}

int event_loop_wait(event_loop_t *loop, int timeout_ms, fired_event_t *events, int max_events) {
    if (!loop || !events) {
        return -1;
    }

    int n = epoll_wait(loop->epfd, loop->events, loop->max_events, timeout_ms);
    if (n <= 0) {
        return n;
    }

    // Convert epoll events to our generic fired_event format
    int count = n < max_events ? n : max_events;
    for (int i = 0; i < count; i++) {
        events[i].fd = loop->events[i].data.fd;
        events[i].mask = 0;

        if (loop->events[i].events & EPOLLIN) {
            events[i].mask |= EVENT_READABLE;
        }
        if (loop->events[i].events & EPOLLOUT) {
            events[i].mask |= EVENT_WRITABLE;
        }
        if (loop->events[i].events & (EPOLLERR | EPOLLHUP)) {
            // Treat errors as both readable and writable to let the application handle them
            events[i].mask |= EVENT_READABLE;
        }
    }

    return count;
}
