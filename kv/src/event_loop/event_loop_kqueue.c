#include "event_loop/event_loop.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>

struct event_loop {
    int kqfd;
    struct kevent *events;
    int max_events;
};

event_loop_t *event_loop_create(int max_events) {
    event_loop_t *loop = malloc(sizeof(event_loop_t));
    if (!loop) {
        return NULL;
    }

    loop->kqfd = kqueue();
    if (loop->kqfd == -1) {
        free(loop);
        return NULL;
    }

    loop->events = calloc(max_events, sizeof(struct kevent));
    if (!loop->events) {
        close(loop->kqfd);
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
    
    if (loop->kqfd >= 0) {
        close(loop->kqfd);
    }
    
    free(loop->events);
    free(loop);
}

int event_loop_add(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    struct kevent changes[2];
    int nchanges = 0;

    if (mask & EVENT_READABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
        nchanges++;
    }
    if (mask & EVENT_WRITABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, NULL);
        nchanges++;
    }

    if (nchanges == 0) {
        return -1;
    }

    return kevent(loop->kqfd, changes, nchanges, NULL, 0, NULL);
}

int event_loop_mod(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    // First delete existing filters, then add new ones
    struct kevent changes[4];
    int nchanges = 0;

    // Delete existing filters
    EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    nchanges++;
    EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    nchanges++;

    // Add new filters based on mask
    if (mask & EVENT_READABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, NULL);
        nchanges++;
    }
    if (mask & EVENT_WRITABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, NULL);
        nchanges++;
    }

    // Ignore errors from delete operations (fd might not have those filters)
    kevent(loop->kqfd, changes, 2, NULL, 0, NULL);
    
    // Apply the add operations
    if (nchanges > 2) {
        return kevent(loop->kqfd, &changes[2], nchanges - 2, NULL, 0, NULL);
    }
    
    return 0;
}

int event_loop_del(event_loop_t *loop, int fd, int mask) {
    if (!loop || fd < 0) {
        return -1;
    }

    struct kevent changes[2];
    int nchanges = 0;

    if (mask & EVENT_READABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        nchanges++;
    }
    if (mask & EVENT_WRITABLE) {
        EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        nchanges++;
    }

    if (nchanges == 0) {
        EV_SET(&changes[nchanges], fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        nchanges++;
        EV_SET(&changes[nchanges], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        nchanges++;
    }

    return kevent(loop->kqfd, changes, nchanges, NULL, 0, NULL);
}

int event_loop_wait(event_loop_t *loop, int timeout_ms, fired_event_t *events, int max_events) {
    if (!loop || !events) {
        return -1;
    }

    struct timespec timeout;
    struct timespec *timeout_ptr = NULL;

    if (timeout_ms >= 0) {
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_nsec = (timeout_ms % 1000) * 1000000;
        timeout_ptr = &timeout;
    }

    int n = kevent(loop->kqfd, NULL, 0, loop->events, loop->max_events, timeout_ptr);
    if (n <= 0) {
        return n;
    }

    // Convert kqueue events to our generic fired_event format
    int count = n < max_events ? n : max_events;
    for (int i = 0; i < count; i++) {
        events[i].fd = (int)loop->events[i].ident;
        events[i].mask = 0;

        if (loop->events[i].filter == EVFILT_READ) {
            events[i].mask |= EVENT_READABLE;
        }
        if (loop->events[i].filter == EVFILT_WRITE) {
            events[i].mask |= EVENT_WRITABLE;
        }
        if (loop->events[i].flags & EV_EOF) {
            // Treat EOF as readable so the application can handle it
            events[i].mask |= EVENT_READABLE;
        }
        if (loop->events[i].flags & EV_ERROR) {
            // Treat errors as readable
            events[i].mask |= EVENT_READABLE;
        }
    }

    return count;
}
