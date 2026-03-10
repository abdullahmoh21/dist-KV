#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

typedef struct event_loop event_loop_t;

typedef struct fired_event {
    int fd;
    int mask;
} fired_event_t;

/**
 * Create a new event loop that can handle up to max_events events
 * @param max_events Maximum number of events that can be polled at once
 * @return Pointer to event_loop_t or NULL on failure
 */
event_loop_t *event_loop_create(int max_events);

/**
 * Destroy the event loop and free all resources
 * @param loop Event loop to destroy
 */
void event_loop_destroy(event_loop_t *loop);

/**
 * Add a file descriptor to the event loop with the specified event mask
 * @param loop Event loop instance
 * @param fd File descriptor to monitor
 * @param mask Event mask (EVENT_READABLE | EVENT_WRITABLE)
 * @return 0 on success, -1 on failure
 */
int event_loop_add(event_loop_t *loop, int fd, int mask);

/**
 * Modify the event mask for an existing file descriptor
 * @param loop Event loop instance
 * @param fd File descriptor to modify
 * @param mask New event mask (EVENT_READABLE | EVENT_WRITABLE)
 * @return 0 on success, -1 on failure
 */
int event_loop_mod(event_loop_t *loop, int fd, int mask);

/**
 * Remove a file descriptor from the event loop
 * @param loop Event loop instance
 * @param fd File descriptor to remove
 * @param mask Event mask to remove (can be partial)
 * @return 0 on success, -1 on failure
 */
int event_loop_del(event_loop_t *loop, int fd, int mask);

/**
 * Wait for events with optional timeout
 * @param loop Event loop instance
 * @param timeout_ms Timeout in milliseconds (-1 for infinite)
 * @param events Array to store fired events
 * @param max_events Maximum number of events to return
 * @return Number of events fired, 0 on timeout, -1 on error
 */
int event_loop_wait(event_loop_t *loop, int timeout_ms, fired_event_t *events, int max_events);

#endif // EVENT_LOOP_H
