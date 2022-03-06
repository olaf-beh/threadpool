#define THREAD 32
#define QUEUE  2000

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>

#include "threadpool.h"

int tasks = 0, done = 0;
pthread_mutex_t lock;

void dummy_task(void *arg) {
    usleep(5000);
    pthread_mutex_lock(&lock);
    done++;
    pthread_mutex_unlock(&lock);
}
#define SPEED_TEST { \
    usleep(10000); done_prev = done; usleep(25000); \
    fprintf(stderr, "%4d tasks done (%.3f/ms)\n", done, (done - done_prev) / 25.0); \
}

int main(int argc, char **argv)
{
    threadpool_t *pool;
    int done_prev;

    fprintf(stderr, "=== Start resize test ===\n");
    pthread_mutex_init(&lock, NULL);

    assert((pool = threadpool_create(THREAD, QUEUE, 0)) != NULL);
    fprintf(stderr, "Pool started with %d threads and "
            "queue size of %d\n", THREAD, QUEUE);

    while(threadpool_add(pool, &dummy_task, NULL, 0) == 0) {
        tasks++;
    }

    fprintf(stderr, "Added %d tasks\n", tasks);

    SPEED_TEST;

    // resize up
    assert( 0 == threadpool_resize(pool, 64));
    fprintf(stderr, "Resize number of working threads to %d\n", threadpool_get_threat_count(pool));

    SPEED_TEST;

    // resize down
    assert( 0 == threadpool_resize(pool, threadpool_get_threat_count(pool)/8));
    fprintf(stderr, "Resize number of working threads to %d\n", threadpool_get_threat_count(pool));

    SPEED_TEST;

    // resize up again
    assert( 0 == threadpool_resize(pool, threadpool_get_threat_count(pool)*100));
    fprintf(stderr, "Resize number of working threads to %d\n", threadpool_get_threat_count(pool));

    SPEED_TEST;

    while((tasks / 2) > done) {
        usleep(10000);
    }
    assert(threadpool_destroy(pool, 0) == 0);
    fprintf(stderr, "Did %d tasks\n", done);
    fprintf(stderr, "=== End resize test ===\n");

    return 0;
}
