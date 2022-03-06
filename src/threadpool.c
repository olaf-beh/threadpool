/*
 * Copyright (c) 2016, Mathias Brossard <mathias@brossard.org>.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 * 
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file threadpool.c
 * @brief Threadpool implementation file
 */

#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include "threadpool.h"

typedef enum {
    immediate_shutdown = 1,
    graceful_shutdown  = 2
} threadpool_shutdown_t;

/**
 *  @struct threadpool_task
 *  @brief the work struct
 *
 *  @var function Pointer to the function that will perform the task.
 *  @var argument Argument to be passed to the function.
 */

typedef struct {
    void (*function)(void *);
    void *argument;
} threadpool_task_t;

/**
 * @struct thread_info_t
 * @brief Manages thread specific information.
 * @var id pthread id.
 * @var terminated 1 (true) if thread ist terminated.
 */
typedef struct {
    pthread_t id;
    char terminated;
} thread_info_t;

/**
 *  @struct threadpool
 *  @brief The threadpool struct
 *
 *  @var notify       Condition variable to notify worker threads.
 *  @var tinfo        Array of thread_info_t elements.
 *  @var tinfo_size   Size of tinfo array.
 *  @var thread_count Requested number of threads.
 *  @var queue        Array containing the task queue.
 *  @var queue_size   Size of the task queue.
 *  @var head         Index of the first element.
 *  @var tail         Index of the next element.
 *  @var count        Number of pending tasks
 *  @var shutdown     Flag indicating if the pool is shutting down
 *  @var started      Number of started threads
 */
struct threadpool_t {
  pthread_mutex_t lock;
  pthread_cond_t notify;
  thread_info_t *tinfo;
  int tinfo_size;
  threadpool_task_t *queue;
  int thread_count;
  int thread_count_limit;
  int queue_size;
  int head;
  int tail;
  int count;
  int shutdown;
  int started;
};

/**
 * @function void *threadpool_thread(void *threadpool)
 * @brief the worker thread
 * @param threadpool the pool which own the thread
 */
static void *threadpool_thread(void *threadpool);

int threadpool_free(threadpool_t *pool);

threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{
    threadpool_t *pool;
    int i;
    (void) flags;

    if(thread_count <= 0 || thread_count > MAX_THREADS || queue_size <= 0 || queue_size > MAX_QUEUE) {
        return NULL;
    }

    if((pool = (threadpool_t *)calloc(1, sizeof(threadpool_t))) == NULL) {
        goto err;
    }

    /* Initialize */
    pool->queue_size = queue_size;
    pool->tinfo_size = pool->thread_count = thread_count;

    /* Allocate thread info and task queue */
    pool->tinfo = (thread_info_t *)calloc(pool->tinfo_size, sizeof(thread_info_t));
    pool->queue = (threadpool_task_t *)malloc
        (sizeof(threadpool_task_t) * queue_size);

    /* Initialize mutex and conditional variable first */
    if((pthread_mutex_init(&(pool->lock), NULL) != 0) ||
       (pthread_cond_init(&(pool->notify), NULL) != 0) ||
       (pool->tinfo == NULL) ||
       (pool->queue == NULL)) {
        goto err;
    }

    /* Start worker threads */
    for(i = 0; i < pool->tinfo_size; i++) {
        if(pthread_create(&(pool->tinfo[i].id), NULL,
                          threadpool_thread, (void*)pool) != 0) {
            threadpool_destroy(pool, 0);
            return NULL;
        }
    }

    return pool;

 err:
    if(pool) {
        threadpool_free(pool);
    }
    return NULL;
}

int threadpool_add(threadpool_t *pool, void (*function)(void *),
                   void *argument, int flags)
{
    int err = 0;
    int next;
    (void) flags;

    if(pool == NULL || function == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    next = (pool->tail + 1) % pool->queue_size;

    do {
        /* Are we full ? */
        if(pool->count == pool->queue_size) {
            err = threadpool_queue_full;
            break;
        }

        /* Are we shutting down ? */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        /* Add task to queue */
        pool->queue[pool->tail].function = function;
        pool->queue[pool->tail].argument = argument;
        pool->tail = next;
        pool->count += 1;

        /* pthread_cond_broadcast */
        if(pthread_cond_signal(&(pool->notify)) != 0) {
            err = threadpool_lock_failure;
            break;
        }
    } while(0);

    if(pthread_mutex_unlock(&pool->lock) != 0) {
        err = threadpool_lock_failure;
    }

    return err;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{
    int i, err = 0;

    if(pool == NULL) {
        return threadpool_invalid;
    }

    if(pthread_mutex_lock(&(pool->lock)) != 0) {
        return threadpool_lock_failure;
    }

    do {
        /* Already shutting down */
        if(pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        pool->shutdown = (flags & threadpool_graceful) ?
            graceful_shutdown : immediate_shutdown;

        /* Wake up all worker threads */
        if((pthread_cond_broadcast(&(pool->notify)) != 0) ||
           (pthread_mutex_unlock(&(pool->lock)) != 0)) {
            err = threadpool_lock_failure;
            break;
        }

        /* Join all worker threads */
        for(i = 0; i < pool->tinfo_size; i++) {
            if(pthread_join(pool->tinfo[i].id, NULL) != 0) {
                err = threadpool_thread_failure;
            }
        }
    } while(0);

    /* Only if everything went well do we deallocate the pool */
    if(!err) {
        threadpool_free(pool);
    }
    return err;
}

int threadpool_free(threadpool_t *pool)
{
    if(pool == NULL || pool->started > 0) {
        return -1;
    }

    /* Did we manage to allocate ? */
    if(pool->tinfo) {
        free(pool->tinfo);
        free(pool->queue);

        /* Because we allocate pool->tinfo after initializing the
           mutex and condition variable, we're sure they're
           initialized. Let's lock the mutex just in case. */
        pthread_mutex_lock(&(pool->lock));
        pthread_mutex_destroy(&(pool->lock));
        pthread_cond_destroy(&(pool->notify));
    }
    free(pool);
    return 0;
}


static void *threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *)threadpool;
    threadpool_task_t task;

    int i;

    pthread_mutex_lock(&(pool->lock));
    /* get tinfo array index */
    for (i = 0; i < pool->tinfo_size; ++i) {
        if (pool->tinfo[i].id == pthread_self())
            break;
    }
    pool->tinfo[i].terminated = 0;
    pool->started++;
    pthread_mutex_unlock(&(pool->lock));

    for(;;) {
        /* Lock must be taken to wait on conditional variable */
        pthread_mutex_lock(&(pool->lock));

        /* Wait on condition variable, check for spurious wakeups.
           When returning from pthread_cond_wait(), we own the lock. */
        while((pool->count == 0) && (!pool->shutdown)) {
            pthread_cond_wait(&(pool->notify), &(pool->lock));
        }

        if((pool->shutdown == immediate_shutdown) ||
           ((pool->shutdown == graceful_shutdown) &&
            (pool->count == 0)) ||
           (pool->started > pool->thread_count)) {
            break;
        }

        /* Grab our task */
        task.function = pool->queue[pool->head].function;
        task.argument = pool->queue[pool->head].argument;
        pool->head = (pool->head + 1) % pool->queue_size;
        pool->count -= 1;

        /* Unlock */
        pthread_mutex_unlock(&(pool->lock));

        /* Get to work */
        (*(task.function))(task.argument);
    }

    pool->started--;
    pool->tinfo[i].terminated = 1;

    pthread_mutex_unlock(&(pool->lock));
    pthread_exit(NULL);
    //return(NULL);
}

int threadpool_resize(threadpool_t *pool, int resize)
{
    int i;

    /* no more than MAX_THREADS */
    resize = resize > MAX_THREADS ? MAX_THREADS : resize;
    /* not less than 1 */
    resize = resize <= 0 ? 1 : resize;

    pthread_mutex_lock(&(pool->lock));
    pool->thread_count = resize;
    pthread_mutex_unlock(&(pool->lock));

    if (resize > pool->tinfo_size) {
        /* Reallocate worker tinfo array */

        int size_prev = pool->tinfo_size;

        pthread_mutex_lock(&(pool->lock));
        pool->tinfo_size = resize;
        pool->tinfo = (thread_info_t *)realloc(pool->tinfo, sizeof(thread_info_t) * pool->tinfo_size);
        pthread_mutex_unlock(&(pool->lock));

        /* Initialize new array elements */
        memset(pool->tinfo + size_prev, 0, sizeof(thread_info_t) * (pool->tinfo_size - size_prev));

        /* Start additional worker threads for new tinfo array elements */
        for(i = size_prev; i < pool->tinfo_size; i++) {
            if(pthread_create(&(pool->tinfo[i].id), NULL,
                        threadpool_thread, (void*)pool) != 0) {
                return threadpool_thread_failure;
            }
        }
    }

    /* Restart worker threads if needed */
    pthread_mutex_lock(&(pool->lock));
    int started = pool->started;
    pthread_mutex_unlock(&(pool->lock));
    for(i = 0; i < pool->tinfo_size && started < resize; i++) {

        pthread_mutex_lock(&(pool->lock));
        int terminated = pool->tinfo[i].terminated;
        pthread_mutex_unlock(&(pool->lock));

        if (terminated) {
            if(pthread_join(pool->tinfo[i].id, NULL) != 0) {
                return threadpool_thread_failure;
            }
            if(pthread_create(&(pool->tinfo[i].id), NULL,
                        threadpool_thread, (void*)pool) != 0) {
                return threadpool_thread_failure;
            }
        }
    }

    return 0;
}

int threadpool_get_threat_count(threadpool_t *pool)
{
    return pool->thread_count;
}
