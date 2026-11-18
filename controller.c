/* STEP 3: Add semaphores - readers share, writers get exclusive access */
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

#define WORK_US    500000             /* time spent "working" on the data */
#define STAGGER_US 100000             /* gap between request arrivals */

int data = 100;
int read_count = 0;                   /* how many readers are inside right now */
sem_t wrt;                            /* 1 = data is free for a writer */
sem_t mutex;                          /* protects read_count */
FILE *logfile;
struct timespec start;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

/* milliseconds since the program started */
double now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (t.tv_sec - start.tv_sec) * 1000.0 + (t.tv_nsec - start.tv_nsec) / 1e6;
}

/* prints one event to the screen (coloured) and to access.log (plain) */
void log_event(int is_writer, int id, const char *event, int value) {
    const char *role  = is_writer ? "Writer" : "Reader";
    const char *color = is_writer ? "\033[33m" : "\033[36m";
    char line[100];

    if (value >= 0)
        snprintf(line, sizeof line, "[%5.0f ms] %s %d: %-10s (data = %d)",
                 now_ms(), role, id, event, value);
    else
        snprintf(line, sizeof line, "[%5.0f ms] %s %d: %s",
                 now_ms(), role, id, event);

    pthread_mutex_lock(&log_lock);    /* so two threads never mix their lines */
    printf("%s%s\033[0m\n", color, line);
    fprintf(logfile, "%s\n", line);
    pthread_mutex_unlock(&log_lock);
}

void *reader(void *arg) {
    int id = *(int *)arg;
    log_event(0, id, "REQUEST", -1);

    sem_wait(&mutex);
    read_count++;
    if (read_count == 1)              /* first reader locks writers out */
        sem_wait(&wrt);
    sem_post(&mutex);

    log_event(0, id, "GRANTED", -1);
    int value = data;
    usleep(WORK_US);
    log_event(0, id, "DONE-READ", value);

    sem_wait(&mutex);
    read_count--;
    if (read_count == 0)              /* last reader lets writers back in */
        sem_post(&wrt);
    sem_post(&mutex);
    return NULL;
}

void *writer(void *arg) {
    int id = *(int *)arg;
    log_event(1, id, "REQUEST", -1);

    sem_wait(&wrt);                   /* wait until nobody else is using the data */

    log_event(1, id, "GRANTED", -1);
    int temp = data;
    usleep(WORK_US);
    data = temp + 10;
    log_event(1, id, "DONE-WRITE", data);

    sem_post(&wrt);                   /* hand the data back */
    return NULL;
}

int main() {
    pthread_t r[3], w[2];
    int rid[3] = {1, 2, 3}, wid[2] = {1, 2};

    sem_init(&wrt, 0, 1);
    sem_init(&mutex, 0, 1);
    logfile = fopen("access.log", "w");
    clock_gettime(CLOCK_MONOTONIC, &start);

    /* requests arrive one after another: W1, R1, R2, W2, R3 */
    pthread_create(&w[0], NULL, writer, &wid[0]); usleep(STAGGER_US);
    pthread_create(&r[0], NULL, reader, &rid[0]); usleep(STAGGER_US);
    pthread_create(&r[1], NULL, reader, &rid[1]); usleep(STAGGER_US);
    pthread_create(&w[1], NULL, writer, &wid[1]); usleep(STAGGER_US);
    pthread_create(&r[2], NULL, reader, &rid[2]);

    for (int i = 0; i < 2; i++) pthread_join(w[i], NULL);
    for (int i = 0; i < 3; i++) pthread_join(r[i], NULL);

    printf("\nFinal data = %d (expected 120)\n", data);
    fclose(logfile);
    sem_destroy(&wrt);
    sem_destroy(&mutex);
    return 0;
}
