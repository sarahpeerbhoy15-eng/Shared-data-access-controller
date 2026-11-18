/* Shared Data Access Controller - Reader/Writer model (final version)
 * Records every read/write request, coordinates access with semaphores,
 * detects conflicts, and prints a statistics summary.
 *   Usage: ./controller [sync|fair|nosync] [num_readers] [num_writers]   */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <time.h>

#define MAX_THREADS 50
#define WORK_US    500000             /* time spent "working" on the data */
#define STAGGER_US 100000             /* gap between request arrivals */

enum Mode { NOSYNC, SYNC, FAIR };
enum Mode mode = SYNC;

/* statistics + conflict detector (protected by stats_lock) */
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
int active_readers = 0, active_writers = 0;   /* who is inside RIGHT NOW */
int max_readers = 0;                          /* most readers inside at once */
int conflicts = 0;                            /* illegal overlaps detected */
int n_reads = 0, n_writes = 0;
double wait_reads = 0, wait_writes = 0;       /* total waiting time (ms) */

int data = 100;
int read_count = 0;                   /* how many readers are inside right now */
sem_t wrt;                            /* 1 = data is free for a writer */
sem_t mutex;                          /* protects read_count */
sem_t turnstile;                      /* FAIR mode: stops new readers while a writer waits */
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

/* called the moment a request is granted: records the wait and checks the rules
 *   rule 1: a writer must be completely alone
 *   rule 2: readers may overlap with each other, but never with a writer   */
void on_granted(int is_writer, double waited) {
    pthread_mutex_lock(&stats_lock);
    if (is_writer) {
        if (active_writers > 0 || active_readers > 0) conflicts++;
        active_writers++;
        n_writes++;
        wait_writes += waited;
    } else {
        if (active_writers > 0) conflicts++;
        active_readers++;
        if (active_readers > max_readers) max_readers = active_readers;
        n_reads++;
        wait_reads += waited;
    }
    pthread_mutex_unlock(&stats_lock);
}

void on_done(int is_writer) {
    pthread_mutex_lock(&stats_lock);
    if (is_writer) active_writers--; else active_readers--;
    pthread_mutex_unlock(&stats_lock);
}

void *reader(void *arg) {
    int id = *(int *)arg;
    double requested = now_ms();
    log_event(0, id, "REQUEST", -1);

    if (mode == FAIR) {               /* queue behind any waiting writer */
        sem_wait(&turnstile);
        sem_post(&turnstile);
    }
    if (mode != NOSYNC) {
        sem_wait(&mutex);
        read_count++;
        if (read_count == 1)          /* first reader locks writers out */
            sem_wait(&wrt);
        sem_post(&mutex);
    }

    on_granted(0, now_ms() - requested);
    log_event(0, id, "GRANTED", -1);
    int value = data;
    usleep(WORK_US);
    log_event(0, id, "DONE-READ", value);
    on_done(0);

    if (mode != NOSYNC) {
        sem_wait(&mutex);
        read_count--;
        if (read_count == 0)          /* last reader lets writers back in */
            sem_post(&wrt);
        sem_post(&mutex);
    }
    return NULL;
}

void *writer(void *arg) {
    int id = *(int *)arg;
    double requested = now_ms();
    log_event(1, id, "REQUEST", -1);

    if (mode == FAIR)
        sem_wait(&turnstile);         /* closes the door to NEW readers */
    if (mode != NOSYNC)
        sem_wait(&wrt);               /* wait until nobody else is using the data */

    on_granted(1, now_ms() - requested);
    log_event(1, id, "GRANTED", -1);
    int temp = data;
    usleep(WORK_US);
    data = temp + 10;
    log_event(1, id, "DONE-WRITE", data);
    on_done(1);

    if (mode != NOSYNC)
        sem_post(&wrt);               /* hand the data back */
    if (mode == FAIR)
        sem_post(&turnstile);         /* reopen the door */
    return NULL;
}

int main(int argc, char *argv[]) {
    int nr = 3, nw = 2;
    const char *mode_name = "sync";

    if (argc > 1) mode_name = argv[1];
    if (argc > 2) nr = atoi(argv[2]);
    if (argc > 3) nw = atoi(argv[3]);

    if      (strcmp(mode_name, "sync")   == 0) mode = SYNC;
    else if (strcmp(mode_name, "fair")   == 0) mode = FAIR;
    else if (strcmp(mode_name, "nosync") == 0) mode = NOSYNC;
    else { fprintf(stderr, "Usage: %s [sync|fair|nosync] [readers] [writers]\n", argv[0]); return 1; }

    if (nr < 0 || nw < 0 || nr > MAX_THREADS || nw > MAX_THREADS) {
        fprintf(stderr, "Readers and writers must be between 0 and %d\n", MAX_THREADS);
        return 1;
    }

    pthread_t r[MAX_THREADS], w[MAX_THREADS];
    int rid[MAX_THREADS], wid[MAX_THREADS];

    sem_init(&wrt, 0, 1);
    sem_init(&mutex, 0, 1);
    sem_init(&turnstile, 0, 1);
    logfile = fopen("access.log", "w");
    clock_gettime(CLOCK_MONOTONIC, &start);

    printf("=== Mode: %s | %d readers, %d writers ===\n", mode_name, nr, nw);

    /* requests arrive one after another: W, R, R, W, R, R, W ... */
    int ri = 0, wi = 0;
    for (int i = 0; i < nr + nw; i++) {
        int spawn_writer = (wi < nw) && (i % 3 == 0 || ri >= nr);
        if (spawn_writer) {
            wid[wi] = wi + 1;
            pthread_create(&w[wi], NULL, writer, &wid[wi]);
            wi++;
        } else {
            rid[ri] = ri + 1;
            pthread_create(&r[ri], NULL, reader, &rid[ri]);
            ri++;
        }
        usleep(STAGGER_US);
    }

    for (int i = 0; i < nw; i++) pthread_join(w[i], NULL);
    for (int i = 0; i < nr; i++) pthread_join(r[i], NULL);

    int expected = 100 + 10 * nw;
    int ok = (data == expected && conflicts == 0);
    printf("\n================ SUMMARY ================\n");
    printf("Mode                : %s\n", mode_name);
    printf("Total time          : %.0f ms\n", now_ms());
    printf("Reads / avg wait    : %d / %.0f ms\n", n_reads, n_reads ? wait_reads / n_reads : 0);
    printf("Writes / avg wait   : %d / %.0f ms\n", n_writes, n_writes ? wait_writes / n_writes : 0);
    printf("Max readers at once : %d\n", max_readers);
    printf("Conflicts detected  : %d\n", conflicts);
    printf("Final data          : %d (expected %d)\n", data, expected);
    printf("%sRESULT: %s\033[0m\n", ok ? "\033[32m" : "\033[31m",
           ok ? "PASS - no conflicting operations" : "FAIL - conflicting operations occurred");
    fprintf(logfile, "\nConflicts: %d | Final data: %d (expected %d) | %s\n",
            conflicts, data, expected, ok ? "PASS" : "FAIL");
    fclose(logfile);
    sem_destroy(&wrt);
    sem_destroy(&mutex);
    sem_destroy(&turnstile);
    return 0;
}
