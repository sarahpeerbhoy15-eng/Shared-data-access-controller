/* STEP 1: The problem - shared data with NO coordination */
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

int data = 100;                       /* the shared data */

void *reader(void *arg) {
    int id = *(int *)arg;
    printf("Reader %d reads %d\n", id, data);
    usleep(500000);                   /* pretend to work for 0.5 s */
    return NULL;
}

void *writer(void *arg) {
    int id = *(int *)arg;
    int temp = data;                  /* 1. read the old value */
    usleep(500000);                   /* 2. work (others can sneak in here!) */
    data = temp + 10;                 /* 3. write the new value back */
    printf("Writer %d wrote %d\n", id, data);
    return NULL;
}

int main() {
    pthread_t r[3], w[2];
    int rid[3] = {1, 2, 3}, wid[2] = {1, 2};

    for (int i = 0; i < 2; i++) pthread_create(&w[i], NULL, writer, &wid[i]);
    for (int i = 0; i < 3; i++) pthread_create(&r[i], NULL, reader, &rid[i]);
    for (int i = 0; i < 2; i++) pthread_join(w[i], NULL);
    for (int i = 0; i < 3; i++) pthread_join(r[i], NULL);

    printf("Final data = %d (expected 120)\n", data);
    return 0;
}
