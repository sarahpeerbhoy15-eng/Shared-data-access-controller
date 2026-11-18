all:
	gcc -Wall -Wextra -pthread controller.c -o controller

run: all
	./controller sync

fair: all
	./controller fair 6 3

conflict: all
	./controller nosync

clean:
	rm -f controller access.log
