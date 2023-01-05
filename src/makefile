libAzStorage.$(dlext): AzStorage.o
	$(CC) -shared -fopenmp -o $@ $^ `curl-config --libs`

AzStorage.o: AzStorage.c
	$(CC) `curl-config --cflags` -O3 -fopenmp -fPIC -c -o $@ $^

clean:
	rm -rf *.$(dlext) *.o
