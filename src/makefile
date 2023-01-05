local:
	$(CC) `curl-config --cflags` -O3 -fopenmp -fPIC -c -o AzStorage.o AzStorage.c
	$(CC) -shared -fopenmp -o libAzStorage.so AzStorage.o `curl-config --libs`

yggdrasil: libAzStorage.$(dlext)

libAzStorage.$(dlext): AzStorage.o
	$(CC) -shared -fopenmp -o $@ $^ `curl-config --libs`

AzStorage.$o: AzStorage.c
	$(CC) `curl-config --cflags` -O3 -fopenmp -fPIC -c -o $@ $^

clean:
	rm -rf *.so *.$(dlext) *.o
