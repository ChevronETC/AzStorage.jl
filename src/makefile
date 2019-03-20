all:
	gcc `curl-config --cflags` -O3 -fopenmp -fPIC -c AzStorage.c
	gcc -shared -fopenmp -o libAzStorage.so AzStorage.o `curl-config --libs` ${LDFLAGS}

clean:
	rm -rf *.so *.o
