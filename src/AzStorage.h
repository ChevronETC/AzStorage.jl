#ifndef AZSTORAGE_H
#define AZSTORAGE_H

#include <curl/curl.h>
#include <math.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define BUFFER_SIZE 16000 // this needs to be large to accomodate large OAuth2 tokens
#define API_HEADER_BUFFER_SIZE 512
#define MAXIMUM_BACKOFF 256.0
#define CURLE_TIMEOUT 600L /* 5 hours */

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

struct ResponseCodes {
    long http;
    long curl;
    int retry_after;
};

struct DataStruct {
    char *data;
    size_t datasize;
    size_t currentsize;
};

struct HeaderStruct {
    int retry_after;
};

char*
api_header();

int
exponential_backoff(
    int i,
    int retry_after);

void
curl_init(
    int   n_http_retry_codes,
    int   n_curl_retry_codes,
    long *http_retry_codes,
    long *curl_retry_codes,
    char *api_version);

size_t
write_callback_null(
    char *ptr,
    size_t size,
    size_t nmemb,
    void *userdata);

size_t
callback_retry_after_header(
        char   *ptr,
        size_t  size,
        size_t  nmemb,
        void   *datavoid);

void
curl_authorization(
    char *token,
    char *authorization);

struct ResponseCodes
curl_writebytes_block_retry_threaded(
    char  *token,
    char  *storageaccount,
    char  *containername,
    char  *blobname,
    char **blockids,
    char  *data,
    size_t datasize,
    int    nthreads,
    int    nblocks,
    int    nretry,
    int    verbose);

struct ResponseCodes
curl_readbytes_retry_threaded(
    char  *token,
    char  *storageaccount,
    char  *containername,
    char  *blobname,
    char  *data,
    size_t dataoffset,
    size_t datasize,
    int    nthreads,
    int    nretry,
    int    verbose);

struct ResponseCodes
curl_refresh_tokens_retry(
    char          *bearer_token,
    char          *refresh_token,
    unsigned long *expiry,
    char          *scope,
    char          *resource,
    char          *clientid,
    char          *client_secret,
    char          *tenant,
    int            nretry,
    int            verbose);

int
isrestretrycode(
    struct ResponseCodes responsecodes);

#endif