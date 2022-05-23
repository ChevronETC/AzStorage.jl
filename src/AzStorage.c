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

int
exponential_backoff(
        int i)
{
    double sleeptime = MIN(pow(2.0, (double)i), MAXIMUM_BACKOFF) + 1.0*rand()/RAND_MAX;
    double sleeptime_seconds = floor(sleeptime);
    double sleeptime_nanoseconds = (long)((sleeptime - sleeptime_seconds) * 1000000000.0);

    struct timespec ts_sleeptime, ts_remainingtime;

    ts_sleeptime.tv_sec = (long)sleeptime_seconds;
    ts_sleeptime.tv_nsec = (long)sleeptime_nanoseconds;

    return nanosleep(&ts_sleeptime, &ts_remainingtime);
}

int N_HTTP_RETRY_CODES = 0;
int N_CURL_RETRY_CODES = 0;
long *HTTP_RETRY_CODES = NULL;
long *CURL_RETRY_CODES = NULL;
char API_HEADER[API_HEADER_BUFFER_SIZE];

void
curl_init(
        int   n_http_retry_codes,
        int   n_curl_retry_codes,
        long *http_retry_codes,
        long *curl_retry_codes,
        char *api_version)
{
    HTTP_RETRY_CODES = http_retry_codes;
    N_HTTP_RETRY_CODES = n_http_retry_codes;

    CURL_RETRY_CODES = curl_retry_codes;
    N_CURL_RETRY_CODES = n_curl_retry_codes;

    snprintf(API_HEADER, API_HEADER_BUFFER_SIZE, "x-ms-version: %s", api_version);

    curl_global_init(CURL_GLOBAL_ALL);
}

struct ResponseCodes {
    long http;
    long curl;
};

/*
https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
https://curl.haxx.se/libcurl/c/libcurl-errors.html
*/
int
isrestretrycode(
        struct ResponseCodes responsecodes)
{
    int i;

    for (i = 0; i < N_HTTP_RETRY_CODES; i++) {
        if (responsecodes.http == HTTP_RETRY_CODES[i]) {
            return 1;
        }
    }

    for (i = 0; i < N_CURL_RETRY_CODES; i++) {
        if (responsecodes.curl == CURL_RETRY_CODES[i]) {
            return 1;
        }
    }

    return 0;
}

size_t
write_callback_null(
        char   *ptr,
        size_t  size,
        size_t  nmemb,
        void   *userdata)
{
    return nmemb;
}

void
curl_authorization(
        char *token,
        char *authorization)
{
    snprintf(authorization, BUFFER_SIZE, "Authorization: Bearer %s", token);
}

void
curl_byterange(
        char   *byterange,
        size_t  dataoffset,
        size_t  datasize)
{
    snprintf(byterange, BUFFER_SIZE, "Range: bytes=%ld-%ld", dataoffset, dataoffset+datasize-1);
}

void
curl_contentlength(
        size_t  datasize,
        char   *contentlength)
{
    snprintf(contentlength, BUFFER_SIZE, "Content-Length: %lu", (unsigned long)datasize);
}

void
curl_lease(
        char *lease,
        char *leaseid)
{
    snprintf(lease, BUFFER_SIZE, "x-ms-lease-id: %s", leaseid);
}

struct DataStruct {
    char *data;
    size_t datasize;
    size_t currentsize;
};

size_t
write_callback_readdata(
        char   *ptr,
        size_t  size,
        size_t  nmemb,
        void   *datavoid)
{
    struct DataStruct *datastruct = (struct DataStruct*)datavoid;
    size_t n = size*nmemb;
    size_t newsize = datastruct->currentsize + n;
    if (newsize > datastruct->datasize) {
        printf("error: read too many bytes, %d in %s\n", __LINE__, __FILE__);
        return 0;
    }
    memcpy(datastruct->data+datastruct->currentsize, ptr, n);
    datastruct->currentsize = newsize;
    return n;
}

struct ResponseCodes
curl_writebytes_block(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *leaseid,
        char   *blockid,
        char   *data,
        size_t  datasize,
        int     verbose)
{
    char authorization[BUFFER_SIZE];
    curl_authorization(token, authorization);
    char contentlength[BUFFER_SIZE];
    curl_contentlength(datasize, contentlength);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, API_HEADER);
    headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
    headers = curl_slist_append(headers, contentlength);
    headers = curl_slist_append(headers, authorization);

    if (strlen(leaseid) > 0) {
        char lease[BUFFER_SIZE];
        curl_lease(lease, leaseid);
        headers = curl_slist_append(headers, lease);
    }

    CURL *curlhandle = curl_easy_init();

    char url[BUFFER_SIZE];
    snprintf(
        url,
        BUFFER_SIZE,
        "https://%s.blob.core.windows.net/%s/%s?comp=block&blockid=%s",
        storageaccount,
        containername,
        blobname,
        blockid);

    curl_easy_setopt(curlhandle, CURLOPT_URL, url);
    curl_easy_setopt(curlhandle, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curlhandle, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDSIZE, datasize);
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(curlhandle, CURLOPT_SSL_VERIFYPEER, 0); /* TODO */
    curl_easy_setopt(curlhandle, CURLOPT_VERBOSE, verbose);
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, write_callback_null);

    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curlhandle, CURLOPT_ERRORBUFFER, errbuf);

    long responsecode_http = 200;
    CURLcode responsecode_curl = curl_easy_perform(curlhandle);
    curl_easy_getinfo(curlhandle, CURLINFO_RESPONSE_CODE, &responsecode_http);

    if ( (responsecode_curl != CURLE_OK || responsecode_http >= 300) && verbose > 0) {
        printf("Warning, curl response=%s, http response code=%ld\n", errbuf, responsecode_http);
    }

    curl_easy_cleanup(curlhandle);
    curl_slist_free_all(headers);

    struct ResponseCodes responsecodes;
    responsecodes.http = responsecode_http;
    responsecodes.curl = (long)responsecode_curl;

    return responsecodes;
}

struct ResponseCodes
curl_writebytes_block_retry(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *leaseid,
        char   *blockid,
        char   *data,
        size_t  datasize,
        int     nretry,
        int     verbose)
{
    int iretry;
    struct ResponseCodes responsecodes;
    for (iretry = 0; iretry < nretry; iretry++) {
        responsecodes = curl_writebytes_block(token, storageaccount, containername, blobname, leaseid, blockid, data, datasize, verbose);
        if (isrestretrycode(responsecodes) == 0) {
            break;
        }
        if (verbose > 0) {
            printf("Warning, bad write, retrying, %d/%d, http_responsecode=%ld, curl_responsecode=%ld.\n", iretry+1, nretry, responsecodes.http, responsecodes.curl);
        }
        if (exponential_backoff(iretry) != 0) {
            printf("Warning, unable to sleep in exponential backoff due to failed nanosleep call.\n");
            break;
        }
    }
    return responsecodes;
}

struct ResponseCodes
curl_writebytes_block_retry_threaded(
        char    *token,
        char    *storageaccount,
        char    *containername,
        char    *blobname,
        char    *leaseid,
        char   **blockids,
        char    *data,
        size_t  datasize,
        int     nthreads,
        int     nblocks,
        int     nretry,
        int     verbose)
{
    size_t block_datasize = datasize/nblocks;
    size_t block_dataremainder = datasize%nblocks;

    int threadid;
    long thread_responsecode_http[nthreads];
    long thread_responsecode_curl[nthreads];
    for (threadid = 0; threadid < nthreads; threadid++) {
        thread_responsecode_http[threadid] = 200;
        thread_responsecode_curl[threadid] = (long)CURLE_OK;
    }

#pragma omp parallel num_threads(nthreads) default(shared)
{
    int threadid = omp_get_thread_num();
    int iblock;
#pragma omp for
    for (iblock = 0; iblock < nblocks; iblock++) {
        size_t block_firstbyte = iblock*block_datasize;
        size_t _block_datasize = block_datasize;
        if (iblock < block_dataremainder) {
            block_firstbyte += iblock;
            _block_datasize += 1;
        } else {
            block_firstbyte += block_dataremainder;
        }

        struct ResponseCodes responsecodes = curl_writebytes_block_retry(token, storageaccount, containername, blobname, leaseid, blockids[iblock], data+block_firstbyte, _block_datasize, nretry, verbose);
        thread_responsecode_http[threadid] = MAX(responsecodes.http, thread_responsecode_http[threadid]);
        thread_responsecode_curl[threadid] = MAX(responsecodes.curl, thread_responsecode_curl[threadid]);
    }
} // end #pragma omp

    struct ResponseCodes responsecodes;
    responsecodes.http = (long)200;
    responsecodes.curl = (long)CURLE_OK;
    for (threadid = 0; threadid < nthreads; threadid++) {
        responsecodes.http = MAX(responsecodes.http, thread_responsecode_http[threadid]);
        responsecodes.curl = MAX(responsecodes.curl, thread_responsecode_curl[threadid]);
    }
    return responsecodes;
}

struct ResponseCodes
curl_readbytes(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *data,
        size_t  dataoffset,
        size_t  datasize,
        int     verbose)
{
    char authorization[BUFFER_SIZE];
    curl_authorization(token, authorization);

    char byterange[BUFFER_SIZE];
    curl_byterange(byterange, dataoffset, datasize);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, authorization);
    headers = curl_slist_append(headers, API_HEADER);
    headers = curl_slist_append(headers, byterange);

    struct DataStruct datastruct;
    datastruct.data = data;
    datastruct.datasize = datasize;
    datastruct.currentsize = 0;

    CURL *curlhandle = curl_easy_init();

    char url[BUFFER_SIZE];
    snprintf(
        url,
        BUFFER_SIZE,
        "https://%s.blob.core.windows.net/%s/%s",
        storageaccount,
        containername,
        blobname);

    curl_easy_setopt(curlhandle, CURLOPT_URL, url);
    curl_easy_setopt(curlhandle, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curlhandle, CURLOPT_SSL_VERIFYPEER, 0); /* TODO */
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_VERBOSE, verbose);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, write_callback_readdata);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEDATA, (void*)&datastruct);

    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curlhandle, CURLOPT_ERRORBUFFER, errbuf);

    long responsecode_http = 200;
    CURLcode responsecode_curl = curl_easy_perform(curlhandle);
    curl_easy_getinfo(curlhandle, CURLINFO_RESPONSE_CODE, &responsecode_http);

    if ( (responsecode_curl != CURLE_OK || responsecode_http >= 300) && verbose > 0) {
        printf("Error, bad read, http response code=%ld, curl response=%s\n", responsecode_http, errbuf);
    }

    curl_easy_cleanup(curlhandle);
    curl_slist_free_all(headers);

    struct ResponseCodes responsecodes;
    responsecodes.http = responsecode_http;
    responsecodes.curl = (long)responsecode_curl;

    return responsecodes;
}

struct ResponseCodes
curl_readbytes_retry(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *data,
        size_t  dataoffset,
        size_t  datasize,
        int     nretry,
        int     verbose)
{
    struct ResponseCodes responsecodes;
    int iretry;
    for (iretry = 0; iretry < nretry; iretry++) {
        responsecodes = curl_readbytes(token, storageaccount, containername, blobname, data, dataoffset, datasize, verbose);
        if (isrestretrycode(responsecodes) == 0) {
            break;
        }
        if (verbose > 0) {
            printf("Warning, bad read, retrying, %d/%d, http responsecode=%ld, curl responsecode=%ld.\n", iretry+1, nretry, responsecodes.http, responsecodes.curl);
        }
        if (exponential_backoff(iretry) != 0) {
            printf("Warning, exponential backoff failed\n");
            break;
        }
    }
    return responsecodes;
}

struct ResponseCodes
curl_readbytes_retry_threaded(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *data,
        size_t  dataoffset,
        size_t  datasize,
        int     nthreads,
        int     nretry,
        int     verbose)
{
    size_t thread_datasize = datasize/nthreads;
    size_t thread_dataremainder = datasize%nthreads;

    long thread_responsecode_http[nthreads];
    long thread_responsecode_curl[nthreads];

#pragma omp parallel num_threads(nthreads)
{
    int threadid = omp_get_thread_num();
    size_t thread_firstbyte = threadid*thread_datasize;
    size_t _thread_datasize = thread_datasize;
    if (threadid < thread_dataremainder) {
        thread_firstbyte += threadid;
        _thread_datasize += 1;
    } else {
        thread_firstbyte += thread_dataremainder;
    }

    struct ResponseCodes responsecodes = curl_readbytes_retry(token, storageaccount, containername, blobname, data+thread_firstbyte, dataoffset+thread_firstbyte, _thread_datasize, nretry, verbose);
    thread_responsecode_http[threadid] = responsecodes.http;
    thread_responsecode_curl[threadid] = responsecodes.curl;
} /* end pragma omp */
    long responsecode_http = 200;
    long responsecode_curl = (long)CURLE_OK;
    int threadid;
    for (threadid = 0; threadid < nthreads; threadid++) {
        responsecode_http = MAX(responsecode_http, thread_responsecode_http[threadid]);
        responsecode_curl = MAX(responsecode_curl, thread_responsecode_curl[threadid]);
    }
    struct ResponseCodes responsecodes;
    responsecodes.http = responsecode_http;
    responsecodes.curl = responsecode_curl;

    return responsecodes;
}
