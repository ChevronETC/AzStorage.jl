#include "AzStorage.h"

int N_HTTP_RETRY_CODES = 0;
int N_CURL_RETRY_CODES = 0;
long *HTTP_RETRY_CODES = NULL;
long *CURL_RETRY_CODES = NULL;
char API_HEADER[API_HEADER_BUFFER_SIZE];

char*
api_header() {
    return API_HEADER;
}

int
exponential_backoff(
        int i,
        int retry_after)
{
    double sleeptime_seconds,sleeptime_nanoseconds;
    if (retry_after > 0) {
        sleeptime_seconds = retry_after + 1.0*rand()/RAND_MAX;
        sleeptime_nanoseconds = 0.0;
    } else {
        double sleeptime = MIN(pow(2.0, (double)i), MAXIMUM_BACKOFF) + 1.0*rand()/RAND_MAX;
        sleeptime_seconds = floor(sleeptime);
        sleeptime_nanoseconds = (long)((sleeptime - sleeptime_seconds) * 1000000000.0);
    }

    struct timespec ts_sleeptime, ts_remainingtime;

    ts_sleeptime.tv_sec = (long)sleeptime_seconds;
    ts_sleeptime.tv_nsec = (long)sleeptime_nanoseconds;

    return nanosleep(&ts_sleeptime, &ts_remainingtime);
}

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

size_t
callback_retry_after_header(
        char   *ptr,
        size_t  size,
        size_t  nmemb,
        void   *datavoid)
{
    struct HeaderStruct *data = (struct HeaderStruct*)datavoid;

    if (strncmp("Retry-After:", ptr, 12) == 0) {
        int n = sscanf(ptr, "Retry-After:%d", &(data->retry_after));
        if (n != 1) {
            printf("Warning: unable to parse Retry-After header, setting Retry-After to 0");
            data->retry_after = 0;
        }
    }

    return size*nmemb;
}

size_t
token_callback_readdata(
        char   *ptr,
        size_t  size,
        size_t  nmemb,
        void   *datavoid)
{
    struct DataStruct *datastruct = (struct DataStruct*)datavoid;
    size_t n = size*nmemb;
    size_t newsize = datastruct->currentsize + n;

    if (datastruct->currentsize == 0) {
        datastruct->data = (char*) malloc(newsize);
    } else {
        datastruct->data = (char*) realloc(datastruct->data, newsize);
    }

    memcpy(datastruct->data+datastruct->currentsize, ptr, n);
    datastruct->currentsize = newsize;

    return n;
}

void
get_next_quoted_string(
        char *data,
        char *value)
{
    int i1 = -1;
    int i2 = -1;
    int i;
    for (i = 0; i < strlen(data); i++) {
        if (data[i] == '"') {
            if (i1 < 0) {
                i1 = i + 1;
            } else if (i2 < 0) {
                i2 = i - 1;
            } else {
                break;
            }
        }
    }
    strncpy(value, data+i1, i2-i1+1);
    value[i2-i1+1] = '\0';
}

void
update_tokens_from_refresh_token(
        char          *data,
        char          *bearer_token,
        char          *refresh_token,
        unsigned long *expiry)
{
    char *_data = data;
    char expiry_string[BUFFER_SIZE];
    int counter = 0;
    while (counter < strlen(data)) {
        if (strncmp(_data, "\"access_token\"", 14) == 0) {
            counter += 14;
            _data += 14;
            get_next_quoted_string(_data, bearer_token);
        } else if (strncmp(_data, "\"refresh_token\"", 15) == 0) {
            counter += 15;
            _data += 15;
            get_next_quoted_string(_data, refresh_token);
        } else if (strncmp(_data, "\"expires_on\"", 12) == 0) {
            counter += 12;
            _data += 12;
            get_next_quoted_string(_data, expiry_string);
            sscanf(expiry_string, "%lu", expiry);
        } else {
            counter += 1;
            _data += 1;
        }
    }
}

struct ProgressStruct {
    unsigned long start_time;
    unsigned long read_timeout;
    curl_off_t dlprev;
    curl_off_t ulprev;
};

int
progress_callback(
        void *_progressstruct,
        curl_off_t dltotal,
        curl_off_t dlnow,
        curl_off_t ultotal,
        curl_off_t ulnow)
{
    struct ProgressStruct *progressstruct = (struct ProgressStruct*)_progressstruct;
    long int elapsed_time = (unsigned long)time(NULL) - progressstruct->start_time;

    curl_off_t dldelta = dlnow - progressstruct->dlprev;
    curl_off_t uldelta = ulnow - progressstruct->ulprev;

    if ( (dldelta == 0 && elapsed_time >= progressstruct->read_timeout) || (uldelta == 0 && elapsed_time >= progressstruct->read_timeout) ) {
        return 1;
    }
    if (dldelta > 0 || uldelta > 0) {
        progressstruct->start_time = (unsigned long)time(NULL);
        progressstruct->dlprev = dlnow;
        progressstruct->ulprev = ulnow;
    }

    return 0;
}

struct ResponseCodes
curl_refresh_tokens_from_refresh_token(
        char          *bearer_token,
        char          *refresh_token,
        unsigned long *expiry,
        char          *scope,
        char          *resource,
        char          *clientid,
        char          *tenant,
        int            verbose,
        long           connect_timeout,
        long           read_timeout)
{
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");

    char body[BUFFER_SIZE];
    snprintf(
        body,
        BUFFER_SIZE,
        "client_id=%s&refresh_token=%s&grant_type=refresh_token&scope=%s&resource=%s",
        clientid,
        refresh_token,
        scope,
        resource);

    char url[BUFFER_SIZE];
    snprintf(
        url,
        BUFFER_SIZE,
        "https://login.microsoft.com/%s/oauth2/token",
        tenant);

    struct DataStruct datastruct;

    datastruct.currentsize = 0;
    datastruct.datasize = 0;
    datastruct.data = NULL;

    struct HeaderStruct headerstruct;

    headerstruct.retry_after = 0;

    struct ProgressStruct progressstruct;
    progressstruct.start_time = (unsigned long)time(NULL);
    progressstruct.read_timeout = read_timeout;
    progressstruct.dlprev = 0;
    progressstruct.ulprev = 0;

    CURL *curlhandle = curl_easy_init();

    curl_easy_setopt(curlhandle, CURLOPT_URL, url);
    curl_easy_setopt(curlhandle, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curlhandle, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDSIZE, strlen(body));
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curlhandle, CURLOPT_SSL_VERIFYPEER, 0); /* TODO */
    curl_easy_setopt(curlhandle, CURLOPT_VERBOSE, verbose);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, token_callback_readdata);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEDATA, (void*)&datastruct);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERFUNCTION, callback_retry_after_header);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERDATA, &headerstruct);
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    curl_easy_setopt(curlhandle, CURLOPT_NOPROGRESS, 0);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFODATA, &progressstruct);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFOFUNCTION, progress_callback);

    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curlhandle, CURLOPT_ERRORBUFFER, errbuf);

    long responsecode_http = 200;
    CURLcode responsecode_curl = curl_easy_perform(curlhandle);
    curl_easy_getinfo(curlhandle, CURLINFO_RESPONSE_CODE, &responsecode_http);

    if ( (responsecode_curl != CURLE_OK || responsecode_http >= 300) && verbose > 0) {
        printf("Warning, curl response=%s, http response code=%ld\n", errbuf, responsecode_http);
    } else {
        update_tokens_from_refresh_token(datastruct.data, bearer_token, refresh_token, expiry);
    }

    if (datastruct.data != NULL) {
        free(datastruct.data);
        datastruct.data = NULL;
    }

    curl_easy_cleanup(curlhandle);
    curl_slist_free_all(headers);

    struct ResponseCodes responsecodes;
    responsecodes.http = responsecode_http;
    responsecodes.curl = (long)responsecode_curl;
    responsecodes.retry_after = headerstruct.retry_after;

    return responsecodes;
}

void
update_tokens_from_client_secret(
        char          *data,
        char          *bearer_token,
        unsigned long *expiry)
{
    char *_data = data;
    char expiry_string[BUFFER_SIZE];
    int counter = 0;
    while (counter < strlen(data)) {
        if (strncmp(_data, "\"access_token\"", 14) == 0) {
            counter += 14;
            _data += 14;
            get_next_quoted_string(_data, bearer_token);
        } else if (strncmp(_data, "\"expires_on\"", 12) == 0) {
            counter += 12;
            _data += 12;
            get_next_quoted_string(_data, expiry_string);
            sscanf(expiry_string, "%lu", expiry);
        } else {
            counter += 1;
            _data += 1;
        }
    }
}

struct ResponseCodes
curl_refresh_tokens_from_client_credentials(
        char          *bearer_token,
        unsigned long *expiry,
        char          *resource,
        char          *clientid,
        char          *client_secret,
        char          *tenant,
        int            verbose,
        long           connect_timeout,
        long           read_timeout)
{
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");

    CURL *curlhandle = curl_easy_init();

    char *_client_secret = curl_easy_escape(curlhandle, client_secret, strlen(client_secret));
    char *_resource = curl_easy_escape(curlhandle, resource, strlen(resource));

    char body[BUFFER_SIZE];
    snprintf(
        body,
        BUFFER_SIZE,
        "grant_type=client_credentials&client_id=%s&client_secret=%s&resource=%s",
        clientid,
        _client_secret,
        _resource);

    char url[BUFFER_SIZE];
    snprintf(
        url,
        BUFFER_SIZE,
        "https://login.microsoft.com/%s/oauth2/token",
        tenant);

    struct DataStruct datastruct;

    datastruct.currentsize = 0;
    datastruct.datasize = 0;
    datastruct.data = NULL;

    struct HeaderStruct headerstruct;

    headerstruct.retry_after = 0;

    struct ProgressStruct progressstruct;
    progressstruct.start_time = (unsigned long)time(NULL);
    progressstruct.read_timeout = read_timeout;
    progressstruct.dlprev = 0;
    progressstruct.ulprev = 0;

    curl_easy_setopt(curlhandle, CURLOPT_URL, url);
    curl_easy_setopt(curlhandle, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curlhandle, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDSIZE, strlen(body));
    curl_easy_setopt(curlhandle, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curlhandle, CURLOPT_SSL_VERIFYPEER, 0); /* TODO */
    curl_easy_setopt(curlhandle, CURLOPT_VERBOSE, verbose);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, token_callback_readdata);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEDATA, (void*)&datastruct);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERFUNCTION, callback_retry_after_header);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERDATA, &headerstruct);
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    curl_easy_setopt(curlhandle, CURLOPT_NOPROGRESS, 0);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFODATA, &progressstruct);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFOFUNCTION, progress_callback);

    char errbuf[CURL_ERROR_SIZE];
    curl_easy_setopt(curlhandle, CURLOPT_ERRORBUFFER, errbuf);

    long responsecode_http = 200;
    CURLcode responsecode_curl = curl_easy_perform(curlhandle);
    curl_easy_getinfo(curlhandle, CURLINFO_RESPONSE_CODE, &responsecode_http);

    if ( (responsecode_curl != CURLE_OK || responsecode_http >= 300) && verbose > 0 ) {
        printf("Warning, curl response=%s, http response code=%ld\n", errbuf, responsecode_http);
    } else {
        update_tokens_from_client_secret(datastruct.data, bearer_token, expiry);
    }

    curl_free(_client_secret);
    curl_free(_resource);

    struct ResponseCodes responsecodes;

    responsecodes.curl = responsecode_curl;
    responsecodes.http = responsecode_http;
    responsecodes.retry_after = headerstruct.retry_after;

    return responsecodes;
}

struct ResponseCodes
curl_refresh_tokens(
        char          *bearer_token,
        char          *refresh_token,
        unsigned long *expiry,
        char          *scope,
        char          *resource,
        char          *clientid,
        char          *client_secret,
        char          *tenant,
        int            verbose,
        long           connect_timeout,
        long           read_timeout)
{
    unsigned long current_time = (unsigned long) time(NULL);
    struct ResponseCodes responsecodes;
    if (current_time < (*expiry - 600)) { /* 10 minute grace period */
        responsecodes.http = 200;
        responsecodes.curl = (long)CURLE_OK;
        return responsecodes;
    }

    if (refresh_token == NULL && client_secret != NULL) {
        responsecodes = curl_refresh_tokens_from_client_credentials(bearer_token, expiry, resource, clientid, client_secret, tenant, verbose, connect_timeout, read_timeout);
    } else if (refresh_token != NULL) {
        responsecodes = curl_refresh_tokens_from_refresh_token(bearer_token, refresh_token, expiry, scope, resource, clientid, tenant, verbose, connect_timeout, read_timeout);
    } else {
        printf("Unable to refresh tokens without either a refresh token or a client secret");
        responsecodes.curl = 1000;
        responsecodes.http = 1000;
        responsecodes.retry_after = 0;
    }

    return responsecodes;
}

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
        int            verbose,
        long           connect_timeout,
        long           read_timeout)
{
    int iretry;
    struct ResponseCodes responsecodes;
    for (iretry = 0; iretry < nretry; iretry++) {
        responsecodes = curl_refresh_tokens(bearer_token, refresh_token, expiry, scope, resource, clientid, client_secret, tenant, verbose, connect_timeout, read_timeout);
        if (isrestretrycode(responsecodes) == 0) {
            break;
        }
        if (verbose > 0) {
            printf("Warning, bad token refresh, retrying, %d/%d, http_responsecode=%ld, curl_responsecode=%ld.\n", iretry+1, nretry, responsecodes.http, responsecodes.curl);
        }
        if (exponential_backoff(iretry, responsecodes.retry_after) != 0) {
            printf("Warning, unable to sleep in exponential backoff due to failed nanosleep call.\n");
            break;
        }
    }
    return responsecodes;
}

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
        char   *blockid,
        char   *data,
        size_t  datasize,
        int     verbose,
        long    connect_timeout,
        long    read_timeout)
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

    struct HeaderStruct header_data;
    header_data.retry_after = 0;

    struct ProgressStruct progressstruct;
    progressstruct.start_time = (unsigned long)time(NULL);
    progressstruct.read_timeout = read_timeout;
    progressstruct.dlprev = 0;
    progressstruct.ulprev = 0;

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
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, write_callback_null);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERFUNCTION, callback_retry_after_header);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERDATA, &header_data);
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    curl_easy_setopt(curlhandle, CURLOPT_NOPROGRESS, 0);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFODATA, &progressstruct);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFOFUNCTION, progress_callback);

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
    responsecodes.retry_after = header_data.retry_after;

    return responsecodes;
}

struct ResponseCodes
curl_writebytes_block_retry(
        char   *token,
        char   *storageaccount,
        char   *containername,
        char   *blobname,
        char   *blockid,
        char   *data,
        size_t  datasize,
        int     nretry,
        int     verbose,
        long    connect_timeout,
        long    read_timeout)
{
    int iretry;
    struct ResponseCodes responsecodes;
    for (iretry = 0; iretry < nretry; iretry++) {
        responsecodes = curl_writebytes_block(token, storageaccount, containername, blobname, blockid, data, datasize, verbose, connect_timeout, read_timeout);
        if (isrestretrycode(responsecodes) == 0) {
            break;
        }
        if (verbose > 0) {
            printf("Warning, bad write, retrying, %d/%d, http_responsecode=%ld, curl_responsecode=%ld.\n", iretry+1, nretry, responsecodes.http, responsecodes.curl);
        }
        if (exponential_backoff(iretry, responsecodes.retry_after) != 0) {
            printf("Warning, unable to sleep in exponential backoff.\n");
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
        char   **blockids,
        char    *data,
        size_t   datasize,
        int      nthreads,
        int      nblocks,
        int      nretry,
        int      verbose,
        long     connect_timeout,
        long     read_timeout)
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

        struct ResponseCodes responsecodes = curl_writebytes_block_retry(token, storageaccount, containername, blobname, blockids[iblock], data+block_firstbyte, _block_datasize, nretry, verbose, connect_timeout, read_timeout);
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
        int     verbose,
        long    connect_timeout,
        long    read_timeout)
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

    struct HeaderStruct header_data;
    header_data.retry_after = 0;

    struct ProgressStruct progressstruct;
    progressstruct.start_time = (unsigned long)time(NULL);
    progressstruct.read_timeout = read_timeout;
    progressstruct.dlprev = 0;
    progressstruct.ulprev = 0;

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
    curl_easy_setopt(curlhandle, CURLOPT_VERBOSE, verbose);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEFUNCTION, write_callback_readdata);
    curl_easy_setopt(curlhandle, CURLOPT_WRITEDATA, (void*)&datastruct);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERFUNCTION, callback_retry_after_header);
    curl_easy_setopt(curlhandle, CURLOPT_HEADERDATA, &header_data);
    curl_easy_setopt(curlhandle, CURLOPT_TIMEOUT, CURLE_TIMEOUT);
    curl_easy_setopt(curlhandle, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    curl_easy_setopt(curlhandle, CURLOPT_NOPROGRESS, 0);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFODATA, &progressstruct);
    curl_easy_setopt(curlhandle, CURLOPT_XFERINFOFUNCTION, progress_callback);

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
    responsecodes.retry_after = header_data.retry_after;

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
        int     verbose,
        long    connect_timeout,
        long    read_timeout)
{
    struct ResponseCodes responsecodes;
    int iretry;
    for (iretry = 0; iretry < nretry; iretry++) {
        responsecodes = curl_readbytes(token, storageaccount, containername, blobname, data, dataoffset, datasize, verbose, connect_timeout, read_timeout);
        if (isrestretrycode(responsecodes) == 0) {
            break;
        }
        if (verbose > 0) {
            printf("Warning, bad read, retrying, %d/%d, http responsecode=%ld, curl responsecode=%ld.\n", iretry+1, nretry, responsecodes.http, responsecodes.curl);
        }
        if (exponential_backoff(iretry, responsecodes.retry_after) != 0) {
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
        int     verbose,
        long    connect_timeout,
        long    read_timeout)
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

    struct ResponseCodes responsecodes = curl_readbytes_retry(token, storageaccount, containername, blobname, data+thread_firstbyte, dataoffset+thread_firstbyte, _thread_datasize, nretry, verbose, connect_timeout, read_timeout);
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
