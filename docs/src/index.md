# AzStorage
AzStorage is a Julia API for Azure storage.  AzStorage provides methods for interacting
with Azure containers and blobs.  In order to obtain reasonable through-put, I/O is threaded via
OpenMP.  In the case of writing, Azure block-blobs are used to help organize the threaded I/O.
