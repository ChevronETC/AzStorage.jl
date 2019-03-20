# Example

Here we show basic usage where we 1) create a container, 2) write a blob to the container, 3) list the contents of the container, 4) read the blob that was previously created, and 5) delete the container and its contents.

```julia
using Pkg
Pkg.add("AzSessions")
Pkg.add("AzStorage")

using AzSessions, AzStorage

# here we use client credentials, but auth-code-flow and device-code flow (etc.) are also available.
# see the AzSessions.jl package for more details on authentication in Azure.
session = AzSession(;protocal=AzClientCredentials, client_id="myclientid", client_secret="verysecret", resource="https://storage.azure.com/")

# create a handle to an Azure container in an existing storage account
container = AzContainer("foo"; storageaccount="mystorageaccount", session=session)

# create the container
mkpath(container)

# write a blob to the container
write(container, "myblob.bin", rand(10))

# list the blobs in the container
readdir(container)

# read the contents of the blob
x = read!(container, "myblob.bin", Vector{Float64}(undef, 10))

# remove the container, and its contents
rm(x)
```

In addition, we can represent blob's, providing an API that is similar to handling POSIX files.

```julia
# create a handle to a blob in a container
io = open(AzContainer("foo"; storageaccount="mystorageaccount"), "myblob.bin")
io = joinpath(AzContainer("foo"; storageaccount="mystorageaccount"), "myblob.bin") # this is equivalent to the previous line.

# write to the blob
write(io, rand(10))

# read the blob
x = read!(io, zeros(10))

# check that the blob exists
isfile(x)

# remove the blob
rm(x)
```
