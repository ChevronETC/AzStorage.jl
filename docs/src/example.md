# Example

Here we show basic usage where we 1) create a container, 2) write a blob to the container, 3) list the contents of the container, 4) read the blob that was previously created, 5) illustrate serialization, and 6) delete the container and its contents.

```julia
using Pkg
Pkg.add("AzSessions")
Pkg.add("AzStorage")

using AzSessions, AzStorage, Serialization

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

# serialize and write structured data to the container.
# here, we illustrate with a named tuple.
serialize(container, "myblob.bin", (a=rand(10), b=rand(10)))

# read and deserialze data from the container
x = deserialize(container, "myblob.bin")

# copy a blob to a local file
cp(container, "myblob.bin", "mylocalfile.bin")

# copy a local file to a blob
cp("mylocalfile.bin", container, "myblob.bin")

# copy from a blob to another blob
cp(container, "myblob.bin", container, "mycopyblob.bin")

# remove the container, and its contents
rm(container)
```

In addition, we can represent blob's, providing an API that is similar to handling POSIX files.

```julia
# create a handle, io,  to a blob, "myblob.bin", in a container, "foo", in storage account "mystorageaccount"
io = open(AzContainer("foo"; storageaccount="mystorageaccount", session), "myblob.bin")
io = joinpath(AzContainer("foo"; storageaccount="mystorageaccount", session), "myblob.bin") # this is equivalent to the previous line.

# write to the blob
write(io, rand(10))

# read the blob
x = read!(io, zeros(10))

# check that the blob exists
isfile(io)

# serialize and write structured data
# here, we illustrate with a named tuple
serialize(io, (a=rand(10),b=rand(10)))
x = deserialize(io)

write(io, rand(10))

# copy a blob, io, to a local file, mylocalfile.bin
cp(io, "mylocalfile.bin")

# copy a local file, mylocalfile.bin, to a blob, io
cp("mylocalfile.bin", io)

# copy from a blob to another blob
io2 = open(AzContainer("foo"; storageaccount="mystorageaccount", session), "mycopyblob.bin")
cp(io, io2)

# remove the blob
rm(io)
```
