module AzStorage

using AbstractStorage, AzSessions, AzStorage_jll, Base64, Dates, DelimitedFiles, HTTP, LightXML, Serialization, Sockets

# https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
const RETRYABLE_HTTP_ERRORS = [
    500, # Internal server error
    503] # Service unavailable

# https://curl.haxx.se/libcurl/c/libcurl-errors.html
const RETRYABLE_CURL_ERRORS = [
    6,  # Couldn't resolve host. The given remote host was not resolved.
    7,  # Failed to connect() to host or proxy.
    28, # Connection timed out.
    35, # SSL handshake failure.
    55, # Failed sendingnetworkdata.
    56] # Failure with received network data.

# https://docs.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services
const API_VERSION = "2021-08-06"

function __init__()
    ccall((:curl_init, libAzStorage), Cvoid, (Cint, Cint, Ptr{Clong}, Ptr{Clong}, Cstring),
        length(RETRYABLE_HTTP_ERRORS), length(RETRYABLE_CURL_ERRORS), RETRYABLE_HTTP_ERRORS, RETRYABLE_CURL_ERRORS, API_VERSION)
end

mutable struct AzContainer{A<:AzSessionAbstract} <: Container
    storageaccount::String
    containername::String
    prefix::String
    session::A
    nthreads::Int
    nretry::Int
    verbose::Int
end

function Base.copy(container::AzContainer)
    AzContainer(
        container.storageaccount,
        container.containername,
        container.prefix,
        copy(container.session),
        container.nthreads,
        container.nretry,
        container.verbose)
end

struct AzObject
    container::AzContainer
    name::String
end

"""
    open(container, blobname) -> AzObject

Create a handle to an Azure blob with the name `blobname::String` in the
Azure storage container: `container::AzContainer`.

# Example:
```julia
io = open(AzContainer("mycontainer"; storageaccount="myaccount"), "foo.bin")
write(io, rand(10))
```
"""
function Base.open(container::AzContainer, name)
    mkpath(container)
    AzObject(container, string(name))
end

"""
    joinpath(container, blobname) -> AzObject

Create a handle to an Azure blob with the name `blobname::String` in the
Azure storage container: `container::AzContainer`.

# Example:
```julia
io = joinpath(AzContainer("mycontainer"; storageaccount="myaccount"), "foo.bin")
write(io, rand(10))
```
"""
Base.joinpath(container::AzContainer, name...) = open(container, join(name, '/'))

Base.close(object::AzObject) = nothing

const __OAUTH_SCOPE = "offline_access+openid+https://storage.azure.com/user_impersonation"

function windows_one_thread(nthreads)
    if Sys.iswindows() && nthreads != 1
        @warn "On Windows, AzStorage is limited to a single thread."
    end
    Sys.iswindows() ? 1 : nthreads
end

"""
    container = AzContainer("containername"; storageaccount="myacccount", kwargs...)

`container` is a handle to a new or existing Azure container in the `myaccount` sorage account.
The storage account must already exist.

# Additional keyword arguments
* `session=AzSession(;lazy=true,scope=$__OAUTH_SCOPE)` user credentials (see AzSessions.jl package).
* `nthreads=Sys.CPU_THREADS` number of system threads that OpenMP will use to thread I/O.
* `nretry=10` number of retries to the Azure service (when Azure throws a retryable error) before throwing an error.
* `verbose=0` verbosity flag passed to libcurl.

# Notes
The container name can container "/"'s.  If this is the case, then the string preceding the first "/" will
be the container name, and the string that remains will be pre-pended to the blob names.  This allows Azure
to present blobs in a pseudo-directory structure.
"""
function AzContainer(containername::AbstractString; storageaccount, session=AzSession(;lazy=true, scope=__OAUTH_SCOPE), nthreads=Sys.CPU_THREADS, nretry=10, verbose=0, prefix="")
    name = split(containername, '/')
    _containername = name[1]
    prefix *= lstrip('/'*join(name[2:end], '/'), '/')
    AzContainer(String(storageaccount), String(_containername), String(prefix), session, windows_one_thread(nthreads), nretry, verbose)
end

function AbstractStorage.Container(::Type{<:AzContainer}, d::Dict, session=AzSession(;lazy=true, scope=__OAUTH_SCOPE); nthreads = Sys.CPU_THREADS, nretry=10, verbose=0)
    AzContainer(
        d["storageaccount"],
        d["containername"],
        d["prefix"],
        session,
        windows_one_thread(get(d, "nthreads", nthreads)),
        get(d, "nretry", nretry),
        get(d, "verbose", verbose))
end

Base.:(==)(x::AzContainer, y::AzContainer) = x.storageaccount == y.storageaccount && x.containername == y.containername && x.prefix == y.prefix

struct ResponseCodes
    http::Int64
    curl::Int64
end

function isretryable(e::HTTP.StatusError)
    e.status ∈ RETRYABLE_HTTP_ERRORS && (return true)
    false
end
isretryable(e::Base.IOError) = true
isretryable(e::HTTP.Exceptions.ConnectError) = true
isretryable(e::HTTP.Exceptions.HTTPError) = true
isretryable(e::HTTP.Exceptions.RequestError) = true
isretryable(e::HTTP.Exceptions.TimeoutError) = true
isretryable(e::Base.EOFError) = true
isretryable(e::Sockets.DNSError) = Base.uverrorname(e.code) == "EAI_NONAME" ? false : true
isretryable(e) = false

function retrywarn(i, s, e)
    @debug "retry $i, sleeping for $s seconds, e=$e"
end

macro retry(retries, ex::Expr)
    quote
        local r
        for i = 1:$(esc(retries))
            try
                r = $(esc(ex))
                break
            catch e
                (i <= $(esc(retries)) && isretryable(e)) || rethrow(e)
                maximum_backoff = 256
                s = min(2.0^(i-1), maximum_backoff) + rand()
                retrywarn(i, s, e)
                sleep(s)
            end
        end
        r
    end
end

"""
    mkpath(container)

create an Azure container from the handle `container::AzContainer`.  If the container
already exists, then this is a no-op.
"""
function Base.mkpath(c::AzContainer)
    if !iscontainer(c)
        @retry c.nretry HTTP.request(
            "PUT",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)?restype=container",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
    end
    nothing
end

# 4000 MiB, there are 2^20 bytes in one MiB
const _MINBYTES_PER_BLOCK = 32 * 2^20
const _MAXBYTES_PER_BLOCK = 4_000 * 2^20
const _MAXBLOCKS_PER_BLOB = 50_000

nblocks_error() = error("data is too large for a block-blob")
function nblocks(nthreads::Integer, nbytes::Integer)
    nblocks = ceil(Int, nbytes/_MAXBYTES_PER_BLOCK + eps(Float64))
    if nblocks < nthreads
        nblocks = clamp(ceil(Int, nbytes/_MINBYTES_PER_BLOCK + eps(Float64)), 1, nthreads)
    end
    nblocks > _MAXBLOCKS_PER_BLOB && nblocks_error()
    nblocks
end

_normpath(s) = Sys.iswindows() ? replace(normpath(s), "\\"=>"/") : normpath(s)

addprefix(c::AzContainer, o) = c.prefix == "" ? o : _normpath("$(c.prefix)/$o")

function writebytes(c::AzContainer, o::AbstractString, data::DenseArray{UInt8}; contenttype="application/octet-stream")
    function writebytes_blob(c, o, data, contenttype)
        @retry c.nretry HTTP.request(
            "PUT",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION,
                "Content-Length" => "$(length(data))",
                "Content-Type" => contenttype,
                "x-ms-blob-type" => "BlockBlob"
            ],
            data,
            retry = false,
            verbose = c.verbose)
        nothing
    end

    function putblocklist(c, o, blockids)
        xdoc = XMLDocument()
        xroot = create_root(xdoc, "BlockList")
        for blockid in blockids
            add_text(new_child(xroot, "Uncommitted"), blockid)
        end
        blocklist = string(xdoc)

        @retry c.nretry HTTP.request(
            "PUT",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))?comp=blocklist",
            [
                "x-ms-version" => API_VERSION,
                "Authorization" => "Bearer $(token(c.session))",
                "Content-Type" => "application/octet-stream",
                "Content-Length" => "$(length(blocklist))"
            ],
            blocklist,
            retry = false)
        nothing
    end

    function writebytes_block(c, o, data, _nblocks)
        # heuristic to increase probability that token is valid during the retry logic in AzSessions.c
        t = token(c.session; offset=Minute(30))
        l = ceil(Int, log10(_nblocks))
        blockids = [base64encode(lpad(blockid-1, l, '0')) for blockid in 1:_nblocks]
        _blockids = [HTTP.escapeuri(blockid) for blockid in blockids]
        r = ccall((:curl_writebytes_block_retry_threaded, libAzStorage), ResponseCodes,
            (Cstring, Cstring,          Cstring,         Cstring,        Ptr{Cstring}, Ptr{UInt8}, Csize_t,      Cint,       Cint,     Cint,     Cint),
             t,       c.storageaccount, c.containername, addprefix(c,o), _blockids,    data,       length(data), c.nthreads, _nblocks, c.nretry, c.verbose)
        (r.http >= 300 || r.curl > 0) && error("writebytes_block error: http code $(r.http), curl code $(r.curl)")

        putblocklist(c, o, blockids)
    end

    _nblocks = nblocks(c.nthreads, length(data))
    if _nblocks > 1
        writebytes_block(c, o, data, _nblocks)
    else
        writebytes_blob(c, o, data, contenttype)
    end
    nothing
end

"""
    write(container, "blobname", data::AbstractString; contenttype="text/plain")

Write the string `data` to a blob with name `blobname` in `container::AzContainer`.
Optionally, one can specify the content-type of the blob using the `contenttype` keyword argument.
For example: `content-type="text/plain", `content-type="applicaton/json", etc..
"""
Base.write(c::AzContainer, o::AbstractString, data::AbstractString; contenttype="text/plain") =
    writebytes(c, o, transcode(UInt8, data); contenttype=contenttype)

_iscontiguous(data::DenseArray) = isbitstype(eltype(data))
_iscontiguous(data::SubArray) = isbitstype(eltype(data)) && Base.iscontiguous(data)
_iscontiguous(data::AbstractArray) = false

"""
    write(container, "blobname", data::StridedArray)

Write the array `data` to a blob with the name `blobname` in `container::AzContainer`.
"""
function Base.write(c::AzContainer, o::AbstractString, data::AbstractArray{T}) where {T}
    if _iscontiguous(data)
        writebytes(c, o, unsafe_wrap(Vector{UInt8}, convert(Ptr{UInt8}, pointer(data)), length(data)*sizeof(T), own=false); contenttype="application/octet-stream")
    else
        error("AzStorage: `write` is not supported on non-isbits arrays and/or non-contiguous arrays")
    end
end

Base.write(c::AzContainer, o::AbstractString, data) = error("AzStorage: `write` is only suppoted for DenseArray.")

"""
    write(io::AzObject, data)

write data to `io::AzObject`.

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.bin")
write(io, rand(10))
x = read!(io, zeros(10))
```
"""
Base.write(o::AzObject, data) = write(o.container, o.name, data)

"""
    serialize(container, "blobname", data)

Serialize and write `data` to a blob with the name `blobname` in `container::AzContainer`.


# Example
```
container = AzContainer("mycontainer";storageaccount="mystorageaccount")
serialize(container, "foo.bin", (rand(10),rand(20)))
a,b = deserialize(io)
```
"""
function Serialization.serialize(c::AzContainer, o::AbstractString, data)
    io = IOBuffer(;write=true)
    serialize(io, data)
    writebytes(c, o, take!(io); contenttype="application/octet-stream")
end

"""
    serialize(io::AzObject, data)

Serialize and write data to `io::AzObject`.  See serialize(conainer, blobname, data).
"""
Serialization.serialize(o::AzObject, data) = serialize(o.container, o.name, data)

"""
    touch(container, "blobname")

Create a zero-byte object with name `blobname` in `container::AzContainer`.

# Example
```
container = AzContainer("mycontainer";storageaccount="mystorageaccount")
touch(container, "foo")
```
"""
Base.touch(c::AzContainer, o::AbstractString) = write(c, o, "")

"""
    touch(io::AzObject)

Create a zero-byte object for `io`.  See `touch(container::AzContainer, blobname)`.
"""
Base.touch(o::AzObject) = touch(o.container, o.name)

"""
    writedlm(container, "blobname", data, args...; options...)

Write the array `data` to a delimited blob with the name `blobname` in container `container::AzContainer`
"""
function DelimitedFiles.writedlm(c::AzContainer, o::AbstractString, data::AbstractArray, args...; opts...)
    io = IOBuffer(;write=true)
    writedlm(io, data, args...; opts...)
    write(c, o, String(take!(io)))
end

"""
    writedlm(io:AzObject, data, args...; options...)

write the array `data` to `io::AzObject`

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.txt")
writedlm(io, rand(10,10))
x = readdlm(io)
```
"""
function DelimitedFiles.writedlm(o::AzObject, data::AbstractArray, args...; opts...)
     writedlm(o.container, o.name, data, args...; opts...)
end

"""
    readdlm(container, "blobname", args...; options...)

Read the data in a delimited blob with the name `blobname` in container `container::AzContainer`
"""
function DelimitedFiles.readdlm(c::AzContainer, o::AbstractString, args...; opts...)
    io = IOBuffer(;write=true, read=true)
    write(io, read(c, o, String))
    seekstart(io)
    readdlm(io, args...; opts...)
end

"""
    readdlm(io:AzObject, args...; options...)

return the parsed delimited blob from the io object `io::AzObject`

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.txt")
data = readdlm(io)
```
"""
function DelimitedFiles.readdlm(o::AzObject, args...; opts...)
    readdlm(o.container, o.name, args...; opts...)
end

#this is to resolve an function call ambiguity
function DelimitedFiles.readdlm(o::AzObject, delim::AbstractChar, args...; opts...)
    readdlm(o.container, o.name, delim, args...; opts...)
end

nthreads_effective(nthreads::Integer, nbytes::Integer) = clamp(div(nbytes, _MINBYTES_PER_BLOCK), 1, nthreads)

function readbytes!(c::AzContainer, o::AbstractString, data::DenseArray{UInt8}; offset=0)
    function readbytes_serial!(c, o, data, offset)
        @retry c.nretry HTTP.open(
                "GET",
                "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
                [
                    "Authorization" => "Bearer $(token(c.session))",
                    "x-ms-version" => API_VERSION,
                    "Range" => "bytes=$offset-$(offset+length(data)-1)"
                ];
                retry = false,
                verbose = c.verbose) do io
            read!(io, data)
        end
        nothing
    end

    function readbytes_threaded!(c, o, data, offset, _nthreads)
        # heuristic to increase probability that token is valid during the retry logic in AzSessions.c
        t = token(c.session; offset=Minute(30))
        r = ccall((:curl_readbytes_retry_threaded, libAzStorage), ResponseCodes,
            (Cstring, Cstring,          Cstring,         Cstring,        Ptr{UInt8}, Csize_t, Csize_t,      Cint,      Cint,     Cint),
             t,       c.storageaccount, c.containername, addprefix(c,o), data,       offset,  length(data), _nthreads, c.nretry, c.verbose)
        (r.http >= 300 || r.curl > 0) && error("readbytes_threaded! error: http code $(r.http), curl code $(r.curl)")
        nothing
    end

    _nthreads = nthreads_effective(c.nthreads, length(data))
    if _nthreads > 1
        readbytes_threaded!(c, o, data, offset, _nthreads)
    else
        readbytes_serial!(c, o, data, offset)
    end
    data
end

"""
    read(container, "blobname", String)

returns the contents of the blob "blobname" in `container::AzContainer` as a string.
"""
Base.read(c::AzContainer, o::AbstractString, T::Type{String}) = String(readbytes!(c, o, Vector{UInt8}(undef, filesize(c,o))))

"""
    read!(container, "blobname", data; offset=0)

read from the blob "blobname" in `container::AzContainer` into `data::DenseArray`, and
where `offset` specifies a number of bytes in the blob to skip before reading.  This
method returns `data`.  For example,
```
data = read!(AzContainer("foo";storageaccount="bar"), "baz.bin", Vector{Float32}(undef,10))
```
"""
function Base.read!(c::AzContainer, o::AbstractString, data::AbstractArray{T}; offset=0) where {T}
    if _iscontiguous(data)
        _data = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), length(data)*sizeof(T), own=false)
        readbytes!(c, o, _data; offset=offset*sizeof(T))
    else
        error("AzStorage does not support reading objects of type $T and/or into a non-contiguous array.")
    end
    data
end

"""
    read(object, String)

read a string from `object::AzObject`.

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.txt")
read(io, String)
```
"""
Base.read(o::AzObject, T::Type{String}) = read(o.container, o.name, String)

"""
    read!(object, x; offset=0) -> x

read data from `object::AzObject` into `x::DenseArray`,
and return `x`.  `offset` is an integer that can be
used to specify the first byte in the object to read. 

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.txt")
read!(io, Vector{Float64}(undef, 10))
```
"""
Base.read!(o::AzObject, data; offset=0) = read!(o.container, o.name, data; offset=offset)

"""
    deserialize(container, "blobname")

read and deserialize from a blob "blobname" in `container::AzContainer`.

# Example
```
io = open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "foo.bin")
serialize(io, (rand(10),rand(20)))
a,b = deserialize(io)
```
"""
function Serialization.deserialize(c::AzContainer, o::AbstractString)
    io = IOBuffer(readbytes!(c, o, Vector{UInt8}(undef,filesize(c, o))); read=true)
    deserialize(io)
end

"""
    deserialize(object)

read and deserialize a blob `object::AzObject`.  See `deserialize(container, "blobname")`.
"""
Serialization.deserialize(o::AzObject) = deserialize(o.container, o.name)

"""
    cp(from..., to...)

copy a blob to a local file, a local file to a blob, or a blob to a blob.

# Examples

## local file to blob
```
cp("localfile.txt", AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt")
```

## blob to local file
```
cp(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt", "localfile.txt")
```

## blob to blob
```
cp(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_in.txt", AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_out.txt")
```
"""
function Base.cp(in::AbstractString, outc::AzContainer, outb::AbstractString)
    bytes = read!(in, Vector{UInt8}(undef,filesize(in)))
    write(outc, outb, bytes)
end

function Base.cp(inc::AzContainer, inb::AbstractString, out::AbstractString)
    bytes = read!(inc, inb, Vector{UInt8}(undef, filesize(inc, inb)))
    write(out, bytes)
end

function Base.cp(inc::AzContainer, inb::AbstractString, outc::AzContainer, outb::AbstractString)
    bytes = read!(inc, inb, Vector{UInt8}(undef, filesize(inc, inb)))
    write(outc, outb, bytes)
end

"""
    cp(from, to)

copy a blob to a local file, a local file to a blob, or a blob to a blob.

# Examples

## local file to blob
```
cp("localfile.txt", open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt"))
```

## blob to local file
```
cp(open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt"), "localfile.txt")
```

## blob to blob
```
cp(open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_in.txt"), open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_out.txt"))
```
"""
Base.cp(in::AbstractString, out::AzObject) = cp(in, out.container, out.name)
Base.cp(in::AzObject, out::AbstractString) = cp(in.container, in.name, out)
Base.cp(in::AzObject, out::AzObject) = cp(in.container, in.name, out.container, out.name)

Base.cp(inc::AzContainer, inb::AbstractString, out::AzObject) = cp(inc, inb, out.container, out.name)
Base.cp(in::AzObject, outc::AzContainer, outb::AbstractString) = cp(in.container, in.name, outc, outb)

"""
    readdir(container)

list of objects in a container.
"""
function Base.readdir(c::AzContainer; filterlist=true)
    marker = ""
    names = String[]
    while true
        r = @retry c.nretry HTTP.request(
            "GET",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)?restype=container&comp=list&marker=$marker",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
        xroot = root(parse_string(String(r.body)))
        blobs = xroot["Blobs"][1]["Blob"]
        _names = [content(blob["Name"][1]) for blob in blobs]
        if filterlist && c.prefix != ""
            _names = replace.(filter(_name->startswith(_name, c.prefix), _names), _normpath(c.prefix*"/")=>"")
        end
        names = [names; _names]
        marker = content(xroot["NextMarker"][1])
        marker == "" && break
    end
    names
end

"""
    dirname(container)

Returns the name of the Azure container that `container::AzContainer` is a handler to.
""" 
function Base.dirname(c::AzContainer)
    local nm
    if c.prefix == ""
        nm = c.containername
    else
        nm = _normpath(c.containername * "/" * c.prefix)
    end
    nm
end

AbstractStorage.session(c::AzContainer) = c.session

function AbstractStorage.scrubsession!(c::AzContainer)
    scrub!(c.session)
    c
end

function AbstractStorage.scrubsession(c::AzContainer)
    _c = copy(c)
    scrub!(_c.session)
    _c
end

AbstractStorage.minimaldict(c::AzContainer) = Dict("storageaccount"=>c.storageaccount, "containername"=>c.containername, "prefix"=>c.prefix)

"""
    isfile(container, "blobname")

Returns true if the blob "object" exists in `container::AzContainer`.
"""
function Base.isfile(c::AzContainer, object::AbstractString)
    try
        @retry c.nretry HTTP.request(
            "GET",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,object))?comp=metadata",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
    catch e
        if isa(e, HTTP.Exceptions.StatusError) && e.status == 404
            return false
        else
            throw(e)
        end
    end
    true
end

"""
    isfile(object::AzObject)

Returns true if the blob corresponding to `object` exists.
"""
Base.isfile(o::AzObject) = isfile(o.container, o.name)

iscontainer(c::AzContainer) = c.containername ∈ containers(storageaccount=c.storageaccount, session=c.session, nretry=c.nretry)

"""
    isdir(container)

Returns true if `container::AzContainer` exists.
"""
function Base.isdir(c::AzContainer)
    if !iscontainer(c)
        return false
    end
    if c.prefix == ""
        return true
    end
    !isempty(readdir(c::AzContainer))
end

"""
    containers(;storageaccount="mystorageaccount", session=AzSession(;lazy=true, scope=__OAUTH_SCOPE))

list all containers in a given storage account.
"""
function containers(;storageaccount, session=AzSession(;lazy=true, scope=__OAUTH_SCOPE), nretry=5)
    marker = ""
    names = String[]
    while true
        r = @retry nretry HTTP.request(
            "GET",
            "https://$storageaccount.blob.core.windows.net/?comp=list&marker=$marker",
            [
                "Authorization" => "Bearer $(token(session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
        xroot = root(parse_string(String(r.body)))
        containers = xroot["Containers"][1]["Container"]
        names = [names; [content(container["Name"][1]) for container in containers]]
        marker = content(xroot["NextMarker"][1])
        marker == "" && break
    end
    names
end

"""
    filesize(container, "blobname")

Returns the size of the blob "blobname" that is in `container::AzContainer`
"""
function Base.filesize(c::AzContainer, o::AbstractString)
    r = @retry c.nretry HTTP.request(
        "HEAD",
        "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
        [
            "Authorization" => "Bearer $(token(c.session))",
            "x-ms-version" => API_VERSION
        ],
        retry = false)
    n = 0
    for header in r.headers
        if header.first == "Content-Length"
            n = parse(Int, header.second)
        end
    end
    n
end

"""
    filesize(object::AzObject)

Returns the size of the blob corresponding to `object::AzObject`
"""
Base.filesize(o::AzObject) = filesize(o.container, o.name)

"""
    rm(container, "blobname")

remove the blob "blobname" from `container::AzContainer`.
"""
function Base.rm(c::AzContainer, o::AbstractString)
    try
        @retry c.nretry HTTP.request(
            "DELETE",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
    catch
        @warn "error removing $(c.containername)/$(addprefix(c,o))"
    end
    nothing
end

"""
    rm(object::AzObject)

remove the blob corresponding to `object::AzObject`
"""
Base.rm(o::AzObject) = rm(o.container, o.name)

"""
    rm(container)

remove `container::AzContainer` and all of its blobs.
"""
function Base.rm(c::AzContainer)
    function _rm(c::AzContainer)
        @retry c.nretry HTTP.request(
            "DELETE",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)?restype=container",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false)
    end

    try
        if c.prefix == ""
            _rm(c)
        else
            for o in readdir(c)
                rm(c, o)
            end
            isempty(readdir(c; filterlist=false)) && _rm(c)
        end
    catch
        @warn "error removing $(c.containername)"
    end

    nothing
end

"""
    cp(container_src, container_dst)

copy `container_src::AzContainer` and its blobs to `container_dst::AzContainer`.
"""
function Base.cp(src::AzContainer, dst::AzContainer)
    mkpath(dst)

    blobs = readdir(src)
    for blob in blobs
        @retry dst.nretry HTTP.request(
            "PUT",
            "https://$(dst.storageaccount).blob.core.windows.net/$(dst.containername)/$(addprefix(dst,blob))",
            [
                "Authorization" => "Bearer $(token(dst.session))",
                "x-ms-version" => API_VERSION,
                "x-ms-copy-source" => "https://$(src.storageaccount).blob.core.windows.net/$(src.containername)/$(addprefix(src,blob))"
            ],
            retry = false)
    end
    nothing
end

export AzContainer, containers, readdlm, writedlm

end
