module AzStorage

using AbstractStorage, AzSessions, AzStorage_jll, Base64, Dates, DelimitedFiles, XML, HTTP, Printf, ProgressMeter, Serialization, Sockets

# https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
# https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/request-limits-and-throttling
const RETRYABLE_HTTP_ERRORS = [
    429, # Too many requests
    500, # Internal server error
    503] # Service unavailable

# https://curl.haxx.se/libcurl/c/libcurl-errors.html
const RETRYABLE_CURL_ERRORS = [
    6,  # Couldn't resolve host. The given remote host was not resolved.
    7,  # Failed to connect() to host or proxy.
    28, # Connection timed out.
    35, # SSL handshake failure.
    42, # aborted by call-back, used to abort when the first byte to read/write times out.
    55, # Failed sendingnetworkdata.
    56] # Failure with received network data.

# https://docs.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services
const API_VERSION = "2021-08-06"

# buffer size for holding OAuth2 tokens
const BUFFER_SIZE = unsafe_load(cglobal((:BUFFER_SIZE, libAzStorage), Int32))

windows_one_thread(nthreads) = Sys.iswindows() ? 1 : nthreads

function __init__()
    if Sys.iswindows()
        @warn "On Windows, AzStorage is limited to a single thread, meaning that performance may be degraded."
    end
    @ccall libAzStorage.curl_init(length(RETRYABLE_HTTP_ERRORS)::Cint, length(RETRYABLE_CURL_ERRORS)::Cint, RETRYABLE_HTTP_ERRORS::Ptr{Clong}, RETRYABLE_CURL_ERRORS::Ptr{Clong}, API_VERSION::Cstring)::Cvoid
    resetperf_counters()
end

mutable struct AzContainer{A<:AzSessionAbstract} <: Container
    storageaccount::String
    containername::String
    prefix::String
    session::A
    nthreads::Int
    connect_timeout::Int
    read_timeout::Int
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
        connect_timeout,
        read_timeout,
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

"""
    open(object::AzObject[, mode="w+"]) -> object

This is an identity operation to support compatability with POSIX I/O. It allows for the
following equivalence which can be useful in building methods that are agnostic to storage
systems:
```
io = open(joinpath(AzContainer("foo";storageaccount="bar"), "bar")) # Azure blob sorage
io = open(joinpath("foo", "bar")) # POSIX
write(io, "hello")
close(io)
```
Please note that the 'mode' is for compatability with `Base.open` and does not have
any effect due to the how Azure blob storage works.
"""
Base.open(object::AzObject, mode="w+") = object

Base.close(object::AzObject) = nothing

const __OAUTH_SCOPE = "offline_access+openid+https://storage.azure.com/user_impersonation"

"""
    container = AzContainer("containername"; storageaccount="myacccount", kwargs...)

`container` is a handle to a new or existing Azure container in the `myaccount` sorage account.
The storage account must already exist.

# Additional keyword arguments
* `session=AzSession(;lazy=false,scope=$__OAUTH_SCOPE)` user credentials (see AzSessions.jl package).
* `nthreads=Sys.CPU_THREADS` number of system threads that OpenMP will use to thread I/O.
* `connect_timeout=30` client-side timeout for connecting to the server.
* `read_timeout=10` client-side timeout for receiving the first byte from the server.
* `nretry=10` number of retries to the Azure service (when Azure throws a retryable error) before throwing an error.
* `verbose=0` verbosity flag passed to libcurl.

# Notes
The container name can container "/"'s.  If this is the case, then the string preceding the first "/" will
be the container name, and the string that remains will be pre-pended to the blob names.  This allows Azure
to present blobs in a pseudo-directory structure.
"""
function AzContainer(containername::AbstractString; storageaccount, session=AzSession(;lazy=false, scope=__OAUTH_SCOPE), nthreads=Sys.CPU_THREADS, connect_timeout=10, read_timeout=30, nretry=10, verbose=0, prefix="")
    name = split(containername, '/')
    _containername = name[1]
    prefix *= lstrip('/'*join(name[2:end], '/'), '/')
    AzContainer(String(storageaccount), String(_containername), String(prefix), session, windows_one_thread(nthreads), connect_timeout, read_timeout, nretry, verbose)
end

function AbstractStorage.Container(::Type{<:AzContainer}, d::Dict, session=AzSession(;lazy=false, scope=__OAUTH_SCOPE); nthreads=Sys.CPU_THREADS, connect_timeout=10, read_timeout=30, nretry=10, verbose=0)
    AzContainer(
        d["storageaccount"],
        d["containername"],
        d["prefix"],
        session,
        windows_one_thread(get(d, "nthreads", nthreads)),
        connect_timeout,
        read_timeout,
        get(d, "nretry", nretry),
        get(d, "verbose", verbose))
end

Base.:(==)(x::AzContainer, y::AzContainer) = x.storageaccount == y.storageaccount && x.containername == y.containername && x.prefix == y.prefix

struct ResponseCodes
    http::Int64
    curl::Int64
    retry_after::Int32
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

status(e::HTTP.StatusError) = e.status
status(e) = 999

function retrywarn(i, s, e)
    @debug "retry $i, sleeping for $s seconds, e=$e"
end

macro retry(retries, ex::Expr)
    quote
        r = nothing
        for i = 1:$(esc(retries))
            try
                r = $(esc(ex))
                break
            catch e
                (i < $(esc(retries)) && isretryable(e)) || throw(e)
                maximum_backoff = 256
                s = min(2.0^(i-1), maximum_backoff) + rand()
                if status(e) == 429
                    i = findfirst(header->header[1] == "Retry-After", e.response.headers)
                    if i !== nothing
                        s = parse(Int, header[2]) + rand()
                    end
                end
                retrywarn(i, s, e)
                sleep(s)
            end
        end
        r
    end
end

function new_pointer_array_from_string(input)
    _input = transcode(UInt8, input)
    output = Vector{UInt8}(undef, BUFFER_SIZE)
    copyto!(output, 1, _input, 1, length(_input))
    output[length(_input)+1] = '\0'
    output
end

function authinfo(session::AzSessions.AzClientCredentialsSession)
    _token = new_pointer_array_from_string(session.token)
    refresh_token = C_NULL
    expiry = [floor(UInt64, datetime2unix(session.expiry))]
    scope = C_NULL
    resource = session.resource
    tenant = session.tenant
    clientid = session.client_id
    client_secret = session.client_secret
    _token,refresh_token,expiry,scope,resource,tenant,clientid,client_secret
end

function authinfo(session::Union{AzSessions.AzDeviceCodeFlowSession,AzSessions.AzAuthCodeFlowSession})
    _token = new_pointer_array_from_string(session.token)
    _refresh_token = new_pointer_array_from_string(session.refresh_token)
    expiry = [floor(UInt64, datetime2unix(session.expiry))]
    scope = session.scope
    resource = AzSessions.audience_from_scope(session.scope)
    tenant = session.tenant
    clientid = session.client_id
    client_secret = C_NULL
    _token,_refresh_token,expiry,scope,resource,tenant,clientid,client_secret
end

function authinfo(session::AzSessions.AzVMSession)
    refresh_token = C_NULL
    expiry = [floor(UInt64, datetime2unix(session.expiry))]
    scope = C_NULL
    resource = session.resource
    tenant = C_NULL
    clientid = C_NULL
    client_secret = C_NULL
    refresh_token,expiry,scope,resource,tenant,clientid,client_secret
end

function authinfo!(session::AzSessions.AzClientCredentialsSession, _token, refresh_token, expiry)
    session.expiry = unix2datetime(expiry[1])
    session.token = unsafe_string(pointer(_token))
end

function authinfo!(session::Union{AzSessions.AzDeviceCodeFlowSession,AzSessions.AzAuthCodeFlowSession}, _token, refresh_token, expiry)
    session.expiry = unix2datetime(expiry[1])
    session.token = unsafe_string(pointer(_token))
    session.refresh_token = unsafe_string(pointer(refresh_token))
end

function authinfo!(session::AzSessions.AzVMSession, _token, refresh_token, expiry)
    session.expiry = unix2datetime(expiry[1])
    session.token = unsafe_string(pointer(_token))
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
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)
    end
    nothing
end

# 4000 MiB, there are 2^20 bytes in one MiB
const _MINBYTES_PER_BLOCK = 32 * 2^20
const _MAXBYTES_PER_BLOCK = 4_000 * 2^20
const _MAXBLOCKS_PER_BLOB = 50_000

nblocks_error() = error("data is too large for a block-blob")
function nblocks(nthreads::Integer, nbytes::Integer, max_bytes_per_block=_MAXBYTES_PER_BLOCK)
    nblocks = ceil(Int, nbytes/min(_MAXBYTES_PER_BLOCK, max_bytes_per_block) + eps(Float64))
    if nblocks < nthreads
        nblocks = clamp(ceil(Int, nbytes/_MINBYTES_PER_BLOCK + eps(Float64)), 1, nthreads)
    end
    nblocks > _MAXBLOCKS_PER_BLOB && nblocks_error()
    nblocks
end

_normpath(s) = Sys.iswindows() ? replace(normpath(s), "\\"=>"/") : normpath(s)

addprefix(c::AzContainer, o) = c.prefix == "" ? o : _normpath("$(c.prefix)/$o")

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
        verbose = c.verbose,
        connect_timeout = c.connect_timeout,
        readtimeout = c.read_timeout)
    nothing
end

function isinvalidblocklist(e)
    b = XML.parse(String(e.response.body), LazyNode)
    for child in children(b)
        if tag(child) == "Error"
            for grandchild in children(child)
                if tag(grandchild) == "Code"
                    if value(first(children(grandchild))) == "InvalidBlockList"
                        return true
                    end
                end
            end
        end
    end
    false
end

function committed_blocklist(c, o)
    r = @retry c.nretry HTTP.request(
        "GET",
        "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))?comp=blocklist",
        [
            "x-ms-version" => API_VERSION,
            "Authorization" => "Bearer $(token(c.session))"
        ],
        retry = false,
        verbose = c.verbose,
        connect_timeout = c.connect_timeout,
        readtimeout = c.read_timeout
    )

    b = XML.parse(String(r.body), LazyNode)
    committedblocks = String[]
    for child in children(b)
        if tag(child) == "BlockList"
            for grandchild in children(child)
                if tag(grandchild) == "CommittedBlocks"
                    for greatgrandchild in children(grandchild)
                        for greatgreatgrandchild in children(greatgrandchild)
                            if tag(greatgreatgrandchild) == "Name"
                                push!(committedblocks, value(first(children(greatgreatgrandchild))))
                                break
                            end
                        end
                    end
                end
                break
            end
            break
        end
    end
    committedblocks
end

function putblocklist(c, o, blockids)
    xroot = XML.Element("BlockList")
    for blockid in blockids
        push!(xroot, XML.Element("Uncommitted", blockid))
    end
    xdoc = XML.Document(XML.Declaration(version=1.0, encoding="UTF-8"), xroot)
    blocklist = XML.write(xdoc; indentsize=0)

    try
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
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)
    catch e
        isa(e, HTTP.Exceptions.StatusError) || throw(e)

        #=
        Special handling for 400 errors with "InvalidBlockList" error code.
        We think there is a race condition that can cause putblocklist to
        be called twice, and putblocklist is not idempotent.
        =#
        (e.response.status == 400 && isinvalidblocklist(e)) || throw(e)

        _blockids = committed_blocklist(c, o)
        sort(blockids) == sort(_blockids) || throw(e) # do nothing if the blocks were already committed
    end
    nothing
end

function blockids(_nblocks)
    l = ceil(Int, log10(_nblocks))
    [base64encode(lpad(blockid-1, l, '0')) for blockid in 1:_nblocks]
end

function writebytes_block(c, o, data, _blockids)
    __blockids = [HTTP.escapeuri(_blockid) for _blockid in _blockids]
    token(c.session)
    _token,refresh_token,expiry,scope,resource,tenant,clientid,client_secret = authinfo(c.session)
    r = @ccall libAzStorage.curl_writebytes_block_retry_threaded(_token::Ptr{UInt8}, refresh_token::Ptr{UInt8}, expiry::Ptr{Culong}, scope::Cstring, resource::Cstring, tenant::Cstring,
        clientid::Cstring, client_secret::Cstring, c.storageaccount::Cstring, c.containername::Cstring, addprefix(c,o)::Cstring, __blockids::Ptr{Cstring}, data::Ptr{UInt8},
        length(data)::Csize_t, c.nthreads::Cint, length(__blockids)::Cint, c.nretry::Cint, c.verbose::Cint, c.connect_timeout::Clong, c.read_timeout::Clong)::ResponseCodes
    (r.http >= 300 || r.curl > 0) && error("writebytes_block error: http code $(r.http), curl code $(r.curl)")
    authinfo!(c.session, _token, refresh_token, expiry)
end

function writebytes(c::AzContainer, o::AbstractString, data::DenseArray{UInt8}; contenttype="application/octet-stream")
    if Sys.iswindows()
        writebytes_blob(c, o, data, contenttype)
    else
        _blockids = blockids(nblocks(c.nthreads, length(data)))
        writebytes_block(c, o, data, _blockids)
        putblocklist(c, o, _blockids)
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
Base.touch(c::AzContainer, o::AbstractString) = write(c, o, "\0")

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
                verbose = c.verbose,
                connect_timeout = c.connect_timeout,
                readtimeout = c.read_timeout) do io
            read!(io, data)
        end
        nothing
    end

    function readbytes_threaded!(c, o, data, offset, _nthreads)
        # heuristic to increase probability that token is valid during the retry logic in AzSessions.c
        t = token(c.session)
        _token,refresh_token,expiry,scope,resource,tenant,clientid,client_secret = authinfo(c.session)
        r = @ccall libAzStorage.curl_readbytes_retry_threaded(_token::Ptr{UInt8}, refresh_token::Ptr{UInt8}, expiry::Ptr{Culong}, scope::Cstring, resource::Cstring, tenant::Cstring,
            clientid::Cstring, client_secret::Cstring, c.storageaccount::Cstring, c.containername::Cstring, addprefix(c,o)::Cstring, data::Ptr{UInt8}, offset::Csize_t,
            length(data)::Csize_t, _nthreads::Cint, c.nretry::Cint, c.verbose::Cint, c.connect_timeout::Clong, c.read_timeout::Clong)::ResponseCodes
        (r.http >= 300 || r.curl > 0) && error("readbytes_threaded! error: http code $(r.http), curl code $(r.curl)")
        authinfo!(c.session, _token, refresh_token, expiry)
        nothing
    end

    _nthreads = nthreads_effective(c.nthreads, length(data))
    if Sys.iswindows()
        readbytes_serial!(c, o, data, offset)
    else
        readbytes_threaded!(c, o, data, offset, _nthreads)
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
cp("localfile.txt", AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt"; buffersize=2_000_000_000, show_progress=false)
```

## blob to local file
```
cp(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt", "localfile.txt", buffersize=2_000_000_000, show_progress=false)
```
`buffersize` is the memory buffer size (in bytes) used in the copy algorithm, and defaults to `2_000_000_000` bytes (2GB).  Note that
half of this memory is used to buffer reads, and the other half is used to buffer writes. Set `show_progress=true` to display a
progress bar for the copy operation.
## blob to blob
```
cp(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_in.txt", AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_out.txt")
```
"""
function Base.cp(in::AbstractString, outc::AzContainer, outb::AbstractString; buffersize=2_000_000_000, show_progress=false)
    if Sys.iswindows()
        bytes = read!(in, Vector{UInt8}(undef, filesize(in)))
        write(outc, outb, bytes)
    else
        n = filesize(in)
        _buffersize = div(buffersize, 2)
        _nblocks = nblocks(outc.nthreads, n, div(_buffersize, outc.nthreads))
        _blockids = blockids(_nblocks)
        nominal_bytes_per_block,remaining_bytes_per_block = divrem(n, _nblocks)
        nblocks_per_buffer,remaining_blocks_per_buffer = divrem(_buffersize, nominal_bytes_per_block)
        nblocks_per_buffer += remaining_blocks_per_buffer > 0 ? 1 : 0

        buffer_read,buffer_write = ntuple(_->Vector{UInt8}(undef, nblocks_per_buffer*(nominal_bytes_per_block + 1)), 2)

        tsk_write = @async nothing
        i2byte = nbytes_buffer = 0
        i1block = 1
        io = open(in, "r")
        iter = 1:_nblocks
        speed_read,speed_write = 0.0,0.0
        progress = Progress(length(iter); enabled=show_progress, desc="read/write = 0.00/0.00 MB/s")
        for iblock = 1:_nblocks
            i1byte = i2byte + 1

            if iblock <= remaining_bytes_per_block
                i2byte = min(n, i1byte + nominal_bytes_per_block)
            else
                i2byte = min(n, i1byte + nominal_bytes_per_block - 1)
            end

            nbytes_buffer += i2byte - i1byte + 1

            if iblock == _nblocks || nbytes_buffer >= _buffersize
                _buffer_read = @view buffer_read[1:nbytes_buffer]
                t_read = @elapsed read!(io, _buffer_read)
                speed_read = (nbytes_buffer / 1_000_000) / t_read

                wait(tsk_write)
                buffer_read,buffer_write = buffer_write,buffer_read

                nbytes_buffer_write,i1block_write,iblock_write = nbytes_buffer,i1block,iblock
                _buffer_write = @view buffer_write[1:nbytes_buffer_write]
                tsk_write = @async begin
                    t_write = @elapsed writebytes_block(outc, outb, _buffer_write, _blockids[i1block_write:iblock_write])
                    speed_write = (nbytes_buffer_write / 1_000_000) / t_write
                end
                i1block = iblock + 1
                nbytes_buffer = 0
            end
            progress.core.desc = @sprintf "read/write = %.2f/%.2f MB/s" speed_read speed_write
            next!(progress)
        end
        wait(tsk_write)
        putblocklist(outc, outb, _blockids)
    end
end

function Base.cp(inc::AzContainer, inb::AbstractString, out::AbstractString; buffersize=2_000_000_000, show_progress=false)
    n = filesize(inc, inb)
    io = open(out, "w")
    _buffersize = div(buffersize, 2)
    buffer_read,buffer_write = ntuple(_->Vector{UInt8}(undef, min(_buffersize, n)), 2)
    tsk_write = @async nothing
    speed_read,speed_write = 0.0,0.0
    iter = 0:_buffersize:n-1
    progress = Progress(length(iter); enabled=show_progress, desc="read/write = 0.00/0.00 MB/s")
    for i1 = iter
        __buffersize = min(_buffersize, n - i1)
        _buffer_read = _buffersize == __buffersize ? buffer_read : view(buffer_read, 1:__buffersize)
        t_read = @elapsed read!(inc, inb, _buffer_read, offset=i1)
        speed_read = (__buffersize / 1_000_000) / t_read

        wait(tsk_write)
        buffer_read,buffer_write = buffer_write,buffer_read

        __buffersize_write = __buffersize
        _buffer_write = _buffersize == __buffersize ? buffer_write : view(buffer_write, 1:__buffersize_write)
        tsk_write = @async begin
            t_write = @elapsed write(io, _buffer_write)
            speed_write = (__buffersize_write / 1_000_000) / t_write
        end
        progress.core.desc = @sprintf "read/write = %.2f/%.2f MB/s" speed_read speed_write
        next!(progress)
    end
    wait(tsk_write)
    close(io)
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
cp("localfile.txt", open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt"); buffersize=2_000_000_000, show_progress=false)
```

## blob to local file
```
cp(open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob.txt"), "localfile.txt"; buffersize=2_000_000_000, show_progress=false)
```
`buffersize` is the memory buffer size (in bytes) used in the copy algorithm, and defaults to `2_000_000_000` bytes (2GB).  Note that
half of this memory is used to buffer reads, and the other half is used to buffer writes. Set `show_progress=true` to display a
progress bar for the copy operation.

## blob to blob
```
cp(open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_in.txt"), open(AzContainer("mycontainer";storageaccount="mystorageaccount"), "remoteblob_out.txt"))
```
"""
Base.cp(in::AbstractString, out::AzObject; kwargs...) = cp(in, out.container, out.name; kwargs...)
Base.cp(in::AzObject, out::AbstractString; kwargs...) = cp(in.container, in.name, out; kwargs...)
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
    prefix = filterlist ? c.prefix : ""
    while true
        r = @retry c.nretry HTTP.request(
            "GET",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)?restype=container&comp=list&prefix=$prefix&marker=$marker",
            [
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)

        xdoc = XML.parse(LazyNode, String(r.body))
        for node in children(xdoc)
            if tag(node) == "EnumerationResults"
                for _node in children(node)
                    if tag(_node) == "Blobs"
                         for __node in children(_node)
                            name = value(first(children(first(children(__node)))))
                            if filterlist
                                push!(names, replace(name, _normpath(c.prefix*"/")=>""))
                            else
                                push!(names, name)
                            end
                        end
                    elseif tag(_node) == "NextMarker"
                       marker = isempty(children(_node)) ? "" : value(first(children(_node)))
                    end
                end
                break
            end
        end
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

AbstractStorage.backend(_::AzContainer) = "azureblob"

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
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)
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
    containers(;storageaccount="mystorageaccount", session=AzSession(;lazy=false, scope=__OAUTH_SCOPE), nretry=5, verbose=0, connect_timeout=30, read_timeout=10)

list all containers in a given storage account.
"""
function containers(;storageaccount, session=AzSession(;lazy=false, scope=__OAUTH_SCOPE), nretry=5, verbose=0, connect_timeout=30, read_timeout=10)
    marker = ""
    containernames = String[]
    while true
        r = @retry nretry HTTP.request(
            "GET",
            "https://$storageaccount.blob.core.windows.net/?comp=list&marker=$marker",
            [
                "Authorization" => "Bearer $(token(session))",
                "x-ms-version" => API_VERSION
            ],
            retry = false,
            verbose = verbose,
            connect_timeout = connect_timeout,
            readtimeout = read_timeout)

        xdoc = XML.parse(LazyNode, String(r.body))
        for node in children(xdoc)
            if tag(node) == "EnumerationResults"
                for _node in children(node)
                    if tag(_node) == "Containers"
                        for __node in children(_node)
                            for ___node in children(__node)
                                if tag(___node) == "Name"
                                    name = value(first(children(___node)))
                                    push!(containernames, name)
                                    break
                                end
                            end
                        end
                    elseif tag(_node) == "NextMarker"
                        marker = isempty(children(_node)) ? "" : value(first(children(_node)))
                    end
                end
            end
        end
        marker == "" && break
    end
    containernames
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
        retry = false,
        verbose = c.verbose,
        connect_timeout = c.connect_timeout,
        readtimeout = c.read_timeout)
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
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)
    catch
        @warn "error removing $(c.containername)/$(addprefix(c,o))"
    end
    nothing
end

"""
    rm(object::AzObject; force=false)

remove the blob corresponding to `object::AzObject`.  Note that
the `force` keyword argument does not change the behavior of this
method.  It is included to match Julia's `Base.rm` method, allowing
the calling code to work on both POSIX and Azure storage.
"""
Base.rm(o::AzObject; force=false) = rm(o.container, o.name)

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
            retry = false,
            verbose = c.verbose,
            connect_timeout = c.connect_timeout,
            readtimeout = c.read_timeout)
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
            retry = false,
            verbose = src.verbose,
            connect_timeout = src.connect_timeout,
            readtimeout = src.read_timeout)
    end
    nothing
end

struct PerfCounters
    ms_wait_throttled::Clonglong
    ms_wait_timeouts::Clonglong
    count_throttled::Clonglong
    count_timeouts::Clonglong
end

"""
    AzStorage.resetperf_counters()

Reset the performance counters to zero.  Please see the `AzStorage.getperf_counters()` documentation for
more information.
"""
resetperf_counters() = @ccall libAzStorage.resetperf_counters()::Cvoid

"""
    performance_counters = AzStorage.getperf_counters()

IO operations performed via the AzStorage package are monitored for client-side timeouts and service throttling.
In particular, `performance_counters` will store the following information:

* `performance_counters.ms_wait_throttled` is the time in milliseconds that IO was delayed due to service throttling.
* `performance_counters.ms_wait_timeouts` is the time in milliseconds that IO was delayed due to client-side time-outs caused by an unresponsive Azure service.
* `performance_counters.count_throttled` is a count of the total number of times that the service throttles requests.
* `performance_counters.count_timeouts` is a count of the total number of times that the service was unresponsive, causing client-side time-outs.

Note that the information stored is global, and not specfic to any one given IO operation.  See `AzStorage.reset_perf_counters()`.
"""
getperf_counters() = @ccall libAzStorage.getperf_counters()::PerfCounters

export AzContainer, containers, readdlm, writedlm

end
