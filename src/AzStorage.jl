module AzStorage

using AbstractStorage, AzSessions#=, AzStorage_jll=#, Base64, HTTP, LightXML, Serialization, Sockets

const libAzStorage = normpath(joinpath(Base.source_path(),"../libAzStorage"))

# https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
const RETRYABLE_HTTP_ERRORS = [
    500, # Internal server error
    503] # Service unavailable

# https://curl.haxx.se/libcurl/c/libcurl-errors.html
const RETRYABLE_CURL_ERRORS = [
    7,  # Failed to connect() to host or proxy.
    28, # Connection timed out.
    55, # Failed sendingnetworkdata.
    56] # Failure with received network data.

function __init__()
    ccall((:curl_init, libAzStorage), Cvoid, (Cint, Cint, Ptr{Clong}, Ptr{Clong}),
        length(RETRYABLE_HTTP_ERRORS), length(RETRYABLE_CURL_ERRORS), RETRYABLE_HTTP_ERRORS, RETRYABLE_CURL_ERRORS)
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
    AzContainer(String(storageaccount), String(_containername), String(prefix), session, nthreads, nretry, verbose)
end

AbstractStorage.Container(::Type{<:AzContainer}, d::Dict, session=AzSession(;lazy=true, scope=__OAUTH_SCOPE)) =
    AzContainer(d["storageaccount"], d["containername"], d["prefix"], session, d["nthreads"], d["nretry"], d["verbose"])

struct ResponseCodes
    http::Int64
    curl::Int64
end

function isretryable(e::HTTP.StatusError)
    e.status ∈ RETRYABLE_HTTP_ERRORS && (return true)
    false
end
isretryable(e::Base.IOError) = true
isretryable(e::HTTP.IOExtras.IOError) = isretryable(e.e)
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
            Dict(
                "Authorization"=>"Bearer $(token(c.session))",
                "x-ms-version"=>"2017-11-09"),
            retry = false)
    end
    nothing
end

const _MINBYTES_PER_BLOCK = 32_000_000
const _MAXBYTES_PER_BLOCK = 100_000_000
const _MAXBLOCKS_PER_BLOB = 50_000

nblocks_error1() = error("data is too large for a block-blob: too many blocks")
nblocks_error2() = error("data is too large for a block-block: too many bytes per block")
function nblocks(nthreads::Integer, nbytes::Integer)
    nblocks = ceil(Int, nbytes/_MAXBYTES_PER_BLOCK + eps(Float64))
    if nblocks < nthreads
        bytes_per_block = max(div(nblocks, nthreads), _MINBYTES_PER_BLOCK)
        nblocks = max(1, ceil(Int, nbytes/bytes_per_block))
    end
    nblocks > _MAXBLOCKS_PER_BLOB && nblocks_error1()
    bytes_per_block = div(nbytes, nblocks) + (rem(nbytes, nblocks) == 0 ? 0 : 1)
    bytes_per_block > _MAXBYTES_PER_BLOCK && nblocks_error2()
    nblocks
end

_normpath(s) = Sys.iswindows() ? replace(normpath(s), "\\"=>"/") : normpath(s)

addprefix(c::AzContainer, o) = c.prefix == "" ? o : _normpath("$(c.prefix)/$o")

function writebytes(c::AzContainer, o::AbstractString, data::DenseArray{UInt8}; contenttype="application/octet-stream")
    function writebytes_blob(c, o, data, contenttype)
        @retry c.nretry HTTP.request(
            "PUT",
            "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
            Dict(
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => "2017-11-09",
                "Content-Length" => "$(length(data))",
                "Content-Type" => contenttype,
                "x-ms-blob-type" => "BlockBlob"),
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
            Dict(
                "x-ms-version" => "2017-11-09",
                "Authorization" => "Bearer $(token(c.session))",
                "Content-Type" => "application/octet-stream",
                "Content-Length" => "$(length(blocklist))"),
            blocklist,
            retry = false)
        nothing
    end

    function writebytes_block(c, o, data, _nblocks)
        t = token(c.session)
        l = ceil(Int, log10(_nblocks))
        blockids = [base64encode(lpad(blockid-1, l, '0')) for blockid in 1:_nblocks]
        _blockids = [HTTP.escapeuri(blockid) for blockid in blockids]
        r = ccall((:curl_writebytes_block_retry_threaded, libAzStorage), ResponseCodes,
            (Cstring, Cstring,          Cstring,         Cstring,        Ptr{Cstring}, Ptr{UInt8}, Csize_t,      Cint,       Cint,     Cint,     Cint),
             t,       c.storageaccount, c.containername, addprefix(c,o), _blockids,    data,       length(data), c.nthreads, _nblocks, c.nretry, c.verbose)
        r.http >= 300 && error("writebytes_block: error code $(r.http)")
        r.curl > 0 && error("curl error, code=$(r.curl)")

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

"""
    write(container, "blobname", data::DenseArray)

Write the array `data` to a blob with the name `blobname` in `container::AzContainer`.
"""
Base.write(c::AzContainer, o::AbstractString, data::DenseArray{T}) where {T<:Number} =
    writebytes(c, o, unsafe_wrap(Vector{UInt8}, convert(Ptr{UInt8}, pointer(data)), length(data)*sizeof(T), own=false); contenttype="application/octet-stream")

function Base.write(c::AzContainer, o::AbstractString, data::AbstractArray)
    io = IOBuffer()
    serialize(io, data)
    writebytes(c, o, take!(io); contenttype="application/octet-stream")
end

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

nthreads_effective(nthreads::Integer, nbytes::Integer) = clamp(div(nbytes, _MINBYTES_PER_BLOCK), 1, nthreads)

function readbytes!(c::AzContainer, o::AbstractString, data::DenseArray{UInt8}; offset=0)
    function readbytes_serial!(c, o, data, offset)
        HTTP.open(
                "GET",
                "https://$(c.storageaccount).blob.core.windows.net/$(c.containername)/$(addprefix(c,o))",
                Dict(
                    "Authorization" => "Bearer $(token(c.session))",
                    "x-ms-version" => "2017-11-09",
                    "Range" => "bytes=$offset-$(offset+length(data)-1)"),
                    retry = false,
                    verbose = c.verbose) do io
            read!(io, data)
        end
        nothing
    end

    function readbytes_threaded!(c, o, data, offset, _nthreads)
        t = token(c.session)
        r = ccall((:curl_readbytes_retry_threaded, libAzStorage), ResponseCodes,
            (Cstring, Cstring,          Cstring,         Cstring,        Ptr{UInt8}, Csize_t, Csize_t,      Cint,      Cint,     Cint),
             t,       c.storageaccount, c.containername, addprefix(c,o), data,       offset,  length(data), _nthreads, c.nretry, c.verbose)
        r.http >= 300 && error("readbytes_threaded!: error code $(r.http)")
        r.curl > 0 && error("curl error, code=$(r.curl))")
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
function Base.read!(c::AzContainer, o::AbstractString, data::DenseArray{T}; offset=0) where {T<:Number}
    _data = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), length(data)*sizeof(T), own=false)
    readbytes!(c, o, _data; offset=offset*sizeof(T))
    data
end

"""
    data = read(container, "blobname")

read from a blob "blobname" in `container::AzContainer` into new memory.
"""
function Base.read(c::AzContainer, o::String)
    io = IOBuffer(readbytes!(c, o, Array{UInt8}(undef, filesize(c, o))))
    deserialize(io)
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
            Dict(
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => "2017-11-09"),
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

"""
    isfile(container, "blobname")

Returns true if the blob "blobname" exists in `container::AzContainer`.
"""
Base.isfile(c::AzContainer, object::AbstractString) = object ∈ readdir(c)

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
            Dict(
                "Authorization" => "Bearer $(token(session))",
                "x-ms-version" => "2017-11-09"),
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
        Dict(
            "Authorization" => "Bearer $(token(c.session))",
            "x-ms-version" => "2017-11-09"),
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
            Dict(
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => "2017-11-09"),
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
            Dict(
                "Authorization" => "Bearer $(token(c.session))",
                "x-ms-version" => "2017-11-09"),
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
            Dict(
                "Authorization" => "Bearer $(token(dst.session))",
                "x-ms-version" => "2017-11-09",
                "x-ms-copy-source" => "https://$(src.storageaccount).blob.core.windows.net/$(src.containername)/$(addprefix(src,blob))"),
            retry = false)
    end
    nothing
end

export AzContainer, containers

end
