using AbstractStorage, AzSessions, AzStorage, AzStorage_jll, Base64, Dates, HTTP, JSON, Serialization, Test, UUIDs

function robust_mkpath(c)
    local _c
    try
        mkpath(c)
        _c = c
    catch
        r = uuid4()
        _c = AzContainer("foo-$r-o"; storageaccount=c.storageaccount, session=c.session, nthreads=2, nretry=10)
        robust_mkpath(_c)
    end
    _c
end

function robust_joinpath(c, s...)
    try
        io = joinpath(c, s...)
        return io
    catch
        r = uuid4()
        _c = AzContainer("foo-$r-o"; storageaccount=c.storageaccount, session=c.session, nthreads=2, nretry=10)
        robust_joinpath(_c, s...)
    end
end

function robust_open(c, s...)
    try
        io = open(c, s...)
        return io
    catch
        r = uuid4()
        _c = AzContainer("foo-$r-o"; storageaccount=c.storageaccount, session=c.session, nthreads=2, nretry=10)
        robust_open(_c, s...)
    end
end

if haskey(ENV, "TENANT") && haskey(ENV, "CLIENT_ID") && haskey(ENV, "CLIENT_SECRET")
    client_id = ENV["CLIENT_ID"]
    client_secret = ENV["CLIENT_SECRET"]
    tenant = ENV["TENANT"]
    AzSessions.write_manifest(;client_id, client_secret, tenant)
    session = AzSession(;protocal=AzClientCredentials, client_id, client_secret, resource="https://storage.azure.com/")
else
    session = AzSession(;scope="openid+offline_access+https://storage.azure.com/user_impersonation")
end

storageaccount = ENV["STORAGE_ACCOUNT"]
@info "storageaccount=$storageaccount"

for container in containers(;storageaccount=storageaccount,session=session)
    rm(AzContainer(container;storageaccount=storageaccount,session=session))
end
@info "sleeping for 60 seconds to ensure Azure clean-up from any previous run"
sleep(60)

@testset "Error codes" begin
    @test unsafe_load(cglobal((:N_HTTP_RETRY_CODES, AzStorage.libAzStorage), Cint)) == 3
    x = Sys.iswindows() ? unsafe_load(cglobal((:HTTP_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clonglong})) : unsafe_load(cglobal((:HTTP_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clong}))
    y = unsafe_wrap(Array, x, (3,); own=false)
    @test y == [429,500,503]

    @test unsafe_load(cglobal((:N_CURL_RETRY_CODES, AzStorage.libAzStorage), Cint)) == 7
    x = Sys.iswindows() ? unsafe_load(cglobal((:CURL_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clonglong})) : unsafe_load(cglobal((:CURL_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clong}))
    y = unsafe_wrap(Array, x, (7,); own=false)
    @test y == [6,7,28,35,42,55,56]
end

@testset "Containers, equivalent" begin
    x = AzContainer("foo/bar"; storageaccount="baz", nthreads=10, session=session)
    y = AzContainer("foo/bar"; storageaccount="baz", nthreads=11, session=session)
    z = AzContainer("foo/bar"; storageaccount="fiz", nthreads=11, session=session)
    w = AzContainer("foo/fiz"; storageaccount="baz", nthreads=11, session=session)
    @test x == y
    @test x != z
    @test x != w
end

@testset "Containers, list" begin
    r = uuid4()
    c = AzContainer("foo-$r-a", storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    write(c, "bar", "one")
    write(c, "baz", "two")

    l = readdir(c)

    @test "bar" ∈ l
    @test "baz" ∈ l

    containers(storageaccount=c.storageaccount, session=c.session)
    @test isdir(c)
    rm(c)
    @test !isdir(c)
end

@testset "Containers, prefix, list" begin
    r = uuid4()
    c = AzContainer("foo-$r-b", prefix="prefix", storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    write(c, "bar", "one")
    write(c, "baz", "two")

    l = readdir(c)

    @test "bar" ∈ l
    @test "baz" ∈ l

    l = readdir(c, filterlist=false)
    @test "prefix/bar" ∈ l
    @test "prefix/baz" ∈ l

    containers(storageaccount=c.storageaccount, session=c.session)
    @test isdir(c)
    rm(c)
    @test !isdir(c)
end

@testset "Containers, prefix, list, alt construction" begin
    r = uuid4()
    c = AzContainer("foo-$r-c/prefix", storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    write(c, "bar", "one")
    write(c, "baz", "two")

    l = readdir(c)

    @test "bar" ∈ l
    @test "baz" ∈ l

    l = readdir(c, filterlist=false)
    @test "prefix/bar" ∈ l
    @test "prefix/baz" ∈ l

    containers(storageaccount=c.storageaccount, session=c.session)
    @test isdir(c)
    rm(c)
    @test !isdir(c)
end

@testset "Containers, dirname" begin
    r = uuid4()
    c = AzContainer("foo-$r-d", storageaccount=storageaccount, session=session)
    @test dirname(c) == "foo-$r-d"
end

@testset "Containers, prefix, dirname" begin
    r = uuid4()
    c = AzContainer("foo-$r-e", prefix="prefix", storageaccount=storageaccount, session=session)
    @test dirname(c) == "foo-$r-e/prefix"
end

@testset "Containers, size, prefix=$prefix" for prefix in ("", "prefix")
    suffix = prefix == "" ? "-foo" : "-bar"
    r = uuid4()
    c = AzContainer("foo-$r-f$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    write(c, "bar", rand(UInt8,10))
    write(c, "baz", rand(UInt8,11))
    @test filesize(c, "bar") == 10
    @test filesize(c, "baz") == 11

    rm(c)
end

@testset "Containers, bytes, nthreads=$nthreads, prefix=$prefix" for nthreads in (1, 2), prefix in ("","prefix")
    suffix = prefix == "" ? "-foo" : "-bar"
    r = uuid4()
    c = AzContainer("foo-$r-g$suffix", prefix=prefix, storageaccount=storageaccount, session=session, nthreads=nthreads)
    c = robust_mkpath(c)

    N = 10
    if nthreads == 2
        N = round(Int, AzStorage._MINBYTES_PER_BLOCK * nthreads * 5 * (1+rand()) / 8)
        nblks = AzStorage.nblocks(nthreads, N*8)
        bytes_per_block = div(8*N, nblks) + (rem(8*N, nblks) == 0 ? 0 : 1)
        @info "bytes_per_block=$bytes_per_block, AzStorage._MAXBYTES_PER_BLOCK=$(AzStorage._MAXBYTES_PER_BLOCK)"
    end

    x = rand(N)
    write(c, "bar", x)
    y = read!(c, "bar", Vector{Float64}(undef, N))
    @test x ≈ y
    rm(c)
end

@testset "Containers, bytes, nested folder, prefix=$prefix" for prefix in ("","prefix")
    r = uuid4()
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-e$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    x = rand(10)
    write(c, "bar/baz", x)
    y = read!(c, "bar/baz", Vector{Float64}(undef, 10))
    @test x ≈ y
    rm(c)
end

@testset "Containers, string, prefix=$prefix" for prefix in ("", "prefix")
    r = uuid4()
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-f$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)

    write(c, "bar", "hello world\n")
    @test read(c, "bar", String) == "hello world\n"
    rm(c)
end

@testset "Containers, rm, prefix=$prefix" for prefix in ("prefix","")
    r = uuid4()
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-g$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)
    write(c, "bar", "bar\n")
    write(c, "foo", "foo\n")
    rm(c, "bar")
    @test readdir(c) == ["foo"]
    rm(c, "baz")
    @test readdir(c) == ["foo"]
    rm(c)
end

@testset "Containers, isfile, prefix=$prefix" for prefix in ("","prefix")
    r = uuid4()
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-h$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    c = robust_mkpath(c)
    write(c, "bar", "bar\n")
    @test isfile(c, "bar")
    write(c, "bar/baz", "bar\n")
    @test isfile(c, "bar/baz")
    @test !isfile(c, "notanobject")
    rm(c)
end

@testset "Containers, cp, prefix=$prefix" for prefix in ("", "prefix")
    r = uuid4()
    suffix = prefix == "" ? "-foo" : "-bar"
    src = AzContainer("foo-$r-i$suffix", storageaccount=storageaccount, session=session)
    r = uuid4()
    dst = AzContainer("foo-$r-j$suffix", storageaccount=storageaccount, session=session)

    c = robust_mkpath(src)
    write(src, "bar", "one")
    write(src, "baz", "two")

    cp(src, dst)

    @test read(dst, "bar", String) == "one"
    @test read(dst, "baz", String) == "two"

    rm(src)
    rm(dst)
end

@testset "Containers, json" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    _c = Container(AzContainer, JSON.parse(json(c)), c.session)
    @test _c.storageaccount == storageaccount
    @test _c.containername == "foo-$r-k"
    @test _c.nretry == 10
    if Sys.iswindows()
        @test _c.nthreads == 1
    else
        @test _c.nthreads == 2
    end
    rm(c)
end

@testset "Containers, serialization" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    x = (a=rand(10), b=rand(10))
    serialize(c, "bar", x)
    _x = deserialize(c, "bar")
    @test x.a ≈ _x.a
    @test x.b ≈ _x.b
    rm(c)
end

@testset "Containers, touch" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    touch(c, "bar")
    @test isfile(c, "bar")
    @test filesize(c, "bar") == 1 # we write a null charachter to the blob.
    rm(c)
end

@testset "Object, bytes" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    x = rand(10)
    write(io, x)
    _x = read!(io, zeros(10))
    rm(c)
    @test x ≈ _x
end

@testset "Object, bytes" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    write(io, "hello")
    x = read(io, String)
    rm(c)
    @test x == "hello"
end

@testset "Object, isfile" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(c)
end

@testset "Object, rm" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(io)
    @test !isfile(io)

    io = robust_open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(io; force=true)
    @test !isfile(io)

    rm(c)
end

@testset "Object, joinpath" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_joinpath(c, "bar", "baz")
    write(io, "hello")
    @test read(io, String) == "hello"
    rm(c)
end

@testset "Object, open" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    for (imode,mode) in enumerate(["r", "w", "w+", "r+"])
        io = open(robust_joinpath(c, "bar", "baz$imode"), mode)
        write(io, "hello")
        @test read(io, String) == "hello"
        rm(io)
    end
    io = open(robust_joinpath(c, "bar", "baz"))
    write(io, "hello")
    @test read(io, String) == "hello"
    rm(io)

    rm(c)
end

@testset "Object, serialization" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    x = (a=rand(10), b=rand(10))
    serialize(io, x)
    _x = deserialize(io)
    @test x.a ≈ _x.a
    @test x.b ≈ _x.b
end

@testset "Object, touch" begin
    r = uuid4()
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_joinpath(c, "bar")
    touch(io)
    @test isfile(io)
    @test filesize(io) == 1
    rm(c)
end

@testset "Object, number of blocks calculation" begin
    nthreads = 16
    @test AzStorage.nblocks(nthreads, nthreads*AzStorage._MAXBYTES_PER_BLOCK) == nthreads
    @test AzStorage.nblocks(nthreads, nthreads*AzStorage._MINBYTES_PER_BLOCK) == nthreads
    @test AzStorage.nblocks(nthreads, div(nthreads*AzStorage._MINBYTES_PER_BLOCK,2)) == div(nthreads, 2)
    @test AzStorage.nblocks(nthreads, 2*nthreads*AzStorage._MAXBYTES_PER_BLOCK) == 2*nthreads
    @test_throws ErrorException AzStorage.nblocks(nthreads, 2*AzStorage._MAXBYTES_PER_BLOCK*AzStorage._MAXBLOCKS_PER_BLOB)
end

# this failed because of a bug in the block-list when using exactly 10 blocks
# Anusha found the failing example.
@testset "Anusha's example" begin
    r = uuid4()
    c = AzContainer("foo-$r-l", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    x = rand(2801,13821)
    write(c, "bar", x)
    _x = read!(c, "bar", Array{Float64,2}(undef,2801,13821))
    @test x ≈ _x
    rm(c)
end

@testset "writedlm and readdlm" begin
    a = rand(1000,1000)
    r = uuid4()
    c = AzContainer("foo-$r-m", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = robust_open(c, "bar")
    writedlm(io,a)
    _a = readdlm(io)
    @test _a ≈ a
    rm(c)
end 

@testset "Containers, bytes, SubArray" begin
    a = rand(10,20,3)
    r = uuid4()
    c = AzContainer("foo-$r-n", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    for i = 1:3
        write(c, "bar$i", @view a[:,:,i])
    end
    _a = zeros(10,20,3)
    for i = 1:3
        read!(c, "bar$i", @view _a[:,:,i])
    end
    @test a ≈ _a
    rm(c)
end

@testset "Containers, bytes, non-contiguous throws" begin
    a = rand(10,20)
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    @test_throws ErrorException write(c, "bar", @view a[1:2:end,1:2:end])
    rm(c)
end

@testset "Container, minimal dictionary" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    _c = minimaldict(c)
    @test _c["storageaccount"] == storageaccount
    @test _c["containername"] == "foo-$r-o"
    @test _c["prefix"] == ""
    @test length(_c) == 3
end

@testset "Container, copy blob to local file" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(c, "foo.txt", "Hello world")
    cp(c, "foo.txt", "foolocal.txt")
    @test read("foolocal.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Container, copy local file to blob" begin
    write("foolocal.txt", "Hello world")
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    cp("foolocal.txt", c, "foo.txt")
    @test read(c, "foo.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Container, copy blob to blob" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(c, "foo.txt", "Hello world")
    cp(c, "foo.txt", c, "bar.txt")
    @test read(c, "bar.txt", String) == "Hello world"
    rm(c)
end

@testset "Object, copy blob to local file" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(robust_open(c, "foo.txt"), "Hello world")
    cp(robust_open(c, "foo.txt"), "foolocal.txt")
    @test read("foolocal.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Object, copy local file to blob" begin
    write("foolocal.txt", "Hello world")
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    cp("foolocal.txt", robust_open(c, "foo.txt"))
    @test read(robust_open(c, "foo.txt"), String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Object, copy blob to blob" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(robust_open(c, "foo.txt"), "Hello world")
    cp(robust_open(c, "foo.txt"), robust_open(c, "bar.txt"))
    @test read(robust_open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Object,Container, copy blob to blob" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(robust_open(c, "foo.txt"), "Hello world")
    cp(robust_open(c, "foo.txt"), c, "bar.txt")
    @test read(robust_open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Container,Object, copy blob to blob" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(robust_open(c, "foo.txt"), "Hello world")
    cp(c, "foo.txt", robust_open(c, "bar.txt"))
    @test read(robust_open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Windows, single thread check" begin
    r = uuid4()
    container = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    if Sys.iswindows()
        @test container.nthreads == 1
    else
        @test container.nthreads == 2
    end
end

@testset "timeouts" begin
    r = uuid4()
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nretry=0, connect_timeout=2, read_timeout=3)
    @test c.connect_timeout == 2
    @test c.read_timeout == 3
end

@testset "redundant putblocklist" begin
    #=
    Test special handling for 400 errors with "InvalidBlockList" error code.
    We think there is a race condition that can cause putblocklist to
    be called twice, and putblocklist is not idempotent.
    =#
    r = uuid4()
    x = rand(UInt8, 500_000)
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    write(robust_open(c, "foo.bin"), x)

    _nblocks = AzStorage.nblocks(c.nthreads, length(x))
    l = ceil(Int, log10(_nblocks))
    blockids = [base64encode(lpad(blockid-1, l, '0')) for blockid in 1:_nblocks]
    AzStorage.putblocklist(c, "foo.bin", blockids) # simulate the race, by putting the block list again
    _x = read!(robust_open(c, "foo.bin"), Vector{UInt8}(undef, 500_000))
    @test _x == x
    rm(c)
end

@testset "backend" begin
    r = uuid4()
    x = rand(UInt8, 500_000)
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)

    @test backend(c) == "azureblob"
end

if !Sys.iswindows()
    @testset "C token refresh, write" begin
        r = uuid4()
        c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=4, connect_timeout=2, read_timeout=3)
        c = robust_mkpath(c)
        o = "foo.bin"

        nthreads = c.nthreads

        N = round(Int, AzStorage._MINBYTES_PER_BLOCK * nthreads * 5)
        data = rand(UInt8, N)

        _nblocks = AzStorage.nblocks(nthreads, length(data))

        l = ceil(Int, log10(_nblocks))
        blockids = [base64encode(lpad(blockid-1, l, '0')) for blockid in 1:_nblocks]
        _blockids = [HTTP.escapeuri(blockid) for blockid in blockids]
        t = token(c.session; offset=Minute(10))

        c.session.expiry = now() # force a token refresh in the C code

        _token,refresh_token,expiry,scope,resource,tenant,clientid,client_secret = AzStorage.authinfo(c.session)

        r = @ccall libAzStorage.curl_writebytes_block_retry_threaded(_token::Ptr{UInt8}, refresh_token::Ptr{UInt8}, expiry::Ptr{Culong}, scope::Cstring, resource::Cstring, tenant::Cstring,
            clientid::Cstring, client_secret::Cstring,c.storageaccount::Cstring, c.containername::Cstring, AzStorage.addprefix(c,o)::Cstring, _blockids::Ptr{Cstring}, data::Ptr{UInt8},
            length(data)::Csize_t, c.nthreads::Cint, _nblocks::Cint, c.nretry::Cint, c.verbose::Cint, c.connect_timeout::Clong, c.read_timeout::Clong)::AzStorage.ResponseCodes

        AzStorage.authinfo!(c.session, _token, refresh_token, expiry)
        @test t != c.session.token

        AzStorage.putblocklist(c, o, blockids)

        _data = read!(c, o, Vector{UInt8}(undef, N))
        @test data ≈ _data
    end

    @testset "C token refresh, read" begin
        r = uuid4()
        c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=4, connect_timeout=2, read_timeout=3)
        c = robust_mkpath(c)
        o = "foo.bin"

        nthreads = c.nthreads

        N = round(Int, AzStorage._MINBYTES_PER_BLOCK * nthreads * 5)
        data = rand(UInt8, N)

        write(c, o, data)

        t = token(c.session; offset=Minute(10))

        _nthreads = AzStorage.nthreads_effective(c.nthreads, length(data))
        offset = 0

        c.session.expiry = now()

        _token,refresh_token,expiry,scope,resource,tenant,clientid,client_secret = AzStorage.authinfo(c.session)

        _data = Vector{UInt8}(undef, N)

        r = @ccall libAzStorage.curl_readbytes_retry_threaded(_token::Ptr{UInt8}, refresh_token::Ptr{UInt8}, expiry::Ptr{Culong}, scope::Cstring, resource::Cstring, tenant::Cstring,
            clientid::Cstring, client_secret::Cstring, c.storageaccount::Cstring, c.containername::Cstring, AzStorage.addprefix(c,o)::Cstring, _data::Ptr{UInt8}, offset::Csize_t,
            length(data)::Csize_t, _nthreads::Cint, c.nretry::Cint, c.verbose::Cint, c.connect_timeout::Clong, c.read_timeout::Clong)::AzStorage.ResponseCodes

        AzStorage.authinfo!(c.session, _token, refresh_token, expiry)

        @test data ≈ _data
    end

    @testset "performance counters" begin
        perfcounters = AzStorage.getperf_counters()
        @info "Throttled events = $(perfcounters.count_throttled), Throttled ms wait: = $(perfcounters.ms_wait_throttled)"

        @test isa(perfcounters.ms_wait_throttled, Clonglong)
        @test isa(perfcounters.ms_wait_timeouts, Clonglong)
        @test isa(perfcounters.count_throttled, Clonglong)
        @test isa(perfcounters.count_timeouts, Clonglong)

        AzStorage.resetperf_counters()
        AzStorage.getperf_counters()
        @test perfcounters.ms_wait_throttled == 0
        @test perfcounters.ms_wait_timeouts == 0
        @test perfcounters.count_throttled == 0
        @test perfcounters.count_timeouts == 0
    end

end