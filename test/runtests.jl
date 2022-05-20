using AbstractStorage, AzSessions, AzStorage, Dates, JSON, Random, Serialization, Test

function robust_mkpath(c)
    for itry = 1:10
        try
            mkpath(c)
            break
        catch e
            if isa(e, HTTP.ExceptionRequest.StatusError) && e.status == 409
                @warn showerror(stdout, e)
                r = lowercase(randstring('a':'z'))
                c = AzContainer("foo-$r-o"; storageaccount=c.storageaccount, session=c.session, nthreads=2, nretry=10)
            else
                throw(e)
            end
        end
        if itry == 10
            throw(e)
        end
    end
    c
end

credentials = JSON.parse(ENV["AZURE_CREDENTIALS"])
AzSessions.write_manifest(;client_id=credentials["clientId"], client_secret=credentials["clientSecret"], tenant=credentials["tenantId"])

session = AzSession(;protocal=AzClientCredentials, client_id=credentials["clientId"], client_secret=credentials["clientSecret"], resource="https://storage.azure.com/")

storageaccount = ENV["STORAGE_ACCOUNT"]
@info "storageaccount=$storageaccount"

for container in containers(;storageaccount=storageaccount,session=session)
    rm(AzContainer(container;storageaccount=storageaccount,session=session))
end
@info "sleeping for 60 seconds to ensure Azure clean-up from any previous run"
sleep(60)

@testset "Error codes" begin
    @test unsafe_load(cglobal((:N_HTTP_RETRY_CODES, AzStorage.libAzStorage), Cint)) == 2
    x = Sys.iswindows() ? unsafe_load(cglobal((:HTTP_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clonglong})) : unsafe_load(cglobal((:HTTP_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clong}))
    y = unsafe_wrap(Array, x, (2,); own=false)
    @test y == [500,503]

    @test unsafe_load(cglobal((:N_CURL_RETRY_CODES, AzStorage.libAzStorage), Cint)) == 6
    x = Sys.iswindows() ? unsafe_load(cglobal((:CURL_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clonglong})) : unsafe_load(cglobal((:CURL_RETRY_CODES, AzStorage.libAzStorage), Ptr{Clong}))
    y = unsafe_wrap(Array, x, (6,); own=false)
    @test y == [6,7,28,35,55,56]
end

@testset "Containers, equivalent" begin
    x = AzContainer("foo/bar"; storageaccount="baz", nthreads=10)
    y = AzContainer("foo/bar"; storageaccount="baz", nthreads=11)
    z = AzContainer("foo/bar"; storageaccount="fiz", nthreads=11)
    w = AzContainer("foo/fiz"; storageaccount="baz", nthreads=11)
    @test x == y
    @test x != z
    @test x != w
end

@testset "Containers, list" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+0)))
    c = AzContainer("foo-$r-a", storageaccount=storageaccount, session=session)
    robust_mkpath(c)

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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+1)))
    c = AzContainer("foo-$r-b", prefix="prefix", storageaccount=storageaccount, session=session)
    robust_mkpath(c)

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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+2)))
    c = AzContainer("foo-$r-c/prefix", storageaccount=storageaccount, session=session)
    robust_mkpath(c)

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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+3)))
    c = AzContainer("foo-$r-d", storageaccount=storageaccount, session=session)
    @test dirname(c) == "foo-$r-d"
end

@testset "Containers, prefix, dirname" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+4)))
    c = AzContainer("foo-$r-e", prefix="prefix", storageaccount=storageaccount, session=session)
    @test dirname(c) == "foo-$r-e/prefix"
end

@testset "Containers, size, prefix=$prefix" for prefix in ("", "prefix")
    sleep(1)
    suffix = prefix == "" ? "-foo" : "-bar"
    r = lowercase(randstring(MersenneTwister(millisecond(now())+5)))
    c = AzContainer("foo-$r-f$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    robust_mkpath(c)

    write(c, "bar", rand(UInt8,10))
    write(c, "baz", rand(UInt8,11))
    @test filesize(c, "bar") == 10
    @test filesize(c, "baz") == 11

    rm(c)
end

@testset "Containers, bytes, nthreads=$nthreads, prefix=$prefix" for nthreads in (1, 2), prefix in ("","prefix")
    sleep(1)
    suffix = prefix == "" ? "-foo" : "-bar"
    r = lowercase(randstring(MersenneTwister(millisecond(now())+6)))
    c = AzContainer("foo-$r-g$suffix", prefix=prefix, storageaccount=storageaccount, session=session, nthreads=nthreads)
    robust_mkpath(c)

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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+7)))
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-e$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    robust_mkpath(c)

    x = rand(10)
    write(c, "bar/baz", x)
    y = read!(c, "bar/baz", Vector{Float64}(undef, 10))
    @test x ≈ y
    rm(c)
end

@testset "Containers, string, prefix=$prefix" for prefix in ("", "prefix")
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+8)))
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-f$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    robust_mkpath(c)

    write(c, "bar", "hello world\n")
    @test read(c, "bar", String) == "hello world\n"
    rm(c)
end

@testset "Containers, rm, prefix=$prefix" for prefix in ("prefix","")
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+9)))
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-g$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    robust_mkpath(c)
    write(c, "bar", "bar\n")
    write(c, "foo", "foo\n")
    rm(c, "bar")
    @test readdir(c) == ["foo"]
    rm(c, "baz")
    @test readdir(c) == ["foo"]
    rm(c)
end

@testset "Containers, isfile, prefix=$prefix" for prefix in ("","prefix")
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+10)))
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-h$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    robust_mkpath(c)
    write(c, "bar", "bar\n")
    @test isfile(c, "bar")
    write(c, "bar/baz", "bar\n")
    @test isfile(c, "bar/baz")
    @test !isfile(c, "notanobject")
    rm(c)
end

@testset "Containers, cp, prefix=$prefix" for prefix in ("", "prefix")
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+11)))
    suffix = prefix == "" ? "-foo" : "-bar"
    src = AzContainer("foo-$r-i$suffix", storageaccount=storageaccount, session=session)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+12)))
    dst = AzContainer("foo-$r-j$suffix", storageaccount=storageaccount, session=session)

    robust_mkpath(src)
    write(src, "bar", "one")
    write(src, "baz", "two")

    cp(src, dst)

    @test read(dst, "bar", String) == "one"
    @test read(dst, "baz", String) == "two"

    rm(src)
    rm(dst)
end

@testset "Containers, json" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+14)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    x = (a=rand(10), b=rand(10))
    serialize(c, "bar", x)
    _x = deserialize(c, "bar")
    @test x.a ≈ _x.a
    @test x.b ≈ _x.b
    rm(c)
end

@test "Containers, touch" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+14)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    touch(c, "bar")
    @test isfile(c, "bar")
    @test filesize(c, "bar") == 0
    rm(c)
end

@testset "Object, bytes" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+15)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    x = rand(10)
    write(io, x)
    _x = read!(io, zeros(10))
    rm(c)
    @test x ≈ _x
end

@testset "Object, bytes" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+16)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    write(io, "hello")
    x = read(io, String)
    rm(c)
    @test x == "hello"
end

@testset "Object, isfile" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+17)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(c)
end

@testset "Object, rm" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+18)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(io)
    @test !isfile(io)
    rm(c)
end

@testset "Object, joinpath" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+19)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = joinpath(c, "bar", "baz")
    write(io, "hello")
    @test read(io, String) == "hello"
    rm(c)
end

@testset "Object, serialization" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+20)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    x = (a=rand(10), b=rand(10))
    serialize(io, x)
    _x = deserialize(io)
    @test x.a ≈ _x.a
    @test x.b ≈ _x.b
end

@testset "Object, touch" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+20)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = AzObject(c, "bar")
    touch(io)
    @test isfile(io)
    @test filesize(io) == 0
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
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+21)))
    c = AzContainer("foo-$r-l", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    x = rand(2801,13821)
    write(c, "bar", x)
    _x = read!(c, "bar", Array{Float64,2}(undef,2801,13821))
    @test x ≈ _x
    rm(c)
end

@testset "writedlm and readdlm" begin
    sleep(1)
    a = rand(1000,1000)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+22)))
    c = AzContainer("foo-$r-m", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    writedlm(io,a)
    _a = readdlm(io)
    @test _a ≈ a
    rm(c)
end 

@testset "Containers, bytes, SubArray" begin
    sleep(1)
    a = rand(10,20,3)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+23)))
    c = AzContainer("foo-$r-n", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
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
    sleep(1)
    a = rand(10,20)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+24)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    @test_throws ErrorException write(c, "bar", @view a[1:2:end,1:2:end])
    rm(c)
end

@testset "Container, minimal dictionary" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+25)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    _c = minimaldict(c)
    @test _c["storageaccount"] == storageaccount
    @test _c["containername"] == "foo-$r-o"
    @test _c["prefix"] == ""
    @test length(_c) == 3
end

@testset "Container, copy blob to local file" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+26)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(c, "foo.txt", "Hello world")
    cp(c, "foo.txt", "foolocal.txt")
    @test read("foolocal.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Container, copy local file to blob" begin
    sleep(1)
    write("foolocal.txt", "Hello world")
    r = lowercase(randstring(MersenneTwister(millisecond(now())+27)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    cp("foolocal.txt", c, "foo.txt")
    @test read(c, "foo.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Container, copy blob to blob" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+28)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(c, "foo.txt", "Hello world")
    cp(c, "foo.txt", c, "bar.txt")
    @test read(c, "bar.txt", String) == "Hello world"
    rm(c)
end

@testset "Object, copy blob to local file" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+29)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    c = robust_mkpath(c)
    write(open(c, "foo.txt"), "Hello world")
    cp(open(c, "foo.txt"), "foolocal.txt")
    @test read("foolocal.txt", String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Object, copy local file to blob" begin
    sleep(1)
    write("foolocal.txt", "Hello world")
    r = lowercase(randstring(MersenneTwister(millisecond(now())+30)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    cp("foolocal.txt", open(c, "foo.txt"))
    @test read(open(c, "foo.txt"), String) == "Hello world"
    rm(c)
    rm("foolocal.txt")
end

@testset "Object, copy blob to blob" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+31)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(open(c, "foo.txt"), "Hello world")
    cp(open(c, "foo.txt"), open(c, "bar.txt"))
    @test read(open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Object,Container, copy blob to blob" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+32)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(open(c, "foo.txt"), "Hello world")
    cp(open(c, "foo.txt"), c, "bar.txt")
    @test read(open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Container,Object, copy blob to blob" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+33)))
    c = AzContainer("foo-$r-o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    robust_mkpath(c)
    write(open(c, "foo.txt"), "Hello world")
    cp(c, "foo.txt", open(c, "bar.txt"))
    @test read(open(c, "bar.txt"), String) == "Hello world"
    rm(c)
end

@testset "Windows, single thread check" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+34)))
    container = AzContainer("foo-$r.o", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    if Sys.iswindows()
        @test container.nthreads == 1
    else
        @test container.nthreads == 2
    end
end
