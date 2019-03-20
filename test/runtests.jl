using AbstractStorage, AzSessions, AzStorage, Dates, JSON, Random, Test

session = AzSession(read(joinpath(homedir(),"session.json"), String))
storageaccount = ENV["STORAGE_ACCOUNT"]
@info "storageaccount=$storageaccount"

for container in  containers(;storageaccount=storageaccount,session=session)
    rm(AzContainer(container;storageaccount=storageaccount,session=session))
end
@info "sleeping for 60 seconds to ensure Azure clean-up from any previous run"
sleep(60)

@testset "Containers, list" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+0)))
    c = AzContainer("foo-$r-a", storageaccount=storageaccount, session=session)
    mkpath(c)

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
    mkpath(c)

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
    mkpath(c)

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
    mkpath(c)

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
    mkpath(c)

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
    mkpath(c)

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
    mkpath(c)

    write(c, "bar", "hello world\n")
    @test read(c, "bar", String) == "hello world\n"
    rm(c)
end

@testset "Containers, rm, prefix=$prefix" for prefix in ("prefix","")
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+9)))
    suffix = prefix == "" ? "-foo" : "-bar"
    c = AzContainer("foo-$r-g$suffix", prefix=prefix, storageaccount=storageaccount, session=session)
    mkpath(c)
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
    mkpath(c)
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

    mkpath(src)
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
    mkpath(c)
    _c = Container(AzContainer, JSON.parse(json(c)), c.session)
    @test _c.storageaccount == storageaccount
    @test _c.containername == "foo-$r-k"
    @test _c.nretry == 10
    @test _c.nthreads == 2
    rm(c)
end

@testset "Object, bytes" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
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
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    write(io, "hello")
    x = read(io, String)
    rm(c)
    @test x == "hello"
end

@testset "Object, isfile" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = open(c, "bar")
    write(io, "hello")
    @test isfile(io)
    rm(c)
end

@testset "Object, rm" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
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
    r = lowercase(randstring(MersenneTwister(millisecond(now())+13)))
    c = AzContainer("foo-$r-k", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    io = joinpath(c, "bar", "baz")
    write(io, "hello")
    @test read(io, String) == "hello"
    rm(c)
end

# this failed because of a bug in the block-list when using exactly 10 blocks
# Anusha found the failing example.
@testset "Anusha's example" begin
    sleep(1)
    r = lowercase(randstring(MersenneTwister(millisecond(now())+14)))
    c = AzContainer("foo-$r-l", storageaccount=storageaccount, session=session, nthreads=2, nretry=10)
    mkpath(c)
    x = rand(2801,13821)
    write(c, "bar", x)
    _x = read!(c, "bar", Array{Float64,2}(undef,2801,13821))
    @test x ≈ _x
    rm(c)
end
