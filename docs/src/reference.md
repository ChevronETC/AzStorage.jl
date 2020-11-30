# Reference

## Construct an Azure Container
```@docs
AzContainer
```

## Container methods
```@docs
containers
cp
dirname
isdir
mkpath
readdir
rm(::AzContainer)
```

## Blob methods
```@docs
deserialize
filesize
isfile
joinpath
open
read
read!
readdlm
rm(::AzContainer, ::AbstractString)
rm(::AzStorage.AzObject)
serialize
write
writedlm
```
