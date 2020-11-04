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
readdlm
rm(::AzContainer)
writedlm
```

## Blob methods
```@docs
filesize
isfile
joinpath
open
read
read!
readdlm
rm(::AzContainer, ::AbstractString)
rm(::AzStorage.AzObject)
write
writedlm
```