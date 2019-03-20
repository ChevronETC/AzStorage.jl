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
filesize
isfile
joinpath
open
read
read!
rm(::AzContainer, ::AbstractString)
rm(::AzStorage.AzObject)
write
```