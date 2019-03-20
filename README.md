# AzStorage
Interface to Azure blob storage.

```julia
using AzSessions, AzStorage

mysession = AzCCSession(client_id="myclientid", client_secret="myclientsecret", resource="https://storage.azure.com")
container = AzContainer("foo", storageaccount="myaccount", session=mysession)

mkpath(container)
write(container, "blob", rand(10))

readdir(container)

x = read!(container, "blob", Vector{Float64}(undef, 10))

...
```
