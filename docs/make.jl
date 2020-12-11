using Documenter, AzStorage, Serialization

makedocs(sitename="AzStorage", modules=[AzStorage])

deploydocs(
    repo = "github.com/ChevronETC/AzStorage.jl.git",
)