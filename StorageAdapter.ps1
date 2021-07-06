using module .\IndexedCollection.psm1

Import-Module .\IndexedCollection.psm1 -Force

remove-item "cache" -erroraction silentlycontinue
$thisStorageAdapter = [CacheableFileStorageAdapter]::new("cache")

start-sleep -seconds 2

$Adapters = @(
    [CacheableFileStorageAdapter]::new("Cache")
    [StorageAdapter]::new()
    [FileStorageAdapter]::new("file")
    [MemoryStorageAdapter]::new()
)

$item = "is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum." * 100

foreach ($Adapter in $Adapters) {

    $AdapterName = $Adapter.GetType().FullName
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

    $one = $stopwatch.elapsedmilliseconds

    $Adapter.Set($item)
    
    $two = $stopwatch.elapsedmilliseconds - $one

    [void] ( $Adapter.Get() )
    $Three = $stopwatch.elapsedmilliseconds - $two
    
    [void] ( $Adapter.GetJsonAsType([string]) )
    $Four = $stopwatch.elapsedmilliseconds - $Three

    write-host "$AdapterName took $two ms to write and $three ms to read a cache miss and $four ms to read a cache hit"
}
<#
ConvertTo-Json ($thisStorageAdapter.GetStatistics())

write-host "Get Object 1"
$thisStorageAdapter.Get()
ConvertTo-Json ($thisStorageAdapter.GetStatistics())

write-host "Get Object 2"
$ThisStorageAdapter.Get()
ConvertTo-Json ($thisStorageAdapter.GetStatistics())

write-host "Set Object 1"
$ThisStorageAdapter.Set("potato Pancakes")
ConvertTo-Json ($thisStorageAdapter.GetStatistics())

write-host "Get Object 3"
$ThisStorageAdapter.Get()
ConvertTo-Json ($thisStorageAdapter.GetStatistics())
#>
<#
$thisStorageAdapter = [DiskStorageAdapter]::new("persistent.store")

write-host ""
write-host "Test Persistence"
$thisStorageAdapter.GetPath()
$thisStorageAdapter.Set("tomato")
$thisStorageAdapter.Get()

get-content -raw "persistent.store"

write-host ""
write-host "Test Disabling Auto Flush"
$thisStorageAdapter.DisableAutoFlush()
$thisStorageAdapter.Set("potato")
$thisStorageAdapter.Get()
write-host ""
write-host "Test manual flush"
get-content -raw "persistent.store"
$thisStorageAdapter.flush()
get-content -raw "persistent.store"

$thisStorageAdapter.isLoaded

$thisStorageAdapter.Unload()

$thisStorageAdapter.isLoaded

$thisStorageAdapter.Get()

$thisStorageAdapter.isLoaded

write-host "Collection Tests"

$NewIndexedCollection = [IndexedCollection]::new([string])
$NewIndexedCollection.GetNextEntryIndex()
#>

#Load-BackedIndexCollection -Path "."
<#
write-host ""
write-host "Generating Objects"
measure-command {
$Objects = @()

$Names = @("Jamie", "Gus", "Sharonda", "Nicholas", "Rafael", "Coy", "Chrystal", "Normand", "Jillian", "Fatimah", "Katina", "Kenyetta", "Linnie", "Catina", "Elouise", "Marx", "Julianne", "Pauletta", "Hana", "Michelina")
foreach ($i in 1..1000) {
    if ($i -eq 1) {
        $Objects += @{
            "first name" = "bill"
            "last name"  = ($Names | get-random)
            "height"     = (get-random -minimum 1 -maximum 100)
        }
    } else {
        $Objects += @{
            "first name" = ($Names | get-random)
            "last name"  = ($Names | get-random)
            "height"     = (get-random -minimum 1 -maximum 100)
        }
    }
}


}
write-host ""
write-host "Compiling Collection"
$timer = measure-command {
$Result = New-IndexedCollection -InputCollection $Objects -Index @("first name")
}
write-host "$($Timer.TotalMilliseconds)"

write-host ""
write-host "Get by Magic Collection"
$timer = measure-command {
$Bill = $result.GetByIndexAndValue("first name", "bill")
}
write-host "$($Timer.TotalMilliseconds)"

write-host ""
write-host "Get by normal means"
$timer = measure-command {
    $Objects | Where-Object { $_."first name" -eq "bill" }
}
write-host "$($Timer.TotalMilliseconds)"

$BIll
<#
$DiskStorageAdapter = [DiskStorageAdapter]::new("collection.store")
$GenericStorageAdapter = ([StorageAdapter] $DiskStorageAdapter)
$Type = [Hashtable]
$NewBackedIndexedCollection = [BackedIndexedCollection]::new($GenericStorageAdapter, $Type)
$NewBackedIndexedCollection.AddIndex("first name")
$NewBackedIndexedCollection.AddIndex("last name")
$NewBackedIndexedCollection.AddIndex("height")

$Names = @("Jamie", "Gus", "Sharonda", "Nicholas", "Rafael", "Coy", "Chrystal", "Normand", "Jillian", "Fatimah", "Katina", "Kenyetta", "Linnie", "Catina", "Elouise", "Marx", "Julianne", "Pauletta", "Hana", "Michelina")
foreach ($i in 1..10) {
    $ItemId = $NewBackedIndexedCollection.GetNextEntryIndex()
    $Path = "$itemId.json"
    if ($i -eq 1) {
        $MyObject = @{
            "first name" = "bill"
            "last name"  = ($Names | get-random)
            "height"     = (get-random -minimum 1 -maximum 100)
        }
    } else {
        $MyObject = @{
            "first name" = ($Names | get-random)
            "last name"  = ($Names | get-random)
            "height"     = (get-random -minimum 1 -maximum 100)
        }
    }

    $ThisStorageAdapter = [DiskStorageAdapter]::new($Path)

    $NewCollectionMember = [BackedCollectionMember]::New($ThisStorageAdapter, $itemId, $MyObject , $Type)

    $NewBackedIndexedCollection.Add($NewCollectionMember)
    [void] ($NewBackedIndexedCollection.GetAll())
}
$NewBackedIndexedCollection.GetByIndexAndValue("first name", "bill")
$NewBackedIndexedCollection.GetByIndexAndValue("first name", "bill")
$NewBackedIndexedCollection.GetByIndexAndValue("first name", "bill")
$NewBackedIndexedCollection.GetByIndexAndValue("first name", "bill")

write-host ""
write-host "Steady"
$NewBackedIndexedCollection.GetMemoryReport()

write-host ""
write-host "UnloadAll"
$NewBackedIndexedCollection.UnloadAll()

$NewBackedIndexedCollection.GetMemoryReport()

$NewBackedIndexedCollection.Flush()
write-host ""
write-host "Warming"
$NewBackedIndexedCollection.PreWarm()

$NewBackedIndexedCollection.GetMemoryReport()
write-host ""
write-host "flushing"
$NewBackedIndexedCollection.Flush()

$NewBackedIndexedCollection.GetMemoryReport()
write-host ""
write-host "other"
$NewBackedIndexedCollection.GetNextEntryIndex()

$NewBackedIndexedCollection.EnableAutoFlush()
$NewBackedIndexedCollection.DisableAutoFlush()

$NewBackedIndexedCollection.GetMemoryReport()
#>#>

