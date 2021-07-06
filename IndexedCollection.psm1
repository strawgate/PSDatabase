$script:jsonMetaDataSerializerSettings = [Newtonsoft.Json.JsonSerializerSettings]::new()
 
$script:jsonMetaDataSerializerSettings.Formatting = [Newtonsoft.Json.Formatting]::Indented
$script:jsonMetaDataSerializerSettings.NullValueHandling  = [Newtonsoft.Json.NullValueHandling]::Ignore
$script:jsonMetaDataSerializerSettings.TypeNameHandling = [Newtonsoft.Json.TypeNameHandling]::Arrays

$script:jsonDataFileSerializerSettings = [Newtonsoft.Json.JsonSerializerSettings]::new()
 
$script:jsonDataFileSerializerSettings.Formatting = [Newtonsoft.Json.Formatting]::Indented
$script:jsonDataFileSerializerSettings.NullValueHandling  = [Newtonsoft.Json.NullValueHandling]::Ignore
$script:jsonDataFileSerializerSettings.TypeNameHandling = [Newtonsoft.Json.TypeNameHandling]::None


### Storage Adapters are a generic way to connect an object to a persistence store.
### The storage adapter keeps some metrics while its alive for diagnostics.
class IStorageAdapter {

    [int] $Reads
    [int] $Writes

    [scriptblock] $Serializer
    [scriptblock] $Deserializer

    IStorageAdapter () {}

    [object] GetStatistics () { throw "Do not call methods on an interface." }
    [object] Get() { throw "Do not call methods on an interface." }
    [void] Set() { throw "Do not call methods on an interface." }
}

class StorageAdapter : IStorageAdapter {

    [int] $Reads = 0
    [int] $Writes = 0

    [scriptblock] $Serializer = { param ($rawdata) write-output $rawdata }
    [scriptblock] $Deserializer
    
    StorageAdapter ( ) {}

    [hashtable] GetStatistics () {
        return @{
            "Reads" = $this.Reads
            "Writes" = $this.Writes
        }
    }

    [void] UseJsonSerializer() {
        $this.Serializer += {
            param (
                $Data
            )

            return (convertto-json $data)
        }
    }
    
    [void] UseJsonDeserializer() {
        $this.Deserializer += {
            param (
                $Data
            )

            return (ConvertFrom-Json $data)
        }
    }
    
    [bool] HasDeserializer() {
        return $this.Deserializer -ne $null
    }
    [bool] HasSerializer() {
        return $this.Serializer -ne $null
    }

    [object] InvokeDeserializer([object] $Item) {
        if ($this.HasDeserializer()) {
            return & $this.Deserializer -Data $Item
        }
        return $Item
    }

    [object] InvokeSerializer([object] $Object) {
        if ($this.HasSerializer()) {
            return & $this.Serializer -Data $Object
        }
        return $Object
    }

    [object] Get() { return $null}
    [void] Set([Object] $Data) { }
}

## This storage adapter keeps objects in memory for persistence. Reads and writes to and from memory storage
# are just reads and writes to and from memory
class MemoryStorageAdapter : StorageAdapter {

    [int] $Reads = 0
    [int] $Writes = 0

    [object] $Data

    MemoryStorageAdapter () : Base () {}

    [object] Get() {
        $this.Reads ++
        $object = $this.InvokeDeserializer($this.Data)
        return $object
    }

    [void] Set([object] $Data) {
        $this.Writes ++
        $object = $this.InvokeSerializer($Data)
        $this.Data = $Object
    }
}

## This storage adapter uses a file for its backing source. This means all gets are direct Disk I/O and all sets are direct Disk I/O
# This is useful if you want the same behavior as the MemoryStorageHelper but dont want to have to worry about data persistence
# For performance use the CacheableFileStorageAdapter

class FileStorageAdapter : StorageAdapter {

    [System.IO.FileInfo] $File

    FileStorageAdapter ([string] $Path) : Base () {
        if (-not (test-path $Path)) {
            new-item -itemtype file -path $Path -value "null"
        }

        $this.File = Get-Item $Path
    }

    [object] Get() {
        $this.Reads ++
        $raw =  (Get-Content -raw $this.File)
        $object = $this.InvokeDeserializer($raw)
        return $object
    }

    [void] Set([object] $Data) {
        $this.Writes ++
        $object = $this.InvokeSerializer($Data)
        Set-Content -value $Object -Path $this.File.FullName
    }
}

## This class is a caching version of the FileStorageAdapter
#
# Reads:
# The first time an object is read it is read into a memory cache, further reads are performed from the memory cache when possible. The cache
# can be unloaded using Unload().
#
# Writes:
# Writes are done to the in memory cache first and then flushed to disk. If AutoFlush is enabled this occurs every time a write is performed.
# If AutoFlush is disabled this will not occur. With Autoflush disabled A Get() may return a different value than what's on disk. 
# You will have to manually call flush to achieve data persistence.
#
# Statistics:
# This class also extends the statistics available for the storage adapter to include cache hits and misses
class CacheableFileStorageAdapter : FileStorageAdapter {
    [object] $Cache

    [object] $CacheAsType

    [bool] $IsLoaded = $false
    [bool] $IsPersisted = $false

    [int] $CacheHits = 0
    [int] $CacheMisses = 0
    [int] $CacheClears = 0

    [bool] $AutoFlush = $true

    CacheableFileStorageAdapter ([string] $Path) : Base ($Path) {}

    [hashtable] GetStatistics () {
        return @{
            "Reads" = $this.Reads
            "Writes" = $this.Writes
            "Cache Hits" = $this.CacheHits
            "Cache Misses" = $this.CacheMisses
            "Cache Clears" = $this.CacheClears
        }
    }
    
    [void] SetCache([object] $Data) {
        $this.Cache = $Data
        $this.CacheAsType = $null
        $this.IsPersisted = $false
        $this.IsLoaded = $true
    }

    [object] GetCache() {
        if ($this.isLoaded) {
            return $this.Cache
        }
        return $null
    }

    [void] ClearCache() {
        $this.Cache = $null
        $this.IsLoaded = $false
        $this.IsPersisted = $false
        $this.CacheClears++
    }

    [void] SetAutoFlush([bool] $enable) {
        $this.AutoFlush = $Enable
    }

    [void] Flush () {
        if (-not ($this.IsPersisted)) {
            ([FileStorageAdapter] $this).Set($this.GetCache())
            $this.IsPersisted = $true
        }
    }

    [void] Set ([Object] $Data) {
        $this.SetCache($Data)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }
    
    [void] SetObjectAsJson ([object] $Object) {
        $json = [Newtonsoft.Json.JsonConvert]::SerializeObject($Object, $This.Type, $script:jsonDataFileSerializerSettings)
        $this.SetCache($json)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }

    [object] GetJsonAsType([Type] $Type) {
        if ($this.CacheAsType -eq $null -or $this.CacheAsType.GetType() -isnot $Type) {
            $object = ([FileStorageAdapter] $this).GetJsonAsType($Type)
            $this.CacheAsType = $object
            return $object
        }
        
        return $this.CacheAsType
    }

    [string] Get () {
        # If it's not in our cache go read it into our cache
        if (-not $this.isLoaded) {
            $this.CacheMisses ++
            # Go out to the disk
            $ObjectFromDisk = ([FileStorageAdapter] $this).Get()
            $this.SetCache($ObjectFromDisk)
        } else {
            $this.CacheHits ++
        }
        
        return $this.GetCache()
    }
}

Class CachableObject {
    [StorageAdapter] $StorageAdapter
    
    hidden [object] $Cache

    [bool] $isLoaded

    [bool] $CacheWrites
    [bool] $CacheReads

    [bool] GetIsLoaded() {
        return $this.isLoaded()
    }

    [void] OverrideCache( [object] $Data ) {
        $this.Cache = $Data
        $this.isLoaded = $true
    }
    [void] ClearCache() {
        $this.Cache = $null
        $this.isLoaded = $false
    }

    [void] LoadCache() {
        if (-not $this.isLoaded) {
            $this.Cache = $this.StorageAdapter.Get()
            $this.isLoaded = $true
        }
    }

    [object] GetCache() {
        if (-not $this.isLoaded) {
            throw "Requested Object not in Cache"
        }
        return $this.Cache
    }

    [void] Flush() {
        $this.StorageAdapter.Set()
    }

    [void] SetValue([object] $Data) {
        $this.OverrideCache($Data)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }

    [object] GetValue() {
        if ($this.isLoaded) {
            return $this.GetCache()
        }
        
        return $this.StorageAdapter.Get()
    }
}

<#

class StorageAdapter : IStorageAdapter {
	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $AutoFlush = $True

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $WasModified = $false

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $isLoaded = $false

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [object] $Cache

    [int] $Accessed = 0
    [int] $Loaded = 0
    [int] $PreWarmed = 0
    [int] $Updated = 0
    [int] $Written = 0

    StorageAdapter () {}

    [void] EnableAutoFlush () {
        $this.AutoFlush = $true
    }

    [void] DisableAutoFlush () {
        $this.AutoFlush = $False
    }

    [void] PreWarm() {
        $this.Load()
        $this.PreWarmed++
    }

    [void] Load() {
        if ($this.isLoaded -eq $false) {
            $this.Loaded++
            $this.isLoaded = $true
        }
    }
    
    [void] Flush () {
        if ($this.WasModified) {
            $this.Written++
            $this.WasModified = $false
        }
    }

    [void] Unload () {
        if ($this.isLoaded -eq $true) {
            $this.Cache = $null
            $this.isLoaded = $false
        }
    }

    [object] Get() {
        $this.Accessed ++
        return $this.Cache
    }

    [void] Set([object] $Data) {
        $this.Updated ++
        $this.Cache = $Data
        $this.WasModified = $true
        $this.isLoaded = $true
    }
}
<#

class MemoryStorageAdapter : StorageAdapter {
    [object] $HiddenCache

    MemoryStorageAdapter () : Base() {}

    [void] Load() {
        $this.isLoaded = $true
        $this.Cache = $this.HiddenCache

        ([StorageAdapter] $this).Load()
    }
    [void] Unload() {
        $this.isLoaded = $false
        $this.HiddenCache = $this.Cache
        
        ([StorageAdapter] $this).Unload()
    }
}

class DiskStorageAdapter : StorageAdapter {
    
	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $AutoFlush = $True

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $WasModified = $false
    
	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $isLoaded = $false

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [object] $Cache

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [System.IO.FileInfo] $File

    [string] $Path

    DiskStorageAdapter ([string] $Path) : Base() {
        if (-not (test-path $Path)) {
            new-item -itemtype file $Path
        }
        $this.File = get-item $Path
        $this.Path = $Path
    }

    [string] GetPath () {return $this.File.FullName}

    [void] Load() {
        if (-not ($this.isLoaded)) {

            $json = Get-Content -raw $this.File
            $this.Cache = [Newtonsoft.Json.JsonConvert]::DeserializeObject($json, $this.Type, $script:jsonDataFileSerializerSettings))
        }

        ([StorageAdapter] $this).Load()
    }

    # Flush is how we actually write to disk
    [void] Flush () {
        if ($this.WasModified) {
            Set-Content -value $this.Cache -Path $this.File.Fullname
        }

        ([StorageAdapter] $this).Flush()
    }

    [object] Get () {
        return $this.Get($True)
    }
    [object] Get ($Load) {
        if (-not $this.isLoaded -and -not $Load) {
            throw "Object representing $($this.File.fullname) was requested but it is not loaded. Either pass $true so we load or add your own guards to check if the data is loaded."
        }

        $this.Load()

        return ([StorageAdapter] $this).Get()
    }


    [void] PreWarm () {
        $this.Load()
        ([StorageAdapter] $this).PreWarm()
    }

    [void] Set ([string] $Data) {
        ([StorageAdapter] $this).Set($Data)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }
}

class ICollectionMember {
    [int] $id
    [object] $Data
    [Type] $Type

    CollectionMember() {}

    [void] Set() { throw "Do not call methods on an interface." }
    [object] Get() { throw "Do not call methods on an interface." }
}

class CollectionMember {
    [int] $Id
    hidden [object] $Data
    [Type] $Type

    CollectionMember ([int] $Id, [Type] $Type) {
        $this.Id = $Id
        $this.Type = $Type
    }

    CollectionMember ([int] $Id, [object] $Data, [Type] $Type) {
        $this.Id = $Id
        $this.Type = $Type
        $this.Data = ($Data -as $this.Type)
    }

    [void] Set ([object] $Data) {
        $this.Data = ($Data -as $this.Type)
    }

    [object] Get () {
        return ($this.Data -as $this.Type)
    }
}

class BackedCollectionMember : CollectionMember {
    [Type] $Type

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    hidden [object] $Data

    [StorageAdapter] $DataAdapter

    [Newtonsoft.Json.JsonConstructorAttribute()]
    BackedCollectionMember ([StorageAdapter] $DataAdapter, [int] $Id, [Type] $Type) : Base($Id, $Type){
        $this.DataAdapter = $DataAdapter
    }

    BackedCollectionMember ([StorageAdapter] $DataAdapter, [int] $Id, [object] $Data, [Type] $Type) : Base($Id, $Type){
        $this.DataAdapter = $DataAdapter
        $this.Set($Data)
    }

    [void] EnableAutoFlush () {
        $this.DataAdapter.AutoFlush = $true
    }

    [void] DisableAutoFlush () {
        $this.DataAdapter.AutoFlush = $False
    }

    [void] Unload() {
        $this.DataAdapter.Unload()
    }

    [void] Load() {
        $this.DataAdapter.Load()
    }

    [void] Flush () {
        $this.DataAdapter.Flush()
    }

    [void] Set ([object] $Data) {
        $DataAsType = [Convert]::ChangeType($Data, $This.Type)
        
        $json = ConvertTo-Json $DataAsType -depth 10
        #$json = [Newtonsoft.Json.JsonConvert]::SerializeObject($DataAsType, $this.Type, $Settings)
        $this.DataAdapter.Set($json)
    }

    [object] Get () {
        $thisRawContents = $this.DataAdapter.Get()
        $thisObject = ([Newtonsoft.Json.JsonConvert]::DeserializeObject($thisRawContents, $this.Type, $script:jsonDataFileSerializerSettings))

        return $thisObject 
    }
}

class IIndexedCollection {
    [Collections.Generic.List[CollectionMember]] $Members
    [Hashtable] $MembersById 

    [Collections.Generic.List[CollectionMember]] $Projections
    [Hashtable] $ProjectionsById 

    [bool] $AutoReIndex

	[string[]] $IndexNames
	[System.Collections.Generic.Dictionary[object,object]] $Indices
    [Type] $Type

    [bool] GetAutoReIndex() { throw "Do not call methods on an interface." }
    
    [void] EnableAutoReIndex() { throw "Do not call methods on an interface." }
    [void] DisableAutoReIndex() { throw "Do not call methods on an interface." }

    [void] AddIndex([string] $Name) { throw "Do not call methods on an interface." }
    
    [void] ReIndex([CollectionMember] $Object) { throw "Do not call methods on an interface." }

    [void] GetIndex([string] $Name) { throw "Do not call methods on an interface." }
    [void] GetIndices() { throw "Do not call methods on an interface." }
    [object[]] GetAll() { throw "Do not call methods on an interface." }
    [int64] GetNextEntryIndex() { throw "Do not call methods on an interface." }

    [void] Add([object] $Object) { throw "Do not call methods on an interface." }
}


class IndexedCollection : IIndexedCollection{
	[Collections.Generic.List[CollectionMember]] $Members = [Collections.Generic.List[CollectionMember]]::new()
	[Hashtable] $MembersById = @{}

	[string[]] $IndexNames = @()

	[System.Collections.Generic.Dictionary[object,object]] $Indices = @{}
    
    [bool] $AutoReIndex = $true

    [Type] $Type

    IndexedCollection([Type] $Type) {
        $this.Type = $Type
    }

    IndexedCollection([Type] $Type, [Collections.Generic.List[CollectionMember]] $Members ) {
        $this.Type = $Type
        $this.Members = $Members
    }

    [void] AddIndex ([string] $Name) {
        $this.IndexNames += $Name

        if (-not $this.AutoReIndex) {
            return
        }

        $this.ReIndexAll()
    }

    [void] ReIndexAll () {
        $this.Indices = @{}
        
        $this.IndexNames.Foreach{$this.Indices.Add($_,@{})}

        foreach ($Member in $this.Members) { 
            $this.ReIndex($Member)
        }
    }

    [void] ReIndex ([CollectionMember] $Member) {
        $thisMemberObject = $Member.Get() 

        foreach ($IndexName in $this.IndexNames) {

            $thisIndex = $this.Indices[$IndexName] # Indices["first name"]
            $thisValue = $thisMemberObject.$IndexName   # Tom - Get "first name" from object

            if ($thisValue -eq $null) { continue }

            if ($thisIndex.ContainsKey($ThisValue)) {
                [void] ($thisIndex[$ThisValue].Add($Member.Id))
            } else {
                $newArrayList = [system.collections.arraylist]::new()
                [void] $newArrayList.Add($Member.Id)

                $thisIndex[$ThisValue] = $newArrayList
            }
        }
    }

    [void] Add ( [CollectionMember] $Member ) {
        $this.Members.Add($Member)
        $this.MembersById.Add($Member.Id, $Member)

        $this.ReIndex($Member)

    }

    [object[]] GetByIndexAndValue ([string] $Index, [object] $Value) {
        $id = $this.Indices[$Index][$Value]
        $thisMember = $this.MembersById[$id]

        if ($thisMember.Count -eq 0) { return @() }


        return $thisMember[0].Get()
    }

    [object[]] GetAll() {
        $result = [Collections.Generic.List[object]]::new() 

        foreach ($Member in $this.Members) {
            $result.add($member.Get())
        }

        return $result.ToArray();
    }

    [int64] GetNextEntryIndex(){ return $this.Members.Count }

    [bool] GetAutoReIndex() { return $this.AutoReIndex }

    [void] EnableAutoReIndex() { $this.AutoReIndex = $true }
    [void] DisableAutoReIndex() { $this.AutoReIndex = $false }
}

class BackedIndexedCollection : IndexedCollection {
    [bool] $AutoFlush = $true

    [StorageAdapter] $MetadataAdapter

    [Newtonsoft.Json.JsonConstructorAttribute()]
    BackedIndexedCollection ([StorageAdapter] $MetadataAdapter, [Collections.Generic.List[BackedCollectionMember]] $Members, [Type] $Type) : Base ($Type) {
        $this.MetadataAdapter = $MetadataAdapter
        $this.AddRange($Members)
    }

    BackedIndexedCollection ([StorageAdapter] $MetadataAdapter, [Type] $Type) : Base ($Type) {
        $this.MetadataAdapter = $MetadataAdapter
    }

    [hashtable] GetMemoryReport () {
        $active = 0
        $Inactive = 0
    
        foreach ($Member in $this.Members) {
            if ($Member.DataAdapter.isLoaded -eq $true) {
                $active ++
            } else {
                $Inactive ++
            }
        }
        return @{
            "Active" = $active
            "Inactive" = $Inactive
        }
    }

    [void] UnloadAll () {
        foreach ($member in $this.Members) { $member.Unload() }
    }
    [void] Unload ([int] $id) {
        ([BackedCollectionMember] $this.MembersById[$id]).Unload()
    }
    [void] LoadAll () {
        foreach ($member in $this.Members) { $member.Load() }
    }
    [void] Load ([int] $id) {
        ([BackedCollectionMember] $this.MembersById[$id]).Load()
    }

    [void] AddRange ([BackedCollectionMember[]] $Members) {

        foreach ($Member in $Members) {
            ([IndexedCollection] $this).Add([CollectionMember]$Member)
            $Member.Unload()
        }

        $this.Save()
    }

    [void] Add ( [BackedCollectionMember] $Member ) {
        ([IndexedCollection] $this).Add([CollectionMember]$Member)

        $this.Save()

        $Member.Unload()
    }

    [void] Save() {
        if ($this.GetAutoFlush) {
            $this.Flush()
        }
    }

    # Preload some number of records, preferably heavier used ones
    [void] PreWarm () {
        $MemberCount = @($this.Members).count
        $TotalAccess = 0

        foreach ($Member in $This.Members) {
            $TotalAccess += $Member.DataAdapter.Accessed
        }
        
        $Average = $TotalAccess / $MemberCount

        foreach ($Member in $This.Members) {

            if ($Member.DataAdapter.Accessed -gt $Average) {
                $thisDataAdapter = ([BackedCollectionMember] $Member).DataAdapter
                $thisDataAdapter.PreWarm()
            }
        }
    }

    [void] Flush() {
        [CollectionMember[]] $tempMembers = $This.Members
        #[BackedCollectionMember[]] $BackedMembers = $tempMembers
        [Collections.Generic.List[BackedCollectionMember]] $BackedMembers = $tempMembers
        $tempObj = @{
            "AutoFlush" = $this.AutoFlush
            "Members" = $BackedMembers
            "IndexNames" = $this.IndexNames
            "Indices" = $this.Indices
            "AutoReIndex" = $this.AutoReIndex
            "Type" = $this.Type
            "MetadataAdapter" = $this.MetadataAdapter
        }
        
        $json = [Newtonsoft.Json.JsonConvert]::SerializeObject($tempObj, $script:jsonMetaDataSerializerSettings)
        $this.MetadataAdapter.Set($json)
    }

    [bool] GetAutoFlush () { return $this.AutoFlush }

    [void] EnableAutoFlush () { 
        @($this.Members).Foreach{$_.EnableAutoFlush()}
        $this.AutoFlush = $True
    }

    [void] DisableAutoFlush () { 
        @($this.Members).Foreach{$_.DisableAutoFlush()}
        $this.AutoFlush = $false
    }
}

Function Load-BackedIndexCollection () {
    param (
        [System.IO.DirectoryInfo] $Path
    )
    $BackedIndexCollectionMetadataPath = (join-path $Path "collection.store")
    if (-not (test-path $BackedIndexCollectionMetadataPath)) {
        throw "No metadata available to load"
    }

    $MetadataRaw = Get-Content -raw $BackedIndexCollectionMetadataPath

    [Newtonsoft.Json.JsonConvert]::DeserializeObject($MetadataRaw, [BackedIndexedCollection], $script:jsonMetaDataSerializerSettings)
}

Function New-IndexedCollection () {
    param ( 
        [string[]] $Index,
        [object[]] $InputCollection
    )

    $Type = $InputCollection[0].GetType()
    $InMemoryAdapter = [MemoryStorageAdapter]::New()

    $thisCollection = [BackedIndexedCollection]::New($InMemoryAdapter, $Type)

    foreach ($Entry in $Index) {
        $thisCollection.AddIndex($Entry)
    }

    foreach ($InputItem in $InputCollection) {
        $ItemId = $thisCollection.GetNextEntryIndex()
        
        $stopwatch = [System.Diagnostics.Stopwatch]::startnew()

        $ItemInMemoryAdapter = [MemoryStorageAdapter]::New()
        $one = $stopwatch.elapsedmilliseconds

        $NewCollectionMember = [BackedCollectionMember]::New($ItemInMemoryAdapter, $itemId, $InputItem , $Type)
        $two = $stopwatch.elapsedmilliseconds - $one

        $ThisCollection.Add($NewCollectionMember)
        $three = $stopwatch.elapsedmilliseconds - $two

        write-host "wait"
    }

    return $thisCollection
}

<#

class IndexedCollection : IIndexedCollection{
	[Collections.Generic.List[CollectionMember]] $Members = [Collections.Generic.List[CollectionMember]]::new()

	[string[]] $IndexNames = @()
	[hashtable] $Indices = @{}
    [bool] $AutoFlush = $true
    [Type] $Type

    IndexedCollection () {}

	IndexedCollection ([Type] $Type) {
        $this.Type = $Type
	}

[int64] GetNextEntryIndex(){ return $this.Entries.Count }

[bool] GetAutoFlush () { return $this.AutoFlush }

[void] EnableAutoFlush () { $this.AutoFlush = $true }
[void] DisableAutoFlush () { $this.AutoFlush = $false }

[hashtable] GetMemoryReport () {
    $active = 0
    $Inactive = 0

    foreach ($Datum in $this.Data) {
        if ($Datum.isLoaded -eq $true) {
            $active ++
        } else {
            $Inactive ++
        }
    }
    return @{
        "Active" = $active
        "Inactive" = $Inactive
    }
}

[void] AddIndex ($name) {
    $this.IndexNames += $name
    $this.Indices.Add($Name, @{})
    $this.Reindex()
}

[void] Reindex() {
    $this.Indices = @{}
    foreach ($IndexName in $this.IndexNames) {
        $this.Indices.Add($IndexName, @{})
    }

    foreach ($Entry in $this.Data) {
        $this.Reindex($Entry)
    }
}

[void] Reindex( [CollectionMember] $Entry ) {
    # Get the keys we need to build our index
    
    foreach ($IndexName in $this.IndexNames) {
        $thisValue = $Entry.$IndexName

        $this.Indices[$IndexName][$ThisValue] = $Entry
    }
}

[void] Add ([CollectionMember] $newEntry
[void] Add ([object] $Data, [bool] $Unload) {
    $newId = $this.GetNextEntryIndex()
    $newObject = $Data
    $newType = $this.Type

    $newCollectionMember = [CollectionMember]::new($newId, $newObject, $newType)
    
    $this.Data.Add($newCollectionMember)
    
    $this.Reindex($newCollectionMember)
} 

[CollectionMember[]] GetAll () {
    return $this.Data
}

[Collections.Generic.List[object]] GetAllValues () {
    return $this.Data.Data
}

[boolean] IndexExists([string] $Key) {
    return $this.Indices.ContainsKey($Key)
}

[Object[]] Get([string] $Key) {
    return $this.Indices[$Key]
}

[Object] Get([string] $Key, [object] $Value) {
    return $this.Indices[$Key][$Value]
}#>
