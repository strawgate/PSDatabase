$script:jsonMetaDataSerializerSettings = [Newtonsoft.Json.JsonSerializerSettings]::new()
 
$script:jsonMetaDataSerializerSettings.Formatting = [Newtonsoft.Json.Formatting]::Indented
$script:jsonMetaDataSerializerSettings.NullValueHandling  = [Newtonsoft.Json.NullValueHandling]::Ignore
$script:jsonMetaDataSerializerSettings.TypeNameHandling = [Newtonsoft.Json.TypeNameHandling]::Auto

$script:jsonDataFileSerializerSettings = [Newtonsoft.Json.JsonSerializerSettings]::new()
 
$script:jsonDataFileSerializerSettings.Formatting = [Newtonsoft.Json.Formatting]::Indented
$script:jsonDataFileSerializerSettings.NullValueHandling  = [Newtonsoft.Json.NullValueHandling]::Ignore
$script:jsonDataFileSerializerSettings.TypeNameHandling = [Newtonsoft.Json.TypeNameHandling]::None


### Storage Adapters are a generic way to connect an object to a persistence store.
### The storage adapter keeps some metrics while its alive for diagnostics.
class IStorageAdapter {

    [int] $Reads
    [int] $Writes

    [scriptblock[]] $Serializer
    [scriptblock[]] $Deserializer

    IStorageAdapter () {}

    [object] GetStatistics () { throw "Do not call methods on an interface." }

    [object] Get() { throw "Do not call methods on an interface." }
    [void] Set([Object] $Data) { throw "Do not call methods on an interface." }
}

class StorageAdapter : IStorageAdapter {

    [int] $Reads = 0
    [int] $Writes = 0

    StorageAdapter () {}

    [hashtable] GetStatistics () {
        return @{
            "Reads" = $this.Reads
            "Writes" = $this.Writes
        }
    }

    [object] Get() { 
        $this.Reads++
        return $null
    }
    [void] Set([Object] $Data) {
        $this.Writes ++
    }
}

## This storage adapter keeps objects in memory for persistence. Reads and writes to and from memory storage
# are just reads and writes to and from memory
class MemoryStorageAdapter : StorageAdapter {

    [object] $Data

    MemoryStorageAdapter () {}

    [object] Get() {
        ([StorageAdapter] $this).Get()

        return $this.Data
    }

    [void] Set([object] $Data) {
        ([StorageAdapter] $this).Set($Data)

        $this.Data = $Data
    }
}

## This storage adapter uses a file for its backing source. This means all gets are direct Disk I/O and all sets are direct Disk I/O
# This is useful if you want the same behavior as the MemoryStorageHelper but dont want to have to worry about data persistence
# For performance use the CacheableFileStorageAdapter

class FileStorageAdapter : StorageAdapter {

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [System.IO.FileInfo] $File

    [string] $Path

    FileStorageAdapter ([string] $Path) : Base () {
        if (-not (test-path $Path)) {
            new-item -itemtype file -path $Path -value "null"
        }
        $this.Path = $Path
        $this.File = Get-Item $Path
    }

    [object] Get() {
        ([StorageAdapter] $this).Get()

        $raw =  (Get-Content -raw $this.File)
        return $raw
    }

    [void] Set([object] $Data) {
        ([StorageAdapter] $this).Set($Data)
        Set-Content -value $Data -Path $this.File.FullName
    }
}

Class CachableObject {
    
    [StorageAdapter] $StorageAdapter
    
	[Newtonsoft.Json.JsonIgnoreAttribute()]
    hidden [object] $Cache

    [type] $Type

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $isLoaded

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $AutoFlush = $true
    
    [datetime] $LastAccess = [datetime] 0
    [int] $CacheHits = 0
    [int] $CacheMisses = 0
    [int] $CacheFlushes = 0
    [int] $CacheWrites = 0
    [int] $CacheUnloads = 0
    [int] $CachePrewarms = 0


    CachableObject ([Type] $Type, [StorageAdapter] $StorageAdapter) {
        $this.StorageAdapter = $StorageAdapter
        $this.Type = $Type
        $this.Initialize()
    }

    CachableObject ([Type] $Type, [Object] $Object, [StorageAdapter] $StorageAdapter) {
        $this.StorageAdapter = $StorageAdapter
        $this.Type = $Type
        $this.SetValue($Object)
        $this.Initialize()
    }
    CachableObject ([Type] $Type, [Object] $Object) {
        $this.StorageAdapter = [MemoryStorageAdapter]::new()
        $this.Type = $Type
        $this.SetValue($Object)
        $this.Initialize()
    }

    CachableObject ([Type] $Type) {
        $this.StorageAdapter = [MemoryStorageAdapter]::new()
        $this.Type = $Type
        $this.Initialize()
    }

    [void] Initialize () {
        #$this.StorageAdapter.Flush()
    }
    
    [hashtable] GetCacheStatistics() {
        $newHt = @{}

        $newHt.Add("Hits",      $this.CacheHits)
        $newHt.Add("Misses",    $this.CacheMisses)
        $newHt.Add("Flushes",   $this.CacheFlushes)
        $newHt.Add("Writes",    $this.CacheWrites)
        $newHt.Add("Unloads",   $this.CacheUnloads)
        $newHt.Add("Prewarms",  $this.CachePrewarms)

        return $NewHt
    }
    [hashtable] GetStatistics () {
        $FinalStatistics = @{}

        $FinalStatistics["Storage"] = $this.StorageAdapter.GetStatistics()
        $FinalStatistics["Cache"] = $this.GetCacheStatistics()
        $FinalStatistics["Meta"] = @{
            "isLoaded" = $this.IsLoaded
        }
        return $FinalStatistics
    }

    [void] Prewarm () {
        if (-not ($this.GetIsLoaded())) {
            $this.CachePrewarms ++
            $this.LoadCache()
        } 
    }
    [bool] GetIsLoaded() {
        return $this.isLoaded
    }

    [void] OverrideCache( [object] $Data ) {
        $this.Cache = $Data
        $this.isLoaded = $true

        $this.CacheWrites ++

    }

    [void] Load() {
        $this.LoadCache()
    }

    [void] Unload() {
        $this.ClearCache()
    }

    [void] ClearCache() {
        $this.Cache = $null
        $this.isLoaded = $false
        $this.CacheUnloads ++
    }

    [void] LoadCache() {
        if (-not $this.isLoaded) {
            $Json = $this.StorageAdapter.Get()
            $Object = [Newtonsoft.Json.JsonConvert]::DeserializeObject($Json, $this.Type, $script:jsonDataFileSerializerSettings)
            $this.OverrideCache($Object)
        }
    }

    [object] GetCache() {
        if (-not $this.isLoaded) {
            throw "Requested Object not in Cache"
        }
        return $this.Cache
    }

    [void] Flush() {
        $this.CacheFlushes ++
        $json = [Newtonsoft.Json.JsonConvert]::SerializeObject($this.GetCache(), $script:jsonDataFileSerializerSettings)
        $this.StorageAdapter.Set($json)
    }

    [void] SetValue([object] $Data) {
        $this.OverrideCache($Data)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }

    [object] GetValue() {
        $result = $null

        if ($this.isLoaded) {
            $this.CacheHits++
        }
        else {
            $this.CacheMisses++
            $This.LoadCache()
        }

        $this.LastAccess = [datetime]::now

        $result = $this.GetCache()

        if ($Result -isnot $This.Type) {
            return [Convert]::ChangeType($Result, $this.Type)
        } else {
            return $Result
        }
    }
}

class IndexedCachableObject : CachableObject {
    [int] $id
    [hashtable] $Indices = @{}
    [string[]] $IndexNames = @()


    [Newtonsoft.Json.JsonConstructorAttribute()]
    IndexedCachableObject ([Type] $Type, [string[]] $IndexNames, [StorageAdapter] $StorageAdapter) : Base ([Type] $Type, [StorageAdapter] $StorageAdapter) {
        $this.IndexNames = $IndexNames
    }

    IndexedCachableObject ([Type] $Type, [Object] $Object, [string[]] $IndexNames, [StorageAdapter] $StorageAdapter) : Base ([Type] $Type, [Object] $Object, [StorageAdapter] $StorageAdapter) {
        $this.IndexNames = $IndexNames
        $this.Reindex()
    }
    IndexedCachableObject ([Type] $Type, [Object] $Object, [StorageAdapter] $StorageAdapter) : Base ([Type] $Type, [Object] $Object, [StorageAdapter] $StorageAdapter) { }

    IndexedCachableObject([Type] $Type, [Object] $Object) : Base([Type] $Type, [Object] $Object) { 
        $this.Reindex()
    }

    [void] AddIndex([string] $Name) {
        $this.IndexNames += $Name
        $this.ReIndex()
    }

    [hashtable] GetProjection() {
        $newht = @{}

        foreach ($IndexKV in $this.Indices.GetEnumerator()) {
            $IndexName = $IndexKV.name
            $Value = $IndexKV.Value
            $newht.add($IndexName, $Value)
        }

        return $newht
    }
    
    [void] Reindex() {
        $object = $This.GetValue()

        $this.Indices = @{}

        foreach ($IndexName in $this.IndexNames) {
            $thisValueForIndexName = $Object.$IndexName
            $this.Indices[$IndexName] = $thisValueForIndexName
        }
    }

    [void] Reindex([string[]] $IndexNames) {
        $object = $This.GetValue()

        $this.Indices = @{}

        foreach ($IndexName in $IndexNames) {
            $thisValueForIndexName = $Object.$IndexName
            $this.Indices[$IndexName] = $thisValueForIndexName
        }
    }
}

class IndexedCachableCollection {
    
    [StorageAdapter] $MetadataAdapter

    [hashtable] $Indices = @{}
    [string[]] $IndexNames = @()

    [Collections.Generic.List[IndexedCachableObject]] $Members = @()

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [Hashtable] $MembersById = @{}

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [bool] $AutoFlush = $true

    IndexedCachableCollection () {
        $this.Initialize(@(), [MemoryStorageAdapter]::new())
    }
    IndexedCachableCollection ([StorageAdapter] $MetadataAdapter) {
        $this.Initialize(@(), $MetadataAdapter)
    }
    IndexedCachableCollection ([string[]] $IndexNames) {
        $this.Initialize($IndexNames, [MemoryStorageAdapter]::new())
    }
    IndexedCachableCollection ([string[]] $IndexNames, [StorageAdapter] $MetadataAdapter) {
        $this.Initialize($IndexNames, $MetadataAdapter)
    }
    
    IndexedCachableCollection ([string[]] $IndexNames, [IndexedCachableObject] $Members) {
        $this.Initialize($IndexNames, [MemoryStorageAdapter]::new())
        foreach ($Member iN $Members) {
            $this.Add($Member)
        }

    }
    IndexedCachableCollection ([string[]] $IndexNames, [IndexedCachableObject] $Members, [StorageAdapter] $MetadataAdapter) {
        $this.Initialize($IndexNames, $MetadataAdapter)
        foreach ($Member iN $Members) {
            $this.Add($Member)
        }

    }

    static [IndexedCachableCollection] Load ($Path) {
        $MetadataJson = Get-Content -raw (Join-Path $Path "collection.json")
        
        $MetadataObj = [Newtonsoft.Json.JsonConvert]::DeserializeObject($MetadataJson, [IndexedCachableCollection], $script:jsonMetaDataSerializerSettings)

        return $MetadataObj
    }

    [void] Initialize([string[]] $IndexNames, [StorageAdapter] $MetadataAdapter) {
        foreach ($indexName in $IndexNames) {
            $this.AddIndex($IndexName)
        }
        $this.MetadataAdapter = $MetadataAdapter
    }

    [void] SetAutoFlush([bool] $bool) {
        $this.AutoFlush = $bool
    }

    [void] Prewarm() {
        [int] $count = 0
        [int] $sum = 0
        foreach ($Member in $this.Members) {
            $theseStatistics = $Member.GetCacheStatistics()

            $count ++ 
            $sum += $theseStatistics.Hits +  $theseStatistics.Misses
        }

        $Average = $Sum / $Count

        foreach ($Member in $this.Members) {
            $theseStatistics = $Member.GetCacheStatistics()

            if (($theseStatistics.Hits +  $theseStatistics.Misses) -gt $Average) {
                $member.Prewarm()
            }
        }

    }

    [hashtable] GetStatistics () {
        
        $Loaded = 0
        $NotLoaded = 0

        foreach ($Member in $this.Members) {
            if ($Member.isLoaded){
                $Loaded ++
            }
             else {
                 $NotLoaded ++
             }
        }

        return @{
            "Loaded" = $Loaded
            "Not Loaded" = $NotLoaded
        }
    }

    [int] GetNextAvailableId() {
        return $this.Members.Count
    }
    [void] AddIndex ([string] $IndexName) {
        $this.IndexNames += $IndexName
        $this.ReIndexAll()
    }

    [void] AddIndices ([string[]] $IndexNames) {
        $this.IndexNames += $IndexNames
        $this.ReIndexAll()
    }

    [void] ReIndexMember([IndexedCachableObject] $Member) {
        $this.ReindexMember($Member, $False)
    }

    [void] ReIndexMember([IndexedCachableObject] $Member, [bool] $Force) {
        if ($Force) { $member.ReIndex() }

        $Object = $member.GetValue()
        foreach ($IndexName in $this.IndexNames) {
            $MemberValue = $Object.$IndexName

            # This object doesn't contain a value for one of our indices. Skip.
            if ($MemberValue -eq $Null) {
                continue;
            }

            if (-not ($this.Indices[$IndexName].ContainsKey($MemberValue))) {
                $this.Indices[$IndexName][$MemberValue] = [System.Collections.Arraylist]::new()
            }

            $this.Indices[$IndexName][$MemberValue].Add($Member.Id)
        }
    }

    [void] GarbageCollection([int] $Minutes) {
        foreach ($Member in $this.Members) {
            $cutoff = ([datetime]::now).addMinutes((-1 * $Minutes))
            if ($Member.LastAccess -lt $cutoff) {
                $Member.Unload()
            }
        }
    }
    
    [void] Load() {
        foreach ($Member in $this.Members) {
            $Member.Load()
        }
    }
    [void] Unload() {
        foreach ($Member in $this.Members) {
            $Member.Unload()
        }
    }

    [void] ReIndexAll() {
        $This.ReIndexAll($False)
    }

    [void] ReIndexAll([bool] $Force) {
        foreach ($IndexName in $this.IndexNames) {
            $this.Indices[$IndexName] = @{}
        }

        foreach ($Member in $this.Members) {
            if ($Force) { $Member.ReIndex()}
            
            $this.ReIndexMember($Member)
        }
    }

    [void] Flush () {
        $Object = [Newtonsoft.Json.JsonConvert]::SerializeObject($This, $This.GetType(), $script:jsonMetadataSerializerSettings)
        $This.MetadataAdapter.Set($Object)
    }

    [void] Add([Object] $Object) {

        $StorageAdapter = [MemoryStorageAdapter]::new()
        $This.Add($Object, $StorageAdapter)
    }

    [void] Add([Object] $Object, [StorageAdapter] $StorageAdapter) {

        $thisNewMember = [IndexedCachableObject]::new($Object.GetType(), $Object, $This.IndexNames, $StorageAdapter)

        $thisNewMember.Id = $this.GetNextAvailableId()
        
        $this.Members.Add($thisNewMember)
        $this.MembersById.Add($thisNewMember.Id, $thisNewMember)

        $this.ReIndexMember($thisNewMember)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }

    [void] Add([IndexedCachableObject] $NewMember) {
        $NewMember.Id = $this.GetNextAvailableId()
        
        $this.Members.Add($NewMember)
        $this.MembersById.Add($NewMember.Id, $NewMember)

        $this.ReIndexMember($NewMember)

        if ($this.AutoFlush) {
            $this.Flush()
        }
    }

    [IndexedCachableObject[]] Get() {
        return $This.Members
    }
    [IndexedCachableObject] Get([int] $Id) {
        return $This.MembersById[$Id]
    }
    
    [IndexedCachableObject[]] Get([int[]] $Ids) {
        $theseResults = [Collections.Generic.List[IndexedCachableObject]]::new()

        foreach ($id in $ids) {
            $theseResults.Add($This.Get($Id))
        }

        return $theseResults.ToArray()
    }


    [object[]] GetValues() {
        return @($This.Get()).Foreach{$_.GetValue()}
    }

    [object[]] GetByNameAndValue([string] $Name, [object] $Value) {
        $MatchingIds = $This.Indices[$Name][$Value]
        
        return $this.Get($MatchingIds)
    }

    [object[]] GetValuesByNameAndValue([string] $Name, [object] $Value) {
        return @($this.GetByNameAndValue($Name, $Value)).Foreach{$_.GetValue()}
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
