class IndexedCollectionMember {
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[System.IO.FileInfo] $DataFile
    [string] $DataFilePath
    
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[object] $Data
    [int] $Id

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[Type] $Type
    [string] $TypeName

	[hashtable] $Indices = @{}

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[bool] $isLoaded = $false

	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [string] $unloadingMode = "Eager"
	
	[Newtonsoft.Json.JsonIgnoreAttribute()]
    [IndexedCollection] $Collection
<#
	IndexedCollectionMember ([int64] $id, [object] $Data, [string] $TypeName, [string] $DataFilePath, [IndexedCollection] $Collection) {
        $tempType = [Type]::GetType($TypeName)

        $this._constructorHelper($id, $tempType, $DataFilePath, @{}, $Collection)

        $this.SetValue($Data)

        $this.ReIndex($Collection.IndexNames)
	}#>
	IndexedCollectionMember ([int64] $id, [object] $Data, [Type] $Type, [string] $DataFilePath, [IndexedCollection] $Collection) {
        $this._constructorHelper($id, $Type, $DataFilePath, @{}, $Collection)

        $this.SetValue($Data)
        
        $this.ReIndex($Collection.IndexNames)
	}


    [Newtonsoft.Json.JsonConstructorAttribute()]
	IndexedCollectionMember ([int64] $id, [string] $TypeName, [Hashtable] $Indices, [string] $DataFilePath) {
        $tempType = [Type]::GetType($TypeName)
        $this._constructorHelper($id, $tempType, $DataFilePath, $Indices, $null)
	}

    [void] _constructorHelper([int64] $id, [Type] $Type, [string] $DataFilePath, [Hashtable] $Indices, [IndexedCollection] $Collection) {

        $this.Indices = $Indices

        $this.Id = $id

		$this.Type = $Type
        $This.TypeName = $type.FullName

        $this.DataFilePath = $DataFilePath
        if (-not (test-path $DataFilePath)) {
            new-item -ItemType file -path $DataFilePath
        }


		$this.DataFile = get-item $DataFilePath

        $This.Collection = $Collection
    }

    [object] GetCoveredValue() {
        $newht = @{}
        foreach ($IndexKv in $this.Indices.GetEnumerator()) {
            $IndexName = $IndexKv.Name
            $IndexValue = $IndexKv.Value
            $newht.add($IndexName, $IndexValue)
        }
        return $newht
    }

    [object] GetProjection([string[]] $Fields) {
        $newht = @{}
        
        $coveredObject = $this.GetCoveredValue()

        $fullObject = $Null
        foreach ($Field in $Fields) {
            # If our covered object doesnt fit our projection load the full value from disk
            if ( -not $coveredObject.ContainsKey($Field) -and $FullObject -eq $null) {
                $FullObject = $this.GetValue()
            }

            # Build our projection
            if ($coveredObject.ContainsKey($Field)) {
                $newHt.add($field, $coveredObject[$Field])
                continue;
            }
            elseif ($FullObject.$Field -ne $null) {
                $newHt.add($Field, $fullObject.$Field)
            }
        }

        return $newht
    }

    [void] SetValue ([object] $Data) {
        $this.Data = $Data
        $this.isLoaded = $true

        $This.SaveData()
    }

	[object] GetValue ($Name) {
        return ($this.GetProjection($Name))[$Name]
	}
	[object] GetValue () {
		if (-not $this.isLoaded) {
            #write-host "Get Value was called and required a load"
			$this.Load()
		}

        return $this.Data
	}

	[void] ReIndex ([string[]] $Indices) {
        if ($this.isLoaded) {
            $thisValue = $this.GetValue()
        } else {
    		$thisValue = $this.GetProjection($Indices)
        }

		foreach ($IndexName in $Indices) {
			$this.Indices[$IndexName] = $thisValue.$IndexName
		}
	}

	[void] Unload () {
        if ($this.unloadingMode -eq "Never") { return }
        
		$this.Data = $null
		$this.isLoaded = $false
	}
	
	[void] Load () {
		$Json = Get-Content -raw -path $this.DataFilePath
        
        $this.Data = [Newtonsoft.Json.JsonConvert]::DeserializeObject($Json, $this.Type)

        $this.isLoaded = $true
	}

    [void] Save() {
        $this.SaveMeta()
        $this.SaveData()
    }

    [void] SaveMeta () {
        $this.Collection.Save()
    }

	[void] SaveData () {
        if (-not ($this.isLoaded)) { return }
		$json = ConvertTo-Json -InputObject $this.data

		set-content -Path $this.DataFile.FullName -Value $Json
	}
}

class IndexedCollection {
	[Collections.Generic.List[IndexedCollectionMember]] $Data = [Collections.Generic.List[IndexedCollectionMember]]::new()
	[System.IO.DirectoryInfo] $Directory
    #[string[]] $ColumnNames = @()
	[string[]] $IndexNames = @()
	[hashtable] $Indices = @{}
    [bool] $Autoflush = $true

	IndexedCollection ([System.IO.DirectoryInfo] $Directory) {
		$this.Directory = $Directory
	}

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
    # Force a flush when you have auto flush disabled
    [void] Flush () {
        $tmpValue = $this.Autoflush
        $this.Autoflush = $true
        $this.Save()
        $this.Autoflush = $tmpValue
    }

    [void] Save () {
        if (-not $this.Autoflush) { return }

        $Path = (join-path $this.Directory "meta.json")  
        $json = ConvertTo-Json -inputobject $this.Data

        set-content -path $Path -value $Json

        @($this.Data).Foreach{
            $thisMember = ([IndexedCollectionMember]$_)
            
            #$thisMember.SaveData()
        }
    }

    [void] Load () {
        $Metadata = (Get-Content -raw (join-path $PSScriptRoot ".\Collections\meta.json"))
        $MetadataArr = ConvertFrom-Json -InputObject $Metadata -AsHashtable -depth 10

        foreach ($MetadataEntry in $MetadataArr) {
            $newMember = [IndexedCollectionMember]::new($MetadataEntry.Id, $MetadataEntry.TypeName, $MetadataEntry.Indices, $MetadataEntry.DataFilePath)
            $newMember.Collection = $this
            $this.data.Add($newMember)
        }

        $this.ReIndex()
    }

	[void] AddIndex ($name) {
        $this.IndexNames += $name
		$this.Indices.Add($Name, @{})
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

    [void] Reindex( [IndexedCollectionMember] $Entry ) {
        # Get the keys we need to build our index
        $EntryHt = $Entry.GetProjection($this.IndexNames)

        foreach ($IndexName in $this.IndexNames) {
            $thisValue = $EntryHt[$IndexName]

            $this.Indices[$IndexName][$ThisValue] = $Entry
        }
    }

	[void] Add ([object] $Data, [bool] $Unload) {
        $id = $this.Data.Count
        [string] $DataPath = (join-path $this.Directory "$id.data.json")

		$newCollectionMember = [IndexedCollectionMember]::new($this.Data.Count, $Data, $Data.GetType(), $dataPath, $This)
        $newCollectionMember.id = $id

		$this.Data.Add($newCollectionMember)
        
        $this.Reindex($newCollectionMember)

        if ($Unload) {
            $newCollectionMember.Unload()
        }

        $this.Save()
	} 

	[IndexedCollectionMember[]] GetAll () {
		return $this.Data
	}

	[Collections.Generic.List[object]] GetAllValues () {
		return $this.Data.Data
	}

	[boolean] IndexExists([string] $Key) {
		return $this.Indices.ContainsKey($Key)
	}

	[Collections.Generic.List[Object]] Get([string] $Key) {
		return $this.Indices[$Key]
	}

	[Object] Get([string] $Key, [object] $Value) {
		return $this.Indices[$Key][$Value]
	}
}
