class IndexedCollectionMember {
	[System.IO.FileInfo] $File
	[object] $Data
	[hashtable] $Indices = @{}
	[Type] $Type
	[bool] $isLoaded = $false
	
	IndexedCollection ([object] $Data, [Type] $Type, [System.IO.FileInfo] $Path) {
		$this.Data = [Convert]::ChangeType($Data, $Type)
		$this.Type = $Type
		$this.Path = $Path
	}

	[object] GetValue () {
		if (-not $this.isLoaded) {
			$this.Load()
		}

		return $this.Data
	}

	[void] ReIndex ([string[]] $Indices) {
		$thisValue = $this.GetValue()
		$this.Unload()
		
		foreach ($IndexKV in $Indices.GetEnumerator()) {
			$IndexName = $IndexKV.Name

			$this.Indices[$IndexName] = $thisValue.$IndexName
		}
	}
	[void] Unload () {
		$this.Data = $null
		$this.isLoaded = $false
	}
	
	[void] Load () {
		$Json = Get-Content -raw -path $this.File
		$this.Data = [Newtonsoft.Json.JsonConvert]::DeserializeObject($Json, $this.Type)

		$this.isLoaded = $true
	}

	[void] Save () {
		$json = ConvertTo-Json -InputObject $this.Data

		set-content -Path $this.File.Fullname -Value $Json
	}
}

class IndexedCollection {
	[Collections.Generic.List[IndexedCollectionMember]] $Data = [Collections.Generic.List[IndexedCollectionMember]]::new()
	[System.IO.DirectoryInfo] $Directory
	[string[]] $IndexNames = @()
	[hashtable] $Indices = @{}

	IndexedCollection ([System.IO.DirectoryInfo] $Directory) {
		$this.Directory = $Directory
	}

	[void] AddIndex ($name) {
		$this.Indices.Add($Name, @{})
	}

	[void] Reindex() {
		$this.Indices = @{}
		foreach ($IndexKV in $this.Indices.GetEnumerator()) {
			$IndexName = $IndexKV.Name

			$this.Indices.Add($IndexName, @{})
		}

		foreach ($Entry in $this.Data) {
			$Entry.ReIndex($this.IndexNames)

			foreach ($IndexName in $this.IndexNames) {
				$this.Indicies[$IndexName][$Entry.Indicies[$IndexName]] = $Entry
			}
		}

	}

	[void] Add ($Data) {
		$newCollectionMember = [IndexedCollectionMember]::new($Data, $Data.GetType(), (join-path $this.Directory "$($Data.GetHashCode()).json"))
		$this.Data.Add($newCollectionMember)

		foreach ($IndexKV in $this.Indices.GetEnumerator()) {
			$IndexName = $IndexKV.Name

			$this.Indices[$IndexName][$Data.$IndexName] = $Data 
		}
	} 

	[Collections.Generic.List[Object]] GetAll () {
		return $this.Data
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

class PSDatabase {
	[Collections.Generic.List[PSDatabaseTable]] $Tables = [Collections.Generic.List[PSDatabaseTable]]::new()
	$Directory

	PSDatabase ($Directory) {
		$This.Directory = $Directory
	}

	[void] Load () {
		
		$tablesToLoad = Get-Childitem $this.Directory -File

		foreach ($table in $tablesToLoad) {
			$tableContents = get-content -raw -path $table.FullName
			$DatabaseTableFromDisk = ConvertFrom-Json $tableContents -ashashtable

			$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

			$DatabaseTableInMemory = [PSDatabaseTable]::new($DatabaseTableFromDisk.Name)
			
			foreach ($Column in $DatabaseTableFromDisk.Columns) {
				$NewColumnType = [Type]::getType($Column.TypeName)
				$DatabaseTableInMemory.AddColumn([PSDatabaseColumn]::new($Column.Id, $Column.Name,$NewColumnType))
			}

			# For higher performance, dont auto refresh the index while loading data
			foreach ($Column in $DatabaseTableInMemory.Columns) { $Column.DisableAutoReindex()}

			foreach ($Row in $DatabaseTableFromDisk.Rows) {
				
				$newRow = [PSDatabaseRow]::new($row.Id)

				foreach ($Cell in $row.Cells) {
					$NewCellColumn = $DatabaseTableInMemory.GetColumnById($Cell.ColumnId)
					$NewCellRow    = $newRow
					$NewCellValue  = $cell.Value

					$newRow.AddCell([PSDatabaseCell]::new($NewCellColumn, $newCellRow, $NewCellValue))
				}

				$DatabaseTableInMemory.AddRow($newRow)
			}

			# Re-enable auto refresh and invoke a one-off refresh
			foreach ($Column in $DatabaseTableInMemory.Columns) { $Column.EnableAutoReindex(); $Column.ReIndex()}

			$Duration = $Stopwatch.ElapsedMilliseconds

			write-host "Loading database took $Duration ms"
			$this.Tables.Add($DatabaseTableInMemory)
		}
	}
	[void] Save () {

		foreach ($Table in $this.Tables) {
			$FilePath = (Join-Path $this.Directory $Table.Name)

			$Value = (ConvertTo-Json $Table -depth 4)

			Set-Content -Path $FilePath -Value $Value
		}
	}

	[PSDatabaseQuery] NewQuery() {
		return [PSDatabaseQuery]::new($this)
	}
	[PSDatabaseTable] GetTable([string] $Name) {
		return @($This.Tables.Where{$_.Name -eq $Name})[0]
	}

	[Void] AddTable ([PSDatabaseTable] $Table) {
		$Table.Database = $this
		$this.Tables += $Table
	}
}

class PSDatabaseQueryComponent {
	[string] $Type
	[scriptblock] $ScriptBlock
	[hashtable] $Variables = @{}

	PSDatabaseQueryComponent () {
		
	}

	PSDatabaseQueryComponent ([scriptblock] $Scriptblock) {
		$this.ScriptBlock = $Scriptblock
	}

	PSDatabaseQueryComponent ([string] $Type, [scriptblock] $Scriptblock) {
		$this.Type = $Type
		$this.ScriptBlock = $Scriptblock
	}

	PSDatabaseQueryComponent ([string] $Type, [scriptblock] $Scriptblock, [hashtable] $Variables) {
		$this.Type = $Type
		$this.ScriptBlock = $Scriptblock
		$this.Variables = $Variables
	}
}

class PSDatabaseQueryForeachComponent : PSDatabaseQueryComponent {
	PSDatabaseQueryForeachComponent ([scriptblock] $Scriptblock) : base("Foreach", $ScriptBlock, @{}) {	}
	PSDatabaseQueryForeachComponent ([scriptblock] $Scriptblock, [hashtable] $Variables) : base("Foreach", $ScriptBlock, $Variables) {	}

	[object[]] Invoke([object[]] $Objects, [bool] $All) {

		[Collections.Generic.List[object]] $Results = [Collections.Generic.List[object]]::new()

		foreach ($object in $objects) {
			$Results.Add($this.Invoke($Object))
		}

		return $Results.ToArray()
	}

	[object] Invoke([object] $Object) {
		$variables = [System.Collections.Generic.List[PSVariable]] @()

		$variables.Add([PSVariable]::new("_", $object))

		foreach ($VariableKV in $this.Variables.GetEnumerator()) {
			$variables.Add([PSVariable]::new($VariableKv.Name, $VariableKV.Value))
		}

		$Result = $This.ScriptBlock.InvokeWithContext($null,[PSVariable[]] $variables,$null)

		if ($Result.count -eq 1) { $result = $result[0]}

		if ($Result -ne $Null) { return $Result }
		else {return $Null}
	}

}

class PSDatabaseQueryWhereComponent : PSDatabaseQueryComponent {
	PSDatabaseQueryWhereComponent ([scriptblock] $Scriptblock) : base("Where", $ScriptBlock) {	}

	[object[]] Invoke([object[]] $Objects, [bool] $All) {

		[Collections.Generic.List[object]] $FilteredResults = [Collections.Generic.List[object]]::new()

		foreach ($object in $objects) {
			$Result = $this.Invoke($Object)
			if ($Result -ne $null) {
				$FilteredResults.Add($Result)
			}
		}

		return $FilteredResults.ToArray()
	}

	[object] Invoke([object] $Object) {
		$variables = [System.Collections.Generic.List[PSVariable]] @()
		$variables.Add([PSVariable]::new("_", $object))

		$Result = $This.ScriptBlock.InvokeWithContext($null,[PSVariable[]] $variables,$null)

		if ($Result -eq $true) { return $Object}
		else {return $Null}
	}

}

class PSDatabaseQuery {
	[hashtable[]] $ColumnQueryComponents = @()
	[PSDatabaseQueryComponent[]] $RowQueryComponents = @()
	
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDatabase] $Database
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDAtabaseTable] $Table

	PSDatabaseQuery () {}

	PSDatabaseQuery ([PSDatabase] $Database) {
		$this.Database = $Database
	}

	# Select generates a Foreach that has the same impact as a traditional select statement
	[PSDatabaseQuery] Select ([string[]] $Fields) {
		$ScriptBlock = {
			$Object = $_
			$newht = @{}
			
			if ($object -is [PSDatabaseRow]) {
				foreach ($Field in $Fields) { $newht.add($Field, $Object.GetValueByColumnName($Field)) }
			} else {
				foreach ($Field in $Fields) { $newht.add($Field, $Object.$Field) }
			}

			return $newht
		}

		$newQueryComponent = [PSDatabaseQueryForeachComponent]::new($ScriptBlock,@{"Fields" = @($Fields)})
		$this.RowQueryComponents += $newQueryComponent

		return $this
	}

	# Creates a foreach block and passes a hashtable of variables into the foreach block
	[PSDatabaseQuery] Foreach ([Scriptblock] $ScriptBlock, [hashtable] $Variables) {
		$newQueryComponent = [PSDatabaseQueryForeachComponent]::new($ScriptBlock,$Variables)
		$this.RowQueryComponents += $newQueryComponent

		return $this
	}

	[PSDatabaseQuery] Foreach ([Scriptblock] $ScriptBlock) {
		return $this.Foreach($ScriptBlock, @{})
	}

	[PSDatabaseQuery] Where ([Scriptblock] $ScriptBlock) {
		$newQueryComponent = [PSDatabaseQueryWhereComponent]::new($ScriptBlock)
		$this.RowQueryComponents += $newQueryComponent

		return $this
	}
	[PSDatabaseQuery] Where ([string] $Key, [object] $Value) {

		$this.ColumnQueryComponents += @{"ColumnName" = $Key; "Value" = $Value}
		return $this
	}

	[PSDatabaseQuery] FromTable ([string] $Name) {
		$this.Table = $this.Database.GetTable($Name)

		return $this
	}

	[object[]] Invoke () {
			
		$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
		[System.Collections.Generic.HashSet[PSDatabaseRow]] $FilteredRows = @()
		
		$firstColumnQuery = $true

		foreach ($QueryComponent in $this.ColumnQueryComponents) {
			
			$ColumnName = $QueryComponent.ColumnName
			$Column = $this.Table.GetColumnByName($ColumnName)

			$Value = $QueryComponent.Value

			$thisFilterResult = $Column.GetRowsByCellValue($Value)

			if ($FirstColumnQuery) {
				$thisFilterResult.Foreach{$FilteredRows.Add($_)}
				$firstColumnQuery = $false
			} else {
				$FilteredRows.IntersectWith($ThisFilterResult)
			}
		}

		$ColumnDuration = $stopwatch.ElapsedMilliseconds 
		[System.Collections.Generic.List[object]] $FilteredObjects = [System.Collections.Generic.List[object]]::new($FilteredRows);
		foreach ($QueryComponent in $this.RowQueryComponents) {
			$FilteredObjects = [object[]] ($QueryComponent.Invoke($FilteredObjects, $True))
		}

		$RowQueryDuration = $stopwatch.ElapsedMilliseconds - $ColumnDuration
		write-host "Query took $($Stopwatch.ElapsedMilliseconds) ms: (Column: $ColumnDuration, Row: $RowQueryDuration)"

		return $FilteredObjects.ToArray()
	}
}

class PSDatabaseTable {
	[string] $Name
	
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDatabase] $Database 

	[Collections.Generic.List[PSDatabaseColumn]] $Columns = [Collections.Generic.List[PSDatabaseColumn]]::new()

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[Hashtable] $ColumnsByName = @{}
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[Hashtable] $ColumnsById = @{}

	[Collections.Generic.List[PSDatabaseRow]] $Rows = [Collections.Generic.List[PSDatabaseRow]]::new()

	PSDatabaseTable ([string] $Name) {
		$this.Name = $Name
	}

	[object[]] Query ([scriptblock] $Query) {
		return $this.Query($Query, @{})
	}

	[object[]] Query ([scriptblock] $Query, [hashtable] $Variables) {

		$Indices = @{}
		Foreach ($column in $this.Columns) {
			$Indices.add($Column.Name, $Column.Index)
		}

		$TempVariables = [System.Collections.Generic.List[PSVariable]] @()
		$TempVariables.Add([PSVariable]::new("Rows", $this.Rows))
		$TempVariables.Add([PSVariable]::new("Columns", $this.Columns))
		$TempVariables.Add([PSVariable]::new("Table", $this.Table))
		$TempVariables.Add([PSVariable]::new("Indices", $Indices))

		foreach ($VariableKV in $Variables.GetEnumerator()) {
			$TempVariables.Add($VariableKV.Name, $VariableKV.Value)
		}
		
		$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
		$Result = $Query.InvokeWithContext($null,[PSVariable[]] $TempVariables,$null)

		$duration = $stopwatch.ElapsedMilliseconds
		write-host "Query took $duration ms"
		return $result
	}

	[PSDatabaseColumn] GetColumnByName ([string] $Name) {
		return $This.ColumnsByName[$Name]
	}

	[PSDatabaseColumn] GetColumnById ([Int] $Id) {
		return $This.ColumnsById[$Id]
	}


	[void] AddColumn ([string] $Name, [Type] $Type) {
		$newColumn = [PSDatabaseColumn]::new($this.columns.count, $Name, $Type)
		$this.AddColumn($newColumn)
	}
	
	[void] AddColumn ([PSDatabaseColumn] $Column) {
		$Column.Table = $this
		$Column.Id = $this.columns.count
		$this.Columns.Add($Column)
		$this.ColumnsByName[$Column.Name] = $Column
		$this.ColumnsById[$Column.Id] = $Column
	}

	[PSDatabaseRow[]] GetRowsByColumnValue ($ColumnName, $Value) {
		return @($this.ColumnsByName[$ColumnName]).foreach{$_.GetRowsByCellValue($Value)}
	}


	[void] AddRange( [object[]] $Object ) {
		$Object.Foreach{$this.Add($_)}
	}

	[void] AddRow ( [PSDatabaseRow] $Row ) {
		$Row.Id = $this.Rows.count
		$this.Rows.Add($Row)
		#$Row.Table = $this
	}

	[void] AddRows( [PSDatabaseRow[]] $Rows ) {
		$Rows.Foreach{ $this.AddRow($_) }
	}
}

class PSDatabaseCell {
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDatabaseColumn] $Column

	[int] $ColumnId

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDatabaseRow] $Row

	[object] $Value

	PSDatabaseCell () {
	}

	PSDatabaseCell ([PSDatabaseColumn] $Column, [PSDatabaseRow] $Row, [object] $Value) {
		$This.Column = $Column
		$this.ColumnId = $Column.Id
		$This.Row = $Row
		
		$this.Value = $Value

		if (-not ($this.Column.ValidateValue($Value))) { throw "Invalid cell value for type: $Value"}
	}
}

class PSDatabaseRow {

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[hashtable] $CellByColumnName = @{}

	[Collections.Generic.List[PSDatabaseCell]] $Cells = [Collections.Generic.List[PSDatabaseCell]]::new()
	
	[int] $Id = 0

	PSDatabaseRow () {}

	PSDatabaseRow ($id) {
		$this.id = $id
	}

	PSDatabaseRow ($id, $Cells) {
		$this.id = $id
		foreach ($Cell in $Cells) { $this.AddCell($Cell) }
	}

	[int] GetHashCode() {
		return $this.id
	}
	
	
	[object] GetCellByColumnName([string] $Name) {
		if (-not $this.CellByColumnName.ContainsKey($Name)) {
			return $Null
		}

		return $this.CellByColumnName[$Name]
	}
	[object] GetValueByColumnName([string] $Name) {
		if (-not $this.CellByColumnName.ContainsKey($Name)) {
			return $Null
		}

		return $this.CellByColumnName[$Name].Value
	}

	[PSDatabaseCell[]] GetCells() {
		return $this.Cells
	}

	[hashtable] Select([string[]] $Properties) {
		return $this.ToHashtable($Properties)
	}
	

	[psobject] ToPSObject() {
		[hashtable] $newHt = @{
			"id" = $this.Id
		}
		foreach ($Cell in $this.Cells) {
			$newHt.Add($Cell.Column.Name, $Cell.Value)
		}
		return (new-item -itemtype PSObject -properties $newHt)
	}

	[hashtable] ToHashtable() {
		return $this.ToHashtable($this.Cells.Column.Name)
	}

	[hashtable] ToHashtable([string[]] $Keys) {
		$KeysHT = @{}
		$Keys.foreach{$KeysHT[$_] = $null}

		[hashtable] $newHt = @{}

		if ($keysHt.ContainsKey("id")) { 
			$newHt.add("Id", $this.Id)
		}
		
		foreach ($Cell in $this.Cells) {
			if (-not $keysHt.ContainsKey($Cell.Column.Name)) { continue; }

			$newHt.Add($Cell.Column.Name, $Cell.Value)
		}
		return $newHt
	}

	[void] AddCell ( [PSDatabaseCell] $Cell ){
		$Cell.Row = $this
		
		# Add the Cell to this Row
		$this.Cells.add($Cell)
		# Add the Cell to this Column
		$Cell.Column.Cells.Add($Cell)
		
		# Reindex the Column
		$Cell.Column.Reindex($Cell)

		$this.CellByColumnName[$Cell.Column.Name] = $Cell
	}
}

class PSDatabaseColumn {
	[int] $Id

	[string] $Name
	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[PSDatabaseTable] $Table

	[boolean] $AutoReindex = $true

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[Type] $Type
	[String] $TypeName

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[Collections.Generic.List[PSDatabaseCell]] $Cells = [Collections.Generic.List[PSDatabaseCell]]::new()

	[Newtonsoft.Json.JsonIgnoreAttribute()]
	[hashtable] $Index = @{}

	PSDatabaseColumn () {}

	PSDatabaseColumn ([string] $Name, [Type] $Type) {
		$this.Name = $Name
		$this.Type = $Type
		$this.TypeName = $Type.FullName
	}

	PSDatabaseColumn ([int] $id, [string] $Name, [Type] $Type) {
		$this.id = $id
		$this.Name = $Name
		$this.Type = $Type
		$this.TypeName = $Type.FullName
	}

	PSDatabaseColumn ([int] $id, [string] $Name, [String] $TypeName) {
		$this.id = $id
		$this.Name = $Name
		$this.Type = [Type]::getType($TypeName)
		if ($this.Type -eq $null) { Throw "Invalid Type: $TypeName"}
		$this.TypeName = $TypeName
	}

	[bool] GetAutoReindex () {
		return $this.AutoReindex
	}
	[void] DisableAutoReindex () {
		$this.AutoReindex = $false
	}
	[void] EnableAutoReindex () {
		$this.AutoReindex = $True
	}

	[object[]] GetIDs() {
		return $this.Index.Keys;
	}

	[PSDatabaseRow[]] GetRowsByCellValue([scriptblock] $ScriptBlock) {
		$AllKeys = @($this.Index.Keys)
		$MatchingCells = @($AllKeys.Where{$_ | & $ScriptBlock})
		[PSDatabaseRow[]] $MatchingRows = $MatchingCells.foreach{$this.Index[$_]}.foreach{[PSDatabaseRow]$_.Row}
		return $MatchingRows
	}

	[PSDatabaseRow[]] GetRowsByCellValue($Value) {
		if ($this.Index.ContainsKey($Value)) {

			$TheseCells = @($this.Index[$Value])
			$TheseRows = $TheseCells.Foreach{$_.Row}

			return $TheseRows

		} else {
			return @()
		}
	}
	
	[PSDatabaseCell[]] GetValues() {
		return $this.Index.Values
	}

	[void] Reindex ([PSDatabaseCell] $Cell) {
		if (-not $this.AutoReindex) { return }

		if ($this.Index.ContainsKey($Cell.Value)) {
			$this.Index[$Cell.Value] = @($this.Index[$Cell.Value]) + @($Cell)
		} else {
			$this.Index[$Cell.Value] = @($Cell)
		}
	}

	[void] Reindex () {
		$this.Index = @{}
		foreach ($Cell in $this.Cells) { $this.Reindex($cell) }
	}
<#
	[void] AddCell ( [PSDatabaseCell] $Cell ){
		$this.Cells.Add($Cell)
		$this.Reindex($Cell)
	}#>

	[bool] ValidateValues ([object[]] $Values) {
		$ValidValues = $Values.Where{$this.ValidateValue($_)}

		return $Values.Count -eq $ValidValues.Count
	}

	[bool] ValidateValue ([object] $value) {
		$result = (($Value -as $this.Type) -is $this.Type)
		return $result
	}
}

Function New-PSDatabase {
	return [PSDatabase]::new()
}
