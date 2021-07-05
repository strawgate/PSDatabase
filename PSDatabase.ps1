using module .\PSDatabase.psm1

import-module .\PSDatabase.psm1
$ErrorActionPreference = "break"
$DataDir = (Join-Path $PSScriptRoot "Data")

New-Item -ItemType Directory -Path $DataDir -ErrorAction SilentlyContinue

$Database = [PSDatabase]::new($DataDir)
#write-host "Loading Database"
$Database.Load()
#write-host "Loading Database"
<#
$Table = [PSDatabaseTable]::New("Files")
$Database.AddTable($Table)

$Table.AddColumn(([PSDatabaseColumn]::new("name", [string])))
$Table.AddColumn(([PSDatabaseColumn]::new("basename", [string])))
$Table.AddColumn(([PSDatabaseColumn]::new("length", [int64])))

write-host "Getting items"
$Items = (Get-ChildItem -Recurse "C:\users\weaston-ou\Downloads" -file)

write-host "Processing $($Items.Count) items."

$i = 0
foreach ($Item in $Items) {
	if ($i % 1000 -eq 0) { 
		write-host "$i / $($Items.count)"
	}
	try {
	$NewRow = [PSDatabaseRow]::new()
	$NewRow.AddCell([PSDatabaseCell]::new($Table.GetColumnByName("name"), $newRow, $Item.name))
	$NewRow.AddCell([PSDatabaseCell]::new($Table.GetColumnByName("basename"), $newRow, $Item.basename))
	$NewRow.AddCell([PSDatabaseCell]::new($Table.GetColumnByName("length"), $newRow, $Item.length))

	$Table.AddRow($NewRow)
	} catch{
		write-host "Failed to process $($Item.name)"
	}
	$i++
}
write-host "Done getting items"
$Database.Save()
write-host "Done Saving database items"

#>
#$Database
<#
write-host "Query"
$Result = $Database.GetTable("Files").Query({
	$Rows.Where{
		$_.GetCellByColumn("name").Value -eq "11032434_William_Easton_17416351_202102231119_Resume (1).pdf"
	}.Where{
		$_.GetCellByColumn("length").Value -eq 170140
	}.Foreach{
		$_.ToHashtable()
	}
})

convertto-json $result

$Result = $Database.GetTable("Files").Query({
	$Table.GetColumnByName("name").GetRowByCellValue("11032434_William_Easton_17416351_202102231119_Resume (1).pdf").Select(@("id","name","length"))
})

convertto-json $result


$Result = $Database.GetTable("Files").Query({
	$Rows.Where{
		$_.GetValueByColumn("name") -eq "11032434_William_Easton_17416351_202102231119_Resume (1).pdf" -and
		$_.GetValueByColumn("length") -eq 170140
	}.Select(
		@("id","name","length")
	)
})

convertto-json $result


$Result = $Database.GetTable("Files").Query({
	$Table.GetRowsByColumnValue("name", "11032434_William_Easton_17416351_202102231119_Resume (1).pdf")
	$Rows.Where{
		$_.GetValueByColumn("name") -eq "11032434_William_Easton_17416351_202102231119_Resume (1).pdf" -and
		$_.GetValueByColumn("length") -eq 170140
	}.Select(
		@("id","name","length")
	)
})

convertto-json $result#>
#write-host "Done"<#
[psdatabaserow[]] $Result = $Database.GetTable("Files").Query{ $Rows.Where{$_.GetValueByColumnName("name") -like "ipIfStatsTable_data_get.h" -and $_.GetValueByColumnName("length") -eq ([int64] 13606) } }
#convertto-json $Result -depth 10
#>
#$Result = $Database.NewQuery().FromTable("Files").Invoke()
#convertto-json $Result
[object[]] $Result = $Database.NewQuery().FromTable("Files").Where("name","ipIfStatsTable_data_get.h").Select("name").Invoke()
#convertto-json $Result
<#
$Result = $Database.NewQuery().FromTable("Files").Where("name","ipIfStatsTable_data_get.h").Where("length", [int64]13606).Foreach{"tomato"}.Invoke()
convertto-json $Result

$Result = $Database.NewQuery().FromTable("Files").Where{$_.GetValueByColumnName("name") -like "ipIfStatsTable_data_get.h"}.Where("length", [int64]13606).Foreach{"tomato"}.Invoke()
convertto-json $Result
#>
$Items = (Get-ChildItem -Recurse "C:\Users\weaston-ou\Downloads" -file)

Measure-Command {
	$Items.Where{$_.Name -eq "ipIfStatsTable_data_get.h"}.Foreach{@{"name" = $_.name}}
}