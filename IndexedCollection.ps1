using module .\IndexedCollection.psm1

import-module .\IndexedCollection.psm1 -force

new-item -ItemType directory -path (Join-path $PSScriptRoot "Collections") -ErrorAction SilentlyContinue

$NewCollection = [IndexedCollection]::new(".\Collections")

$NewCollection.AddIndex("Name")
$NewCollection.AddIndex("Length")
$NewCollection.AddIndex("BaseName")

#$NewCollection.Load()

$Items = Get-ChildItem "C:\Users\weaston-ou\Downloads\en_windows_10_business_editions_version_20h2_updated_april_2021_x64_dvd_61562f02" -recurse -file

$MyObjects = $Items.Foreach{
    @{
        "Name" = $_.Name
        "Length" = $_.Length
        "BaseName" = $_.BaseName
        "CreationTimeUtc" = $_.CreationTimeUtc
        "FullName" = $_.FullName
        "Extension" = $_.Extension
    }
}
$measure = measure-command {
    $NewCollection.AutoFlush = $false
    $MyObjects.Foreach{
        $NewCollection.Add($_, $true)
    }

    $NewCollection.AutoFlush = $true
    $NewCollection.Flush()
}
write-host "Took $($measure.TotalMilliseconds)ms"

$result = $NewCollection.Get("Name", "wgl4_boot.ttf")

write-host "Starting Query"
$measure = measure-command {
$Result = $NewCollection.GetAll().Where{
    $_.GetValue("BaseName") -eq "bootres"
}.Foreach{
    $_.GetValue()
}

}

$NewCollection.GetMemoryReport()

write-host "Took $($measure.TotalMilliseconds)ms"

write-host "Collection Loaded with $($NewCollection.Data.Count) Entries"
#convertto-json $Result -depth 5 
write-host $PID
$result.Indices