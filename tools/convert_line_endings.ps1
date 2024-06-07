Get-ChildItem -Path '..\docker\*.sh' -Recurse | ForEach-Object {
    $content = Get-Content -Raw -Path $_.FullName
    Write-Host "Processing file: $($_.FullName)"
    $content = $content -replace "`r`n", "`n"
    Set-Content -NoNewline -Path $_.FullName -Value $content
    Write-Host "File processed: $($_.FullName)"
}
