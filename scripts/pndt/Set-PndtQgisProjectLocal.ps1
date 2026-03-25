param(
    [string]$InputProject = "D:\utforsk\PNDT_entrega\portal\pndtservices\qgis\qgisMap4.qgs",
    [string]$OutputProject = "D:\utforsk\PNDT_entrega_output\qgisMap4.local.qgs",
    [string]$OldHost = "151.236.33.117",
    [string]$NewHost = "localhost"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $InputProject)) {
    throw "Input QGIS project not found: $InputProject"
}

$outputDir = Split-Path -Parent $OutputProject
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$content = Get-Content -LiteralPath $InputProject -Raw
$before = ([regex]::Matches($content, [regex]::Escape("host=$OldHost"))).Count
$updated = $content.Replace("host=$OldHost", "host=$NewHost")
$after = ([regex]::Matches($updated, [regex]::Escape("host=$NewHost"))).Count

Set-Content -LiteralPath $OutputProject -Value $updated -Encoding UTF8

Write-Host "Wrote localized QGIS project: $OutputProject"
Write-Host "  Remote host refs replaced : $before"
Write-Host "  Local host refs now       : $after"
