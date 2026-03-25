param(
    [string]$SourceRoot = "D:\utforsk\PNDT_entrega",
    [string]$OutputDir = "D:\utforsk\PNDT_entrega_output"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-Count {
    param(
        [string]$Path,
        [string]$Filter
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return 0
    }

    return (Get-ChildItem -LiteralPath $Path -Recurse -File -Filter $Filter | Measure-Object).Count
}

function Get-ExtensionCounts {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }

    return Get-ChildItem -LiteralPath $Path -Recurse -File |
        Group-Object Extension |
        Sort-Object Count -Descending |
        ForEach-Object {
            [pscustomobject]@{
                extension = if ([string]::IsNullOrWhiteSpace($_.Name)) { "<no extension>" } else { $_.Name }
                count = $_.Count
            }
        }
}

function Get-TableManifestFromPlainDump {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }

    $rows = New-Object System.Collections.Generic.List[object]
    $current = $null

    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^CREATE TABLE\s+(.+?)\s+\(') {
            $fullName = $matches[1]
            $schema = "public"
            $table = $fullName

            if ($fullName -match '^"([^"]+)"\."([^"]+)"$') {
                $schema = $matches[1]
                $table = $matches[2]
            }
            elseif ($fullName -match '^([^\.]+)\."([^"]+)"$') {
                $schema = $matches[1]
                $table = $matches[2]
            }
            elseif ($fullName -match '^"([^"]+)"\.([^\.]+)$') {
                $schema = $matches[1]
                $table = $matches[2]
            }
            elseif ($fullName -match '^([^\.]+)\.([^\.]+)$') {
                $schema = $matches[1]
                $table = $matches[2]
            }

            $current = [ordered]@{
                schema = $schema
                table = $table
                has_geometry = $false
            }
            continue
        }

        if ($null -ne $current -and $line -match 'geometry\(') {
            $current["has_geometry"] = $true
            continue
        }

        if ($null -ne $current -and $line -eq ');') {
            $rows.Add([pscustomobject]$current)
            $current = $null
        }
    }

    return $rows
}

$camadasPath = Join-Path $SourceRoot "Camadas"
$portalPath = Join-Path $SourceRoot "portal"
$plainDumpPath = Join-Path $portalPath "pndtdb-2020-01-16.sql"
$customDumpPath = Join-Path $SourceRoot "pndtdb.sql"
$qgisPath = Join-Path $portalPath "pndtservices\qgis\qgisMap4.qgs"
$serviceRastersPath = Join-Path $portalPath "pndtservices\mapserver\rasters"

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$tableManifest = Get-TableManifestFromPlainDump -Path $plainDumpPath
$spatialManifest = @($tableManifest | Where-Object { $_.has_geometry })

$summary = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    source_root = $SourceRoot
    paths = [ordered]@{
        camadas = $camadasPath
        portal = $portalPath
        custom_dump = $customDumpPath
        plain_dump = $plainDumpPath
        qgis_project = $qgisPath
        service_rasters = $serviceRastersPath
    }
    inventory = [ordered]@{
        camadas_shapefiles = Get-Count -Path $camadasPath -Filter "*.shp"
        camadas_prj = Get-Count -Path $camadasPath -Filter "*.prj"
        camadas_dbf = Get-Count -Path $camadasPath -Filter "*.dbf"
        camadas_tif = Get-Count -Path $camadasPath -Filter "*.tif"
        service_rasters_tif = Get-Count -Path $serviceRastersPath -Filter "*.tif"
        mapserver_layer_files = Get-Count -Path (Join-Path $portalPath "pndtservices\mapserver\layers") -Filter "*.map"
        wordpress_sql_present = Test-Path -LiteralPath (Join-Path $portalPath "pndt.sql")
        custom_dump_present = Test-Path -LiteralPath $customDumpPath
        plain_dump_present = Test-Path -LiteralPath $plainDumpPath
    }
    qgis = [ordered]@{
        project_exists = Test-Path -LiteralPath $qgisPath
        remote_host_refs = if (Test-Path -LiteralPath $qgisPath) { (Select-String -LiteralPath $qgisPath -Pattern 'host=151\.236\.33\.117' | Measure-Object).Count } else { 0 }
        localhost_refs = if (Test-Path -LiteralPath $qgisPath) { (Select-String -LiteralPath $qgisPath -Pattern 'host=localhost' | Measure-Object).Count } else { 0 }
        postgres_layer_refs = if (Test-Path -LiteralPath $qgisPath) { (Select-String -LiteralPath $qgisPath -Pattern 'providerKey="postgres"' | Measure-Object).Count } else { 0 }
    }
    postgis_dump = [ordered]@{
        total_tables = $tableManifest.Count
        spatial_tables = $spatialManifest.Count
        schemas = @($tableManifest | Group-Object schema | Sort-Object Name | ForEach-Object {
            [pscustomobject]@{
                schema = $_.Name
                total_tables = $_.Count
                spatial_tables = @($_.Group | Where-Object { $_.has_geometry }).Count
            }
        })
    }
    camadas_extensions = @(Get-ExtensionCounts -Path $camadasPath)
}

$summaryPath = Join-Path $OutputDir "pndt_inventory.json"
$tablePath = Join-Path $OutputDir "pndt_tables.csv"
$spatialPath = Join-Path $OutputDir "pndt_spatial_tables.csv"

$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
$tableManifest | Export-Csv -LiteralPath $tablePath -NoTypeInformation -Encoding UTF8
$spatialManifest | Export-Csv -LiteralPath $spatialPath -NoTypeInformation -Encoding UTF8

Write-Host "Wrote inventory:"
Write-Host "  $summaryPath"
Write-Host "  $tablePath"
Write-Host "  $spatialPath"
Write-Host ""
Write-Host "Headline counts:"
Write-Host "  Camadas shapefiles : $($summary.inventory.camadas_shapefiles)"
Write-Host "  Camadas TIFFs      : $($summary.inventory.camadas_tif)"
Write-Host "  Service TIFFs      : $($summary.inventory.service_rasters_tif)"
Write-Host "  Total dump tables  : $($summary.postgis_dump.total_tables)"
Write-Host "  Spatial dump tables: $($summary.postgis_dump.spatial_tables)"
