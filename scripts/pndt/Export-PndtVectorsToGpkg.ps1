param(
    [string]$SourceRoot = "D:\utforsk\PNDT_entrega",
    [string]$DumpPath = "D:\utforsk\PNDT_entrega\pndtdb.sql",
    [string]$PlainDumpPath = "D:\utforsk\PNDT_entrega\portal\pndtdb-2020-01-16.sql",
    [string]$OutputGpkg = "D:\utforsk\PNDT_entrega_output\pndt_vectors.gpkg",
    [string]$ManifestCsv = "D:\utforsk\PNDT_entrega_output\pndt_postgis_layers.csv",
    [string]$DatabaseName = "pndtdb",
    [string]$Host = "localhost",
    [string]$Port = "5432",
    [string]$User = "postgres",
    [string]$Password = "",
    [string]$TargetSrs = "EPSG:3857",
    [string[]]$ExcludeSchemas = @("public"),
    [switch]$SkipRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "Required command not found on PATH: $Name"
    }

    return $command.Source
}

function Quote-PgIdent {
    param([string]$Value)
    return '"' + $Value.Replace('"', '""') + '"'
}

function Convert-ToLayerName {
    param(
        [string]$Schema,
        [string]$Table
    )

    $raw = "{0}__{1}" -f $Schema, $Table
    $normalized = $raw.Normalize([Text.NormalizationForm]::FormD)
    $builder = New-Object System.Text.StringBuilder

    foreach ($char in $normalized.ToCharArray()) {
        $category = [Globalization.CharUnicodeInfo]::GetUnicodeCategory($char)
        if ($category -ne [Globalization.UnicodeCategory]::NonSpacingMark) {
            [void]$builder.Append($char)
        }
    }

    $ascii = $builder.ToString()
    $ascii = $ascii -replace '[^A-Za-z0-9_]', '_'
    $ascii = $ascii -replace '_{2,}', '_'
    $ascii = $ascii.Trim('_')

    if ([string]::IsNullOrWhiteSpace($ascii)) {
        throw "Could not derive a safe layer name from $Schema.$Table"
    }

    return $ascii
}

function Invoke-Step {
    param(
        [string]$Executable,
        [string[]]$Arguments
    )

    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $Executable $($Arguments -join ' ')"
    }
}

$psql = Require-Command -Name "psql"
$pgRestore = if (-not $SkipRestore) { Require-Command -Name "pg_restore" } else { $null }
$createdb = if (-not $SkipRestore) { Require-Command -Name "createdb" } else { $null }
$ogr2ogr = Require-Command -Name "ogr2ogr"

$outputDir = Split-Path -Parent $OutputGpkg
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

if ($Password) {
    $env:PGPASSWORD = $Password
}

if (-not $SkipRestore) {
    if (Test-Path -LiteralPath $DumpPath) {
        Write-Host "Restoring PostgreSQL custom dump: $DumpPath"
        Invoke-Step -Executable $pgRestore -Arguments @(
            "--clean",
            "--if-exists",
            "--create",
            "--dbname=postgres",
            "--host=$Host",
            "--port=$Port",
            "--username=$User",
            $DumpPath
        )
    }
    elseif (Test-Path -LiteralPath $PlainDumpPath) {
        Write-Host "Restoring PostgreSQL plain dump: $PlainDumpPath"
        Invoke-Step -Executable $psql -Arguments @(
            "--host=$Host",
            "--port=$Port",
            "--username=$User",
            "--dbname=postgres",
            "-c",
            "DROP DATABASE IF EXISTS $DatabaseName;"
        )
        Invoke-Step -Executable $createdb -Arguments @(
            "--host=$Host",
            "--port=$Port",
            "--username=$User",
            $DatabaseName
        )
        Invoke-Step -Executable $psql -Arguments @(
            "--host=$Host",
            "--port=$Port",
            "--username=$User",
            "--dbname=$DatabaseName",
            "--file=$PlainDumpPath"
        )
    }
    else {
        throw "Neither dump file exists. Checked: $DumpPath and $PlainDumpPath"
    }
}

$excludeList = ($ExcludeSchemas | ForEach-Object { "'" + $_.Replace("'", "''") + "'" }) -join ","
$query = @"
SELECT
    f_table_schema,
    f_table_name,
    f_geometry_column,
    srid,
    type
FROM geometry_columns
WHERE f_table_schema NOT IN ($excludeList)
ORDER BY f_table_schema, f_table_name;
"@

$rows = & $psql `
    "--host=$Host" `
    "--port=$Port" `
    "--username=$User" `
    "--dbname=$DatabaseName" `
    "-At" `
    "-F" "`t" `
    "-c" $query

if ($LASTEXITCODE -ne 0) {
    throw "psql failed while reading geometry_columns"
}

$manifest = @()
foreach ($row in $rows) {
    if ([string]::IsNullOrWhiteSpace($row)) {
        continue
    }

    $parts = $row -split "`t"
    if ($parts.Count -lt 5) {
        throw "Unexpected psql row format: $row"
    }

    $schema = $parts[0]
    $table = $parts[1]
    $geometryColumn = $parts[2]
    $srid = $parts[3]
    $geometryType = $parts[4]
    $layerName = Convert-ToLayerName -Schema $schema -Table $table

    $manifest += [pscustomobject]@{
        schema = $schema
        table = $table
        geometry_column = $geometryColumn
        srid = $srid
        geometry_type = $geometryType
        gpkg_layer = $layerName
    }
}

if (-not $manifest) {
    throw "No spatial layers found in geometry_columns"
}

$manifest | Export-Csv -LiteralPath $ManifestCsv -NoTypeInformation -Encoding UTF8
Write-Host "Wrote layer manifest: $ManifestCsv"

if (Test-Path -LiteralPath $OutputGpkg) {
    Remove-Item -LiteralPath $OutputGpkg -Force
}

$pgConnection = 'PG:host={0} port={1} dbname={2} user={3}' -f $Host, $Port, $DatabaseName, $User
if ($Password) {
    $pgConnection += ' password=' + $Password
}

$index = 0
foreach ($layer in $manifest) {
    $index += 1
    $sql = "SELECT * FROM {0}.{1}" -f (Quote-PgIdent -Value $layer.schema), (Quote-PgIdent -Value $layer.table)

    $args = @(
        "--config", "PG_USE_COPY", "YES",
        "-gt", "65536",
        "-f", "GPKG",
        $OutputGpkg,
        $pgConnection,
        "-sql", $sql,
        "-nln", $layer.gpkg_layer,
        "-t_srs", $TargetSrs,
        "-lco", "SPATIAL_INDEX=YES",
        "-progress"
    )

    if ($index -gt 1) {
        $args = @("-append") + $args
    }

    Write-Host ("[{0}/{1}] Exporting {2}.{3} -> {4}" -f $index, $manifest.Count, $layer.schema, $layer.table, $layer.gpkg_layer)
    Invoke-Step -Executable $ogr2ogr -Arguments $args
}

Write-Host ""
Write-Host "Vector GeoPackage export complete: $OutputGpkg"
Write-Host "Next step for style fidelity:"
Write-Host "  1. Create a localhost QGIS project copy with scripts\\pndt\\Set-PndtQgisProjectLocal.ps1"
Write-Host "  2. Open that project in QGIS"
Write-Host "  3. Use Package Layers with Save layer styles into GeoPackage enabled"
Write-Host "  4. Store the QGIS project in the same GeoPackage if you need exact project reproduction"
