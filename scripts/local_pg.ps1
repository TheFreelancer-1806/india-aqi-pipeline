<#
.SYNOPSIS
    Run a throwaway local PostgreSQL instance for developing this pipeline.

.DESCRIPTION
    Downloads the official EnterpriseDB PostgreSQL Windows *binaries* zip (no
    installer, no administrator rights, no system service), initialises a data
    directory under .local\ and runs it on a non-default port so it cannot
    collide with a real Postgres install.

    Everything it creates lives in .local\, which is gitignored.

        .local\dl\pg.zip      downloaded archive
        .local\pgroot\pgsql   extracted binaries
        .local\pgdata         database cluster
        .local\pg.log         server log

.PARAMETER Command
    setup    Download + extract binaries and initialise the cluster.
    start    Start the server (runs setup first if needed) and create databases.
    stop     Stop the server.
    status   Report whether the server is accepting connections.
    psql     Open an interactive psql session against aqi_dev.
    url      Print the DATABASE_URL for the dev database.
    destroy  Stop the server and delete the cluster (keeps the binaries).

.EXAMPLE
    .\scripts\local_pg.ps1 start
    $env:DATABASE_URL = .\scripts\local_pg.ps1 url
    python -m src.etl --backfill
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('setup', 'start', 'stop', 'status', 'psql', 'url', 'destroy')]
    [string]$Command = 'status',

    [int]$Port = 55432
)

$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LocalDir    = Join-Path $ProjectRoot '.local'
$ZipPath     = Join-Path $LocalDir 'dl\pg.zip'
$PgRoot      = Join-Path $LocalDir 'pgroot\pgsql'
$PgBin       = Join-Path $PgRoot 'bin'
$PgData      = Join-Path $LocalDir 'pgdata'
$LogFile     = Join-Path $LocalDir 'pg.log'

$PgVersion   = '16.4-1'
$ZipUrl      = "https://get.enterprisedb.com/postgresql/postgresql-$PgVersion-windows-x64-binaries.zip"

$DevDb   = 'aqi_dev'
$TestDb  = 'aqi_test'
$DbUser  = 'postgres'

function Get-Exe([string]$name) { Join-Path $PgBin "$name.exe" }

function Assert-Binaries {
    if (Test-Path (Get-Exe 'pg_ctl')) { return }

    Write-Host "PostgreSQL binaries not found. Downloading $PgVersion (about 340 MB)..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Force (Split-Path -Parent $ZipPath) | Out-Null
    if (-not (Test-Path $ZipPath)) {
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $ZipUrl -OutFile $ZipPath -UseBasicParsing
    }
    Write-Host 'Extracting...' -ForegroundColor Cyan
    Expand-Archive -Path $ZipPath -DestinationPath (Join-Path $LocalDir 'pgroot') -Force
    if (-not (Test-Path (Get-Exe 'pg_ctl'))) {
        throw "Extraction finished but pg_ctl.exe is missing under $PgBin"
    }
}

function Assert-Cluster {
    Assert-Binaries
    if (Test-Path (Join-Path $PgData 'PG_VERSION')) { return }

    Write-Host "Initialising cluster at $PgData ..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Force $PgData | Out-Null
    # trust auth is acceptable here and only here: the server listens on
    # 127.0.0.1, on a non-default port, and holds nothing but public AQI data.
    & (Get-Exe 'initdb') --pgdata=$PgData --username=$DbUser --auth=trust `
        --encoding=UTF8 --locale=C | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "initdb failed with exit code $LASTEXITCODE" }
}

function Test-ServerUp {
    if (-not (Test-Path (Get-Exe 'pg_isready'))) { return $false }
    & (Get-Exe 'pg_isready') --host=127.0.0.1 --port=$Port --username=$DbUser --quiet 2>$null
    return ($LASTEXITCODE -eq 0)
}

function Start-Server {
    Assert-Cluster
    if (Test-ServerUp) {
        Write-Host "Already running on port $Port." -ForegroundColor Green
    }
    else {
        Write-Host "Starting PostgreSQL on 127.0.0.1:$Port ..." -ForegroundColor Cyan
        & (Get-Exe 'pg_ctl') --pgdata=$PgData --log=$LogFile `
            --options="-p $Port -c listen_addresses=127.0.0.1" -w start
        if ($LASTEXITCODE -ne 0) {
            Write-Host "--- tail of $LogFile ---" -ForegroundColor Yellow
            if (Test-Path $LogFile) { Get-Content $LogFile -Tail 30 }
            throw "pg_ctl start failed with exit code $LASTEXITCODE"
        }
    }

    foreach ($db in @($DevDb, $TestDb)) {
        $exists = & (Get-Exe 'psql') --host=127.0.0.1 --port=$Port --username=$DbUser `
            --dbname=postgres --tuples-only --no-align `
            --command="SELECT 1 FROM pg_database WHERE datname = '$db'"
        # psql prints nothing when the database is absent, so $exists is $null.
        $exists = ($exists | Out-String).Trim()
        if ($exists -ne '1') {
            & (Get-Exe 'createdb') --host=127.0.0.1 --port=$Port --username=$DbUser $db
            Write-Host "Created database $db." -ForegroundColor Green
        }
    }

    Write-Host ''
    Write-Host 'Ready. Point the pipeline at it with:' -ForegroundColor Green
    Write-Host "  `$env:DATABASE_URL      = '$(Get-DevUrl)'"
    Write-Host "  `$env:TEST_DATABASE_URL = '$(Get-TestUrl)'"
}

function Stop-Server {
    if (-not (Test-Path (Join-Path $PgData 'PG_VERSION'))) {
        Write-Host 'No cluster to stop.' -ForegroundColor Yellow
        return
    }
    if (-not (Test-ServerUp)) {
        Write-Host "Not running on port $Port; nothing to stop." -ForegroundColor Yellow
        return
    }
    & (Get-Exe 'pg_ctl') --pgdata=$PgData --mode=fast -w stop
    # Report what actually happened. Reporting success on a failed pg_ctl is
    # worse than reporting nothing: it sends you looking for the problem
    # somewhere else.
    if ($LASTEXITCODE -ne 0) {
        throw "pg_ctl stop failed with exit code $LASTEXITCODE; the server may still be running."
    }
    if (Test-ServerUp) {
        throw "pg_ctl reported success but the server is still accepting connections on port $Port."
    }
    Write-Host 'Stopped.' -ForegroundColor Green
}

function Get-DevUrl  { "postgresql://$DbUser@127.0.0.1:$Port/$DevDb" }
function Get-TestUrl { "postgresql://$DbUser@127.0.0.1:$Port/$TestDb" }

switch ($Command) {
    'setup'  { Assert-Cluster; Write-Host 'Cluster ready (not started).' -ForegroundColor Green }
    'start'  { Start-Server }
    'stop'   { Stop-Server }
    'url'    { Write-Output (Get-DevUrl) }
    'psql'   { & (Get-Exe 'psql') --host=127.0.0.1 --port=$Port --username=$DbUser --dbname=$DevDb }
    'status' {
        if (Test-ServerUp) { Write-Host "PostgreSQL is up on 127.0.0.1:$Port." -ForegroundColor Green }
        else { Write-Host "PostgreSQL is not responding on 127.0.0.1:$Port." -ForegroundColor Yellow }
    }
    'destroy' {
        if (Test-ServerUp) { Stop-Server }
        if (Test-Path $PgData) {
            Remove-Item -Recurse -Force $PgData -Confirm:$false
            Write-Host 'Cluster deleted. Binaries kept under .local\pgroot.' -ForegroundColor Green
        }
    }
}
