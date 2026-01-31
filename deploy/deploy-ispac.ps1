param(
  [Parameter(Mandatory=$true)][string]$SqlServer,
  [Parameter(Mandatory=$true)][string]$SqlUser,
  [Parameter(Mandatory=$true)][string]$SqlPassword,
  [Parameter(Mandatory=$true)][string]$PackagesJsonPath
)

function Exec-Sql {
  param([Parameter(Mandatory=$true)][string]$Query)

  $env:SQLCMDPASSWORD = $SqlPassword
  sqlcmd -S $SqlServer -d "SSISDB" -U $SqlUser -C -b -l 180 -Q $Query

  if ($LASTEXITCODE -ne 0) {
    throw "sqlcmd failed."
  }
}

$cfg = Get-Content $PackagesJsonPath -Raw | ConvertFrom-Json

foreach ($p in $cfg.projects) {
  $folder = $p.folder
  $projectName = $p.projectName
  $ispacPath = $p.ispacPath

  if (-not (Test-Path $ispacPath)) {
    throw "ISPAC not found: $ispacPath"
  }

  Write-Host "Deploying: Folder=$folder, Project=$projectName, File=$ispacPath"

  [byte[]]$bytes = [System.IO.File]::ReadAllBytes($ispacPath)
  $hex = "0x" + ([System.BitConverter]::ToString($bytes) -replace "-", "")

  $tsql = @"
IF NOT EXISTS (SELECT 1 FROM SSISDB.catalog.folders WHERE name = N'$folder')
  EXEC SSISDB.catalog.create_folder @folder_name = N'$folder';

DECLARE @ispac varbinary(max) = $hex;

EXEC SSISDB.catalog.deploy_project
  @folder_name = N'$folder',
  @project_name = N'$projectName',
  @project_stream = @ispac;
"@

  Exec-Sql -Query $tsql

  Write-Host "✅ Deployed $projectName"
}

Write-Host "🎉 All ISPAC deployments completed."
