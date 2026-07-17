$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

Write-Host "Creating medallion containers in $STORAGE_ACCOUNT"

foreach ($container in $CONTAINERS) {
    Write-Host "  -> $container"
    az storage container create `
      --account-name $STORAGE_ACCOUNT `
      --name $container `
      --auth-mode login `
      --output none
}

Write-Host "Done. Containers created: $($CONTAINERS -join ', ')"
