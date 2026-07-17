$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

Write-Host "Creating Azure Data Factory: $ADF_NAME"

az extension add --name datafactory --upgrade --yes --only-show-errors 2>$null

az datafactory create `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --location $LOCATION `
  --output table
