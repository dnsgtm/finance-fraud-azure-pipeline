$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

Write-Host "Creating Azure Databricks workspace: $DATABRICKS_NAME (SKU: $DATABRICKS_SKU)"

az extension add --name databricks --upgrade --yes --only-show-errors 2>$null

az databricks workspace create `
  --resource-group $RESOURCE_GROUP `
  --name $DATABRICKS_NAME `
  --location $LOCATION `
  --sku $DATABRICKS_SKU `
  --output table

Write-Host ""
Write-Host "Reminder: after this workspace is up, create clusters with:"
Write-Host "  - Single node mode"
Write-Host "  - Smallest available VM size (e.g. Standard_DS3_v2)"
Write-Host "  - Auto-termination set to 15-20 minutes"
Write-Host "to keep credit usage under control."
