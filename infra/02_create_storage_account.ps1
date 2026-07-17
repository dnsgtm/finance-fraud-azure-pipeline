$ErrorActionPreference = "Continue"   # this step is expected to "fail" (already exists) - don't stop the run
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"


Write-Host "Creating ADLS Gen2 storage account: $STORAGE_ACCOUNT"

az storage account create `
  --name $STORAGE_ACCOUNT `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku $STORAGE_SKU `
  --kind StorageV2 `
  --hierarchical-namespace true `
  --access-tier Hot `
  --output table

$ErrorActionPreference = "Stop"
