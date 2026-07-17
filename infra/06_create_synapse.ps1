$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

Write-Host "Creating dedicated filesystem container for Synapse: $SYNAPSE_FS"
az storage container create `
  --account-name $STORAGE_ACCOUNT `
  --name $SYNAPSE_FS `
  --auth-mode login `
  --output none

# Prompt securely - never hardcode this in a script that goes into git.
$SecurePassword = Read-Host -Prompt "Enter SQL admin password for Synapse (min 8 chars, upper/lower/digit/symbol)" -AsSecureString
$BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
$SQL_ADMIN_PASSWORD = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)

Write-Host "Creating Synapse workspace: $SYNAPSE_WORKSPACE"

az synapse workspace create `
  --name $SYNAPSE_WORKSPACE `
  --resource-group $RESOURCE_GROUP `
  --storage-account $STORAGE_ACCOUNT `
  --file-system $SYNAPSE_FS `
  --sql-admin-login-user $SQL_ADMIN_USER `
  --sql-admin-login-password $SQL_ADMIN_PASSWORD `
  --location $LOCATION `
  --output table

Write-Host ""
Write-Host "NOTE: this creates the workspace only - it does NOT create a dedicated"
Write-Host "SQL pool. Use the built-in serverless SQL endpoint for this project;"
Write-Host "a dedicated pool bills hourly just for existing and is not needed here."
