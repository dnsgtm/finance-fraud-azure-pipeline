$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

$storageKey = az storage account keys list `
  --resource-group $RESOURCE_GROUP `
  --account-name $STORAGE_ACCOUNT `
  --query "[0].value" -o tsv

az keyvault secret set `
  --vault-name $KEYVAULT_NAME `
  --name "storagefinancialfraud-accountkey" `
  --value $storageKey `
  --output none

Write-Host "Storage account key loaded into Key Vault."

$dbToken = Read-Host -Prompt "Paste Databricks personal access token" -AsSecureString
$BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($dbToken)
$dbTokenPlain = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)

az keyvault secret set `
  --vault-name $KEYVAULT_NAME `
  --name "databricks-access-token" `
  --value $dbTokenPlain `
  --output none

Write-Host "Databricks token loaded into Key Vault."