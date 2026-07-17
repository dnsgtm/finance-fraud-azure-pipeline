$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

$adfPrincipalId = az datafactory show `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --query identity.principalId -o tsv

$vaultId = az keyvault show --name $KEYVAULT_NAME --resource-group $RESOURCE_GROUP --query id -o tsv

az role assignment create `
  --role "Key Vault Secrets User" `
  --assignee $adfPrincipalId `
  --scope $vaultId

Write-Host "ADF managed identity granted read access to Key Vault secrets."