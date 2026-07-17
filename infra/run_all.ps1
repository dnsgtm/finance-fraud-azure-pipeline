$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "=== Verifying Azure CLI login ==="
try {
    az account show --output table
} catch {
    Write-Host "Run 'az login' first."
    exit 1
}

Write-Host ""
$confirm = Read-Host "This will provision resources into your Azure subscription. Continue? (y/n)"
if ($confirm -ne "y") {
    Write-Host "Aborted."
    exit 0
}

& "$ScriptDir\01_create_resource_group.ps1"
& "$ScriptDir\02_create_storage_account.ps1"
& "$ScriptDir\03_create_containers.ps1"
& "$ScriptDir\04_create_adf.ps1"
& "$ScriptDir\05_create_databricks.ps1"
& "$ScriptDir\06_create_synapse.ps1"

Write-Host ""
Write-Host "=== All resources provisioned ==="
. "$ScriptDir\variables.ps1"
az resource list --resource-group $RESOURCE_GROUP --output table
