$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
. "$ScriptDir\variables.ps1"

Write-Host "Creating resource group: $RESOURCE_GROUP in $LOCATION"

az group create `
  --name $RESOURCE_GROUP `
  --location $LOCATION `
  --output table
