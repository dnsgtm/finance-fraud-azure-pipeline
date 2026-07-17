# Infrastructure scripts can be used for Windows machine (PowerShell)

## Prerequisites

- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows) installed
  (`winget install Microsoft.AzureCLI`), verify with `az --version`
- Logged in: `az login`
- Correct subscription: `az account set --subscription "<name-or-id>"`

## One-time setup: PowerShell execution policy

Windows blocks running .ps1 scripts by default. Open PowerShell **as Administrator**
and run this once if not already done

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

This allows locally-created scripts to run while still blocking unsigned scripts
downloaded from the internet - a reasonable, commonly-used setting. If you see
an error like "script is not digitally signed" or "cannot be loaded because
running scripts is disabled", this is the fix.

## Usage

Run everything in order:

```powershell
cd infra
.\run_all.ps1
```

Or run scripts individually, in numeric order:

| Script | Creates |
|---|---|
| `01_create_resource_group.ps1` | Resource group |
| `02_create_storage_account.ps1`
| `03_create_containers.ps1` | `landing`, `bronze`, `silver`, `gold`, `checkpoints` containers |
| `04_create_adf.ps1` | Azure Data Factory instance |
| `05_create_databricks.ps1` | Azure Databricks workspace (Standard SKU) |
| `06_create_synapse.ps1` | Azure Synapse workspace (serverless SQL only) |

## Config

All resource names/locations live in `variables.ps1`. Edit that file to change
naming or region - it's dot-sourced (`. .\variables.ps1`) by every other script.

## Cost notes

Databricks and Synapse are the services that cost money; storage and ADF are negligible for this data volume; no Synapse dedicated SQL pool is created; Databricks clusters should be single-node with short auto-termination (set when creating the cluster in the workspace UI).

`06_create_synapse.ps1` prompts for the SQL admin password using a masked
`Read-Host -AsSecureString` prompt - it is never written to disk or committed.
