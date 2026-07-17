# Shared configuration for all infra scripts.
# Dot-source this in each script: . .\variables.ps1

$RESOURCE_GROUP   = "rg-finance-fraud-analytics-dev"
$LOCATION         = "australiaeast"

# Storage (ADLS Gen2) - already created manually via portal.
$STORAGE_ACCOUNT  = "storagefinancialfraud"
$STORAGE_SKU      = "Standard_LRS"

# Medallion containers
$CONTAINERS = @("landing", "bronze", "silver", "gold", "checkpoints")

# Data Factory
$ADF_NAME = "adf-finance-fraud-dev"

# Databricks
$DATABRICKS_NAME = "dbw-finance-fraud-dev"
$DATABRICKS_SKU = "premium" #standard is not supported after 2024-06-01

# Synapse
$SYNAPSE_WORKSPACE = "syn-finance-fraud-dev"
$SYNAPSE_FS        = "synapsefs"
$SQL_ADMIN_USER    = "sqladminuser"
# SQL_ADMIN_PASSWORD is intentionally not set here - scripts that need it
# will prompt for it interactively via Read-Host.

# Key Vault
$KEYVAULT_NAME = "kv-finance-fraud-dev-dg"
