// ---------------------------------------------
// Provider Configuration: Connect to Azure
// ---------------------------------------------
provider "azurerm" {
  features {}
}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 0.5.0" # or whichever version you intend to use
    }
  }
  required_version = ">= 0.14"
}



//////////////////////////////////////////////////////////
// Resource Group: Logical container for all resources
//////////////////////////////////////////////////////////
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name // Using variable for resource group name
  location = var.location            // Using variable for location
}

# Create the storage account that imitates source data store
resource "azurerm_storage_account" "source_taxi_data_storage" {
  name                     = "sourcetaxidata340g"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create the container for dimension tables CSV data
resource "azurerm_storage_container" "taxi_dim_tables" {
  name                  = "taxi-dim-tables"
  storage_account_name  = azurerm_storage_account.source_taxi_data_storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "taxi_rides_hist" {
  name                  = "taxi-rides-historical"
  storage_account_name  = azurerm_storage_account.source_taxi_data_storage.name
  container_access_type = "private"
}

# Get list of files to act as source data store
locals {
  taxi_dim_tables_files = fileset("../data/dim_tables", "*")
  taxi_rides_hist_files = fileset("../data/generated_historical_taxi_data", "*")
}

# Upload each file from the dim_tables folder as a blob in the taxi-dim-tables container
resource "azurerm_storage_blob" "taxi_dim_tables_blobs" {
  for_each = { for file in local.taxi_dim_tables_files : file => file }

  name                   = each.value
  storage_account_name   = azurerm_storage_account.source_taxi_data_storage.name
  storage_container_name = azurerm_storage_container.taxi_dim_tables.name
  type                   = "Block"
  source                 = "../data/dim_tables/${each.value}"
}

# Upload each file from the generated taxi data folder as a blob in the taxi-rides-historical container
resource "azurerm_storage_blob" "taxi_rides_hist_blobs" {
  for_each = { for file in local.taxi_rides_hist_files : file => file }

  name                   = each.value
  storage_account_name   = azurerm_storage_account.source_taxi_data_storage.name
  storage_container_name = azurerm_storage_container.taxi_rides_hist.name
  type                   = "Block"
  source                 = "../data/generated_historical_taxi_data/${each.value}"
}

//////////////////////////////////////////////////////////
// Storage Account: Configured as Data Lake Storage Gen2 (raw zone)
//////////////////////////////////////////////////////////
resource "azurerm_storage_account" "storage" {
  name                     = var.storage_account_name // Unique storage account name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  // Enable hierarchical namespace for Data Lake Storage Gen2 functionality
  is_hns_enabled = true
}


//////////////////////////////////////////////////////////
// Storage Containers
//////////////////////////////////////////////////////////
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze_zone" {
  name               = "bronze" // Container to hold raw data files
  storage_account_id = azurerm_storage_account.storage.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver_zone" {
  name               = "silver" // Container to hold cleaned data files
  storage_account_id = azurerm_storage_account.storage.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold_zone" {
  name               = "gold" // Container to hold aggregated data files
  storage_account_id = azurerm_storage_account.storage.id
}

