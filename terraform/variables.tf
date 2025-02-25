// Variable for Resource Group Name with a default value
variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "TaxiAnalyticsRG" // Default name for the resource group
}

// Variable for Azure location with a default value
variable "location" {
  description = "Azure region for deployment"
  type        = string
  default     = "East US"
}

// Variable for Storage Account Name with a default value (must be globally unique and lowercase)
variable "storage_account_name" {
  description = "Unique name for the Storage Account"
  type        = string
  default     = "taxianalyticsstorage" // Adjust this name as needed to ensure uniqueness
}

// Variable for Data Factory Name with a default value
variable "data_factory_name" {
  description = "Name of the Azure Data Factory instance"
  type        = string
  default     = "TaxiAnalyticsADF"
}


variable "taxi_data_blob_storage_connection_str" {
  type      = string
  sensitive = true
}

variable "client_id" {
  type      = string
  sensitive = true
}


variable "tenant_id" {
  type      = string
  sensitive = true
}

variable "client_secret" {
  type      = string
  sensitive = true
}


