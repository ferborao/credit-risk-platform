terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
  required_version = ">= 1.0"
}

provider "azurerm" {
  features {}
}

# Variables
variable "resource_group_name" {
  description = "Nombre del resource group"
  default     = "credit-risk-rg"
}

variable "location" {
  description = "Región de Azure"
  default     = "westeurope"
}

variable "environment" {
  description = "Entorno (dev, staging, prod)"
  default     = "dev"
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    environment = var.environment
    project     = "credit-risk-platform"
    managed_by  = "terraform"
  }
}

# Storage Account para los datos (Bronze, Silver, Gold)
resource "azurerm_storage_account" "datalake" {
  name                     = "creditriskdl${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace = ADLS Gen2

  tags = {
    environment = var.environment
    project     = "credit-risk-platform"
  }
}

# Contenedores del datalake
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Outputs útiles
output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_account_primary_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}