# ─────────────────────────────────────────────────────────────────────────────
# main.tf — DR Backup : ADLS Gen2 + Private Endpoints + Access Connector RBAC
#            + Unity Catalog External Location
#
# Ressources créées :
#   1. Resource Group DR
#   2. Storage Account ADLS Gen2 (HNS, accès public désactivé)
#   3. Container "dr-backup"
#   4. Subnet dédié PE dans le VNet Databricks (optionnel)
#   5. Private Endpoint "dfs"  → privatelink.dfs.core.windows.net
#   6. Private Endpoint "blob" → privatelink.blob.core.windows.net
#   7. Private DNS Zones + liens VNet
#   8. RBAC : Access Connector → Storage Blob Data Contributor
#   9. Databricks Storage Credential (via Access Connector)
#  10. Databricks External Location (UC)
# ─────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA SOURCES — Ressources existantes
# ─────────────────────────────────────────────────────────────────────────────

data "azurerm_client_config" "current" {}

# VNet Databricks existant (VNET injection obligatoire pour les Private Endpoints)
data "azurerm_virtual_network" "databricks_vnet" {
  name                = var.databricks_vnet_name
  resource_group_name = var.databricks_vnet_rg
}

# Access Connector existant en production
data "azurerm_databricks_access_connector" "prod" {
  name                = var.access_connector_name
  resource_group_name = var.access_connector_rg
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. RESOURCE GROUP DR
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "dr" {
  name     = var.rg_dr_name
  location = var.location
  tags     = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. STORAGE ACCOUNT ADLS Gen2 — DR Backup
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_storage_account" "dr" {
  name                = var.storage_account_name
  resource_group_name = azurerm_resource_group.dr.name
  location            = azurerm_resource_group.dr.location
  tags                = var.tags

  account_kind             = "StorageV2"
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type

  # ── ADLS Gen2 (Hierarchical Namespace) ───────────────────────────────────
  is_hns_enabled = true

  # ── Accès public totalement désactivé ────────────────────────────────────
  public_network_access_enabled = false

  # ── Réseau : tout refuser sauf Private Endpoints ─────────────────────────
  network_rules {
    default_action             = "Deny"
    bypass                     = ["AzureServices"]
    # Le "AzureServices" bypass permet à l'Access Connector (Managed Identity)
    # de contourner le firewall même sans être dans le VNet
    ip_rules                   = []
    virtual_network_subnet_ids = []
  }

  # ── Soft delete (protection suppression accidentelle) ─────────────────────
  blob_properties {
    delete_retention_policy {
      days = var.soft_delete_retention_days
    }
    container_delete_retention_policy {
      days = var.soft_delete_retention_days
    }
  }

  # ── TLS minimum ──────────────────────────────────────────────────────────
  min_tls_version = "TLS1_2"

  # ── Shared Key désactivé → uniquement Managed Identity ou OAuth ──────────
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. CONTAINER ADLS Gen2
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_storage_data_lake_gen2_filesystem" "dr_backup" {
  name               = var.container_name
  storage_account_id = azurerm_storage_account.dr.id

  depends_on = [
    azurerm_private_endpoint.adls_dfs,  # Le PE doit exister avant de créer le filesystem
  ]
}

# ─────────────────────────────────────────────────────────────────────────────
# 4. SUBNET DÉDIÉ AUX PRIVATE ENDPOINTS (dans le VNet Databricks)
# ─────────────────────────────────────────────────────────────────────────────
# ⚠️  NE PAS utiliser les subnets public/private déjà gérés par Databricks.
#     Créer un 3ème subnet dédié aux PE dans le même VNet.

resource "azurerm_subnet" "private_endpoints" {
  count = var.create_pe_subnet ? 1 : 0

  name                 = var.pe_subnet_name
  resource_group_name  = var.databricks_vnet_rg
  virtual_network_name = var.databricks_vnet_name
  address_prefixes     = [var.pe_subnet_address_prefix]

  # Obligatoire pour les Private Endpoints (désactive les policies réseau)
  private_endpoint_network_policies = "Disabled"
}

# Référence au subnet (créé ou existant)
locals {
  pe_subnet_id = var.create_pe_subnet ? azurerm_subnet.private_endpoints[0].id : (
    data.azurerm_subnet.pe_existing[0].id
  )
}

data "azurerm_subnet" "pe_existing" {
  count                = var.create_pe_subnet ? 0 : 1
  name                 = var.pe_subnet_name
  virtual_network_name = var.databricks_vnet_name
  resource_group_name  = var.databricks_vnet_rg
}

# ─────────────────────────────────────────────────────────────────────────────
# 5. PRIVATE ENDPOINT — Subresource "dfs" (ABFS / ADLS Gen2)
#    Databricks utilise abfss:// → endpoint dfs est OBLIGATOIRE
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_private_endpoint" "adls_dfs" {
  name                = "pe-${var.storage_account_name}-dfs"
  location            = data.azurerm_virtual_network.databricks_vnet.location
  resource_group_name = var.databricks_vnet_rg   # PE dans le même RG que le VNet
  subnet_id           = local.pe_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.storage_account_name}-dfs"
    private_connection_resource_id = azurerm_storage_account.dr.id
    is_manual_connection           = false
    subresource_names              = ["dfs"]   # ABFS protocol
  }

  private_dns_zone_group {
    name                 = "dzg-dfs"
    private_dns_zone_ids = [azurerm_private_dns_zone.adls_dfs.id]
  }

  depends_on = [azurerm_storage_account.dr]
}

# ─────────────────────────────────────────────────────────────────────────────
# 6. PRIVATE ENDPOINT — Subresource "blob" (Blob REST API)
#    Requis pour : Terraform azurerm provider, AzCopy, certaines opérations UC
#    Note: 1 PE par subresource (dfs ET blob = 2 PEs séparés)
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_private_endpoint" "adls_blob" {
  name                = "pe-${var.storage_account_name}-blob"
  location            = data.azurerm_virtual_network.databricks_vnet.location
  resource_group_name = var.databricks_vnet_rg
  subnet_id           = local.pe_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.storage_account_name}-blob"
    private_connection_resource_id = azurerm_storage_account.dr.id
    is_manual_connection           = false
    subresource_names              = ["blob"]
  }

  private_dns_zone_group {
    name                 = "dzg-blob"
    private_dns_zone_ids = [azurerm_private_dns_zone.adls_blob.id]
  }

  depends_on = [azurerm_storage_account.dr]
}

# ─────────────────────────────────────────────────────────────────────────────
# 7. PRIVATE DNS ZONES + LIENS VNET
#    Nécessaire pour que les clusters Databricks résolvent le FQDN du storage
#    vers l'IP privée du PE (sinon → IP publique → refus firewall)
# ─────────────────────────────────────────────────────────────────────────────

# ── DNS Zone pour dfs (ABFS) ──────────────────────────────────────────────────
resource "azurerm_private_dns_zone" "adls_dfs" {
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = var.databricks_vnet_rg
  tags                = var.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "adls_dfs" {
  name                  = "link-dfs-${var.databricks_vnet_name}"
  resource_group_name   = var.databricks_vnet_rg
  private_dns_zone_name = azurerm_private_dns_zone.adls_dfs.name
  virtual_network_id    = data.azurerm_virtual_network.databricks_vnet.id
  registration_enabled  = false
  tags                  = var.tags
}

# ── DNS Zone pour blob ────────────────────────────────────────────────────────
resource "azurerm_private_dns_zone" "adls_blob" {
  name                = "privatelink.blob.core.windows.net"
  resource_group_name = var.databricks_vnet_rg
  tags                = var.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "adls_blob" {
  name                  = "link-blob-${var.databricks_vnet_name}"
  resource_group_name   = var.databricks_vnet_rg
  private_dns_zone_name = azurerm_private_dns_zone.adls_blob.name
  virtual_network_id    = data.azurerm_virtual_network.databricks_vnet.id
  registration_enabled  = false
  tags                  = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# 8. RBAC — Access Connector → Storage Blob Data Contributor sur DR storage
#    Permet au workspace Databricks d'écrire via Managed Identity (sans clé)
# ─────────────────────────────────────────────────────────────────────────────

resource "azurerm_role_assignment" "ac_storage_contributor" {
  scope                = azurerm_storage_account.dr.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_databricks_access_connector.prod.identity[0].principal_id

  description = "Permet à l'Access Connector Databricks d'accéder au storage DR pour les backups"
}

# ─────────────────────────────────────────────────────────────────────────────
# 9. DATABRICKS STORAGE CREDENTIAL (Unity Catalog)
#    Référence le Managed Identity de l'Access Connector
# ─────────────────────────────────────────────────────────────────────────────

resource "databricks_storage_credential" "dr_backup" {
  name    = "sc-dr-backup"
  comment = "Storage credential pour le backup DR — Access Connector ${var.access_connector_name}"

  azure_managed_identity {
    access_connector_id = data.azurerm_databricks_access_connector.prod.id
  }

  depends_on = [azurerm_role_assignment.ac_storage_contributor]
}

# ─────────────────────────────────────────────────────────────────────────────
# 10. DATABRICKS EXTERNAL LOCATION (Unity Catalog)
#     Point d'entrée UC vers le container DR sur ADLS Gen2
# ─────────────────────────────────────────────────────────────────────────────

resource "databricks_external_location" "dr_backup" {
  name            = "el-dr-backup"
  url             = "abfss://${var.container_name}@${azurerm_storage_account.dr.name}.dfs.core.windows.net"
  credential_name = databricks_storage_credential.dr_backup.name
  comment         = "External Location DR Backup — ${azurerm_storage_account.dr.name} (Private Endpoint)"

  depends_on = [
    azurerm_private_endpoint.adls_dfs,
    azurerm_private_dns_zone_virtual_network_link.adls_dfs,
    azurerm_storage_data_lake_gen2_filesystem.dr_backup,
  ]
}
