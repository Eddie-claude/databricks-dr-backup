# ─────────────────────────────────────────────────────────────────────────────
# variables.tf — DR Backup Storage : Private Endpoint pour ADLS Gen2
# ─────────────────────────────────────────────────────────────────────────────

# ── Général ──────────────────────────────────────────────────────────────────

variable "location" {
  description = "Région Azure du storage DR (idéalement différente de la prod)"
  type        = string
  default     = "westeurope"
}

variable "location_prod" {
  description = "Région Azure du workspace Databricks de production"
  type        = string
  default     = "switzerlandnorth"
}

variable "tags" {
  description = "Tags communs appliqués à toutes les ressources"
  type        = map(string)
  default = {
    project     = "databricks-dr"
    environment = "dr"
    managed_by  = "terraform"
  }
}

# ── Resource Groups ───────────────────────────────────────────────────────────

variable "rg_dr_name" {
  description = "Resource Group dédié au stockage DR"
  type        = string
  default     = "rg-databricks-dr"
}

variable "rg_databricks_name" {
  description = "Resource Group du workspace Databricks de production"
  type        = string
  # ex: "rg-databricks-prod"
}

# ── Réseau Databricks (existant) ──────────────────────────────────────────────

variable "databricks_vnet_name" {
  description = "Nom du VNet injecté du workspace Databricks (VNET injection requis)"
  type        = string
  # ex: "vnet-databricks-prod"
}

variable "databricks_vnet_rg" {
  description = "Resource Group du VNet Databricks"
  type        = string
}

variable "pe_subnet_name" {
  description = "Nom du subnet dédié aux Private Endpoints dans le VNet Databricks"
  type        = string
  default     = "snet-private-endpoints"
  # ⚠️ Doit être un subnet DISTINCT des subnets public/private Databricks
  # (Databricks gère lui-même ses subnets — ne pas les réutiliser)
}

variable "pe_subnet_address_prefix" {
  description = "CIDR du subnet Private Endpoints (si création requise)"
  type        = string
  default     = "10.0.3.0/27"   # /27 = 32 IPs, largement suffisant
}

variable "create_pe_subnet" {
  description = "Créer le subnet PE s'il n'existe pas encore"
  type        = bool
  default     = true
}

# ── Storage Account DR ────────────────────────────────────────────────────────

variable "storage_account_name" {
  description = "Nom du storage account DR (3-24 chars, minuscules, chiffres uniquement)"
  type        = string
  default     = "stdbdrbackup"
  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "Le nom du storage account doit faire 3-24 caractères, minuscules et chiffres uniquement."
  }
}

variable "storage_account_tier" {
  description = "Tier du storage account"
  type        = string
  default     = "Standard"
}

variable "storage_replication_type" {
  description = "Type de réplication (LRS = même région, ZRS = zone-redundant, GRS = geo-redundant)"
  type        = string
  default     = "ZRS"   # Zone-redundant recommandé pour DR
  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "RAGRS", "GZRS"], var.storage_replication_type)
    error_message = "Valeurs acceptées: LRS, ZRS, GRS, RAGRS, GZRS."
  }
}

variable "container_name" {
  description = "Nom du container ADLS Gen2 pour les backups DR"
  type        = string
  default     = "dr-backup"
}

variable "soft_delete_retention_days" {
  description = "Rétention soft delete (jours) — protection contre suppression accidentelle"
  type        = number
  default     = 14
}

# ── Access Connector (Managed Identity) ──────────────────────────────────────

variable "access_connector_name" {
  description = "Nom de l'Access Connector Databricks existant en production"
  type        = string
  # ex: "ac-databricks-prod"
}

variable "access_connector_rg" {
  description = "Resource Group de l'Access Connector"
  type        = string
}

# ── Unity Catalog (post-Terraform) ───────────────────────────────────────────

variable "databricks_host" {
  description = "URL du workspace Databricks (ex: https://adb-xxx.azuredatabricks.net)"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  description = "PAT Databricks pour le provider Terraform (création External Location)"
  type        = string
  sensitive   = true
}

# ── DNS existante (optionnel) ─────────────────────────────────────────────────

variable "existing_private_dns_zone_rg" {
  description = "RG de la Private DNS Zone existante (laisser vide si à créer)"
  type        = string
  default     = ""
}
