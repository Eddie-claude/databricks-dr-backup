# ─────────────────────────────────────────────────────────────────────────────
# outputs.tf — Valeurs exportées après le terraform apply
# ─────────────────────────────────────────────────────────────────────────────

output "storage_account_name" {
  description = "Nom du storage account DR"
  value       = azurerm_storage_account.dr.name
}

output "storage_account_id" {
  description = "Resource ID du storage account DR"
  value       = azurerm_storage_account.dr.id
}

output "adls_backup_url" {
  description = "URL ABFS à utiliser dans les notebooks Databricks (backup_root)"
  value       = "abfss://${var.container_name}@${azurerm_storage_account.dr.name}.dfs.core.windows.net"
}

output "private_endpoint_dfs_ip" {
  description = "IP privée du Private Endpoint DFS (à vérifier via nslookup depuis le cluster)"
  value       = azurerm_private_endpoint.adls_dfs.private_service_connection[0].private_ip_address
}

output "private_endpoint_blob_ip" {
  description = "IP privée du Private Endpoint Blob"
  value       = azurerm_private_endpoint.adls_blob.private_service_connection[0].private_ip_address
}

output "private_dns_zone_dfs" {
  description = "Nom de la Private DNS Zone créée pour DFS"
  value       = azurerm_private_dns_zone.adls_dfs.name
}

output "storage_credential_name" {
  description = "Nom du Storage Credential Unity Catalog"
  value       = databricks_storage_credential.dr_backup.name
}

output "external_location_name" {
  description = "Nom de l'External Location Unity Catalog"
  value       = databricks_external_location.dr_backup.name
}

output "external_location_url" {
  description = "URL de l'External Location UC"
  value       = databricks_external_location.dr_backup.url
}

output "next_steps" {
  description = "Instructions post-deploy"
  value       = <<-EOT
    ✅ Infrastructure DR déployée.

    → Copier dans le widget 'backup_root' du notebook 00_dr_orchestrator :
       abfss://${var.container_name}@${azurerm_storage_account.dr.name}.dfs.core.windows.net

    → Créer le Secret Scope Databricks :
       databricks secrets create-scope dr-backup
       databricks secrets put-secret dr-backup databricks-token --string-value <PAT>

    → Valider la connectivité depuis un cluster Databricks :
       %sh nslookup ${azurerm_storage_account.dr.name}.dfs.core.windows.net
       # Doit retourner : ${azurerm_private_endpoint.adls_dfs.private_service_connection[0].private_ip_address}

    → Tester l'External Location depuis UC :
       VALIDATE STORAGE CREDENTIAL `sc-dr-backup`;
       VALIDATE EXTERNAL LOCATION `el-dr-backup`;
  EOT
}
