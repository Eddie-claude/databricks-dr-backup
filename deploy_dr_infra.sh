#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy_dr_infra.sh
# Script de déploiement et validation de l'infrastructure DR Databricks
#
# Usage:
#   chmod +x deploy_dr_infra.sh
#   ./deploy_dr_infra.sh [plan|apply|validate|destroy]
#
# Prérequis:
#   - az CLI connecté (az login ou Service Principal)
#   - terraform >= 1.5.0 installé
#   - databricks CLI installé (pour les étapes de validation)
#   - terraform.tfvars renseigné à partir de terraform.tfvars.example
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Couleurs ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

info()    { echo -e "${CYAN}[INFO]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC}   $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }
section() { echo -e "\n${BLUE}══════════════════════════════════════════════${NC}"; \
            echo -e "${BLUE}  $*${NC}"; \
            echo -e "${BLUE}══════════════════════════════════════════════${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${SCRIPT_DIR}/../terraform"
ACTION="${1:-plan}"

# ─────────────────────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────────────────────────────────────

check_prerequisites() {
  section "Vérification des prérequis"

  local missing=0

  # az CLI
  if command -v az &>/dev/null; then
    AZ_VERSION=$(az version --query '"azure-cli"' -o tsv 2>/dev/null || echo "?")
    success "az CLI ${AZ_VERSION}"
  else
    error "az CLI non trouvé — https://docs.microsoft.com/cli/azure/install-azure-cli"
    missing=1
  fi

  # terraform
  if command -v terraform &>/dev/null; then
    TF_VERSION=$(terraform version -json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['terraform_version'])" 2>/dev/null || terraform version | head -1)
    success "Terraform ${TF_VERSION}"
  else
    error "Terraform non trouvé — https://developer.hashicorp.com/terraform/install"
    missing=1
  fi

  # databricks CLI (optionnel pour la validation)
  if command -v databricks &>/dev/null; then
    DB_VERSION=$(databricks version 2>/dev/null || echo "?")
    success "Databricks CLI ${DB_VERSION}"
  else
    warn "Databricks CLI non trouvé — validation finale nécessitera une installation manuelle"
  fi

  # jq (pour parser les outputs)
  if command -v jq &>/dev/null; then
    success "jq $(jq --version)"
  else
    warn "jq non trouvé — parsing des outputs JSON désactivé"
  fi

  # Vérifier la connexion Azure
  if az account show &>/dev/null; then
    SUBSCRIPTION=$(az account show --query name -o tsv)
    TENANT=$(az account show --query tenantId -o tsv)
    success "Connecté Azure: ${SUBSCRIPTION} (${TENANT})"
  else
    error "Non connecté à Azure — exécuter: az login"
    missing=1
  fi

  # Vérifier que terraform.tfvars existe
  if [ -f "${TF_DIR}/terraform.tfvars" ]; then
    success "terraform.tfvars trouvé"
  else
    error "terraform.tfvars absent — copier terraform.tfvars.example et renseigner les valeurs"
    missing=1
  fi

  [ $missing -eq 0 ] || { error "Prérequis manquants — arrêt."; exit 1; }
}

tf_init() {
  section "Terraform Init"
  cd "${TF_DIR}"
  terraform init -upgrade
  success "Init terminé"
}

tf_validate() {
  section "Terraform Validate"
  cd "${TF_DIR}"
  terraform validate
  success "Configuration valide"
}

tf_plan() {
  section "Terraform Plan"
  cd "${TF_DIR}"
  terraform plan -out=tfplan -detailed-exitcode
  local rc=$?
  if [ $rc -eq 0 ]; then
    info "Aucun changement à appliquer"
  elif [ $rc -eq 2 ]; then
    success "Plan généré avec des changements — fichier: tfplan"
  else
    error "Plan échoué (code $rc)"
    exit 1
  fi
}

tf_apply() {
  section "Terraform Apply"
  cd "${TF_DIR}"

  if [ -f tfplan ]; then
    info "Application du plan existant (tfplan)..."
    terraform apply tfplan
  else
    warn "Pas de tfplan — génération et application directe..."
    terraform apply -auto-approve
  fi

  success "Apply terminé"

  # Extraction des outputs
  info "Extraction des outputs Terraform..."
  ADLS_URL=$(terraform output -raw adls_backup_url 2>/dev/null || echo "N/A")
  PE_DFS_IP=$(terraform output -raw private_endpoint_dfs_ip 2>/dev/null || echo "N/A")
  PE_BLOB_IP=$(terraform output -raw private_endpoint_blob_ip 2>/dev/null || echo "N/A")
  STORAGE_NAME=$(terraform output -raw storage_account_name 2>/dev/null || echo "N/A")
  EL_NAME=$(terraform output -raw external_location_name 2>/dev/null || echo "N/A")
  SC_NAME=$(terraform output -raw storage_credential_name 2>/dev/null || echo "N/A")

  echo ""
  echo -e "${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
  echo -e "${GREEN}║  INFRASTRUCTURE DR DÉPLOYÉE                         ║${NC}"
  echo -e "${GREEN}╠══════════════════════════════════════════════════════╣${NC}"
  echo -e "${GREEN}║${NC}  Storage Account   : ${STORAGE_NAME}"
  echo -e "${GREEN}║${NC}  ADLS URL          : ${ADLS_URL}"
  echo -e "${GREEN}║${NC}  PE DFS IP privée  : ${PE_DFS_IP}"
  echo -e "${GREEN}║${NC}  PE Blob IP privée : ${PE_BLOB_IP}"
  echo -e "${GREEN}║${NC}  Storage Credential: ${SC_NAME}"
  echo -e "${GREEN}║${NC}  External Location : ${EL_NAME}"
  echo -e "${GREEN}╚══════════════════════════════════════════════════════╝${NC}"

  # Sauvegarder les outputs pour la validation
  cat > /tmp/dr_infra_outputs.env <<EOF
ADLS_URL="${ADLS_URL}"
PE_DFS_IP="${PE_DFS_IP}"
PE_BLOB_IP="${PE_BLOB_IP}"
STORAGE_NAME="${STORAGE_NAME}"
EL_NAME="${EL_NAME}"
SC_NAME="${SC_NAME}"
EOF
  info "Outputs sauvegardés dans /tmp/dr_infra_outputs.env"
}

validate_infrastructure() {
  section "Validation de l'Infrastructure DR"

  # Charger les outputs si disponibles
  [ -f /tmp/dr_infra_outputs.env ] && source /tmp/dr_infra_outputs.env || {
    cd "${TF_DIR}"
    ADLS_URL=$(terraform output -raw adls_backup_url 2>/dev/null || echo "N/A")
    STORAGE_NAME=$(terraform output -raw storage_account_name 2>/dev/null || echo "N/A")
    PE_DFS_IP=$(terraform output -raw private_endpoint_dfs_ip 2>/dev/null || echo "N/A")
    SC_NAME=$(terraform output -raw storage_credential_name 2>/dev/null || echo "N/A")
    EL_NAME=$(terraform output -raw external_location_name 2>/dev/null || echo "N/A")
  }

  local errors=0

  # ── 1. Storage Account existe ─────────────────────────────────────────────
  info "1. Vérification Storage Account..."
  if az storage account show --name "${STORAGE_NAME}" --query id -o tsv &>/dev/null; then
    success "Storage Account '${STORAGE_NAME}' trouvé"
  else
    error "Storage Account '${STORAGE_NAME}' introuvable"
    errors=$((errors+1))
  fi

  # ── 2. Accès public bien désactivé ───────────────────────────────────────
  info "2. Vérification accès public..."
  PUBLIC_ACCESS=$(az storage account show --name "${STORAGE_NAME}" \
    --query publicNetworkAccess -o tsv 2>/dev/null || echo "Unknown")
  if [ "${PUBLIC_ACCESS}" = "Disabled" ]; then
    success "Accès public: Disabled ✓"
  else
    warn "Accès public: ${PUBLIC_ACCESS} (attendu: Disabled)"
  fi

  # ── 3. Private Endpoints ──────────────────────────────────────────────────
  info "3. Vérification Private Endpoints..."
  PE_COUNT=$(az network private-endpoint list \
    --query "[?contains(name, '${STORAGE_NAME}')].name" -o tsv 2>/dev/null | wc -l | tr -d ' ')
  if [ "${PE_COUNT}" -ge 2 ]; then
    success "Private Endpoints: ${PE_COUNT} trouvés (dfs + blob)"
  else
    error "Private Endpoints insuffisants: ${PE_COUNT} trouvés (attendu: 2)"
    errors=$((errors+1))
  fi

  # ── 4. DNS Zones ──────────────────────────────────────────────────────────
  info "4. Vérification Private DNS Zones..."
  for dns_zone in "privatelink.dfs.core.windows.net" "privatelink.blob.core.windows.net"; do
    if az network private-dns zone show --name "${dns_zone}" &>/dev/null 2>&1; then
      success "DNS Zone: ${dns_zone} ✓"
    else
      # Chercher dans tous les RGs
      FOUND=$(az network private-dns zone list --query "[?name=='${dns_zone}'].name" -o tsv 2>/dev/null || echo "")
      if [ -n "${FOUND}" ]; then
        success "DNS Zone: ${dns_zone} ✓ (trouvée)"
      else
        error "DNS Zone manquante: ${dns_zone}"
        errors=$((errors+1))
      fi
    fi
  done

  # ── 5. RBAC Access Connector ──────────────────────────────────────────────
  info "5. Vérification RBAC Access Connector..."
  STORAGE_ID=$(az storage account show --name "${STORAGE_NAME}" --query id -o tsv 2>/dev/null || echo "")
  if [ -n "${STORAGE_ID}" ]; then
    RBAC_OK=$(az role assignment list --scope "${STORAGE_ID}" \
      --query "[?roleDefinitionName=='Storage Blob Data Contributor'].principalType" \
      -o tsv 2>/dev/null | head -1)
    if [ -n "${RBAC_OK}" ]; then
      success "RBAC Storage Blob Data Contributor assigné ✓"
    else
      error "RBAC manquant sur le storage account"
      errors=$((errors+1))
    fi
  fi

  # ── 6. Container ADLS ─────────────────────────────────────────────────────
  info "6. Vérification container ADLS..."
  # Via az (peut échouer si accès public désactivé sans être dans le bon réseau)
  warn "Vérification du container ADLS à effectuer depuis un cluster Databricks (réseau privé requis)"

  # ── 7. Validation Databricks CLI (si disponible) ──────────────────────────
  if command -v databricks &>/dev/null; then
    info "7. Validation Unity Catalog via Databricks CLI..."

    # Storage Credential
    if databricks storage-credentials get "${SC_NAME}" &>/dev/null 2>&1; then
      success "Storage Credential UC: '${SC_NAME}' ✓"
    else
      warn "Storage Credential UC non validé via CLI (vérifier manuellement)"
    fi

    # External Location
    if databricks external-locations get "${EL_NAME}" &>/dev/null 2>&1; then
      success "External Location UC: '${EL_NAME}' ✓"
    else
      warn "External Location UC non validé via CLI (vérifier manuellement)"
    fi
  else
    warn "7. Databricks CLI absent — valider manuellement (voir section 'Validation depuis Databricks' ci-dessous)"
  fi

  # ── Résumé ────────────────────────────────────────────────────────────────
  echo ""
  if [ $errors -eq 0 ]; then
    success "Validation complète — 0 erreur"
  else
    error "Validation terminée avec ${errors} erreur(s)"
    exit 1
  fi

  print_databricks_validation_steps
}

print_databricks_validation_steps() {
  section "Validation depuis un cluster Databricks"

  [ -f /tmp/dr_infra_outputs.env ] && source /tmp/dr_infra_outputs.env

  cat <<EOF

  Exécuter les commandes suivantes depuis un notebook Databricks
  (sur un cluster dans le VNet injecté) :

  ── 1. Test DNS (résolution vers IP privée) ───────────────────────────────────
  %sh
  nslookup ${STORAGE_NAME:-<storage_name>}.dfs.core.windows.net
  # Attendu: ${PE_DFS_IP:-<ip_privee_pe_dfs>}
  # Si retourne une IP publique : DNS Zone mal liée au VNet

  ── 2. Connectivité TCP ───────────────────────────────────────────────────────
  %sh
  nc -zv ${PE_DFS_IP:-<ip_privee_pe_dfs>} 443 && echo "OPEN" || echo "CLOSED"
  # Attendu: OPEN

  ── 3. Écriture test via abfss:// ─────────────────────────────────────────────
  %python
  test_path = "${ADLS_URL:-abfss://dr-backup@<storage>.dfs.core.windows.net}/_test/connectivity"
  spark.range(1).write.format("delta").mode("overwrite").save(test_path)
  print("✅ Écriture OK")
  spark.read.format("delta").load(test_path).show()
  dbutils.fs.rm(test_path, recurse=True)

  ── 4. Validation Storage Credential UC ──────────────────────────────────────
  %sql
  VALIDATE STORAGE CREDENTIAL \`${SC_NAME:-sc-dr-backup}\`;

  ── 5. Validation External Location UC ───────────────────────────────────────
  %sql
  VALIDATE EXTERNAL LOCATION \`${EL_NAME:-el-dr-backup}\`;

  ── 6. Créer le Secret Scope pour le PAT ─────────────────────────────────────
  databricks secrets create-scope dr-backup
  databricks secrets put-secret dr-backup databricks-token --string-value <PAT>

EOF
}

tf_destroy() {
  section "Terraform Destroy ⚠️"
  warn "Cette action va SUPPRIMER toute l'infrastructure DR (storage + PE + DNS + UC)"
  read -rp "  Confirmer (yes/no): " confirm
  if [ "${confirm}" != "yes" ]; then
    info "Destroy annulé"
    exit 0
  fi
  cd "${TF_DIR}"
  terraform destroy -auto-approve
  success "Infrastructure DR supprimée"
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Databricks DR — Déploiement Infrastructure              ║${NC}"
echo -e "${BLUE}║  Action : ${ACTION}${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

check_prerequisites

case "${ACTION}" in
  plan)
    tf_init
    tf_validate
    tf_plan
    ;;
  apply)
    tf_init
    tf_validate
    tf_plan
    tf_apply
    validate_infrastructure
    ;;
  validate)
    validate_infrastructure
    ;;
  steps)
    print_databricks_validation_steps
    ;;
  destroy)
    tf_destroy
    ;;
  *)
    error "Action inconnue: ${ACTION}"
    echo "Usage: $0 [plan|apply|validate|steps|destroy]"
    exit 1
    ;;
esac

echo ""
success "Script terminé — action: ${ACTION}"
