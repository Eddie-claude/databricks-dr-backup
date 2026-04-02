# 🔒 DR Infra — Private Endpoint ADLS Gen2 pour Databricks DR

## Ce que provisionne ce module Terraform

```
┌──────────────────────────────────────────────────────────────────┐
│  VNet Databricks (VNET injection — existant)                     │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │  snet-public    │  │  snet-private    │  │ snet-pe (NEW)  │  │
│  │  (Databricks)   │  │  (Databricks)    │  │  10.0.3.0/27   │  │
│  └─────────────────┘  └──────────────────┘  └───────┬────────┘  │
│                                                       │           │
│              Private DNS Zones (liées au VNet)        │           │
│              privatelink.dfs.core.windows.net  ◄──────┤           │
│              privatelink.blob.core.windows.net ◄──────┤           │
└──────────────────────────────────────────────────────┼───────────┘
                                                        │ IP privée
                                           ─────────────┼───────────
                                                        │
┌──────────────────────────────────────────────────────▼───────────┐
│  Resource Group DR (rg-databricks-dr)                            │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  Storage Account ADLS Gen2 (stdbdrbackup)                   │ │
│  │  ├─ HNS activé                                              │ │
│  │  ├─ Public Network Access: DISABLED                         │ │
│  │  ├─ Shared Key: DISABLED (Managed Identity uniquement)      │ │
│  │  ├─ Replication: ZRS (Zone-redundant)                       │ │
│  │  └─ Container: dr-backup/                                   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  PE dfs  → NIC → IP 10.0.3.4  (abfss://)                        │
│  PE blob → NIC → IP 10.0.3.5  (blob REST)                       │
└──────────────────────────────────────────────────────────────────┘

Unity Catalog (dans Databricks workspace prod)
  Storage Credential : sc-dr-backup  ← Access Connector Managed Identity
  External Location  : el-dr-backup  ← abfss://dr-backup@stdbdrbackup.dfs...
```

---

## Pourquoi 2 Private Endpoints (dfs + blob) ?

| Endpoint | FQDN | Usage |
|---|---|---|
| **dfs** | `privatelink.dfs.core.windows.net` | ABFS (`abfss://`) — Databricks notebooks, DEEP CLONE, spark.read/write |
| **blob** | `privatelink.blob.core.windows.net` | Blob REST API — Terraform provider azurerm, AzCopy, certaines opérations UC |

Un seul subresource par Private Endpoint (limitation Azure) — donc 2 PEs obligatoires.

---

## Prérequis

1. **Workspace Databricks avec VNET injection** — obligatoire (sans ça, impossible de lier la DNS Zone au managed VNet Databricks)
2. **Access Connector existant** en production (crée avec le workspace)
3. **az CLI** connecté avec droits : Contributor sur le RG DR + Network Contributor sur le VNet
4. **Terraform** ≥ 1.5.0
5. **PAT Databricks** avec droits `metastore admin` ou `CREATE STORAGE CREDENTIAL`

---

## Déploiement

```bash
# 1. Cloner / copier les fichiers
cd terraform/

# 2. Configurer
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars   # Renseigner VOS valeurs

# 3. Plan (simulation)
../scripts/deploy_dr_infra.sh plan

# 4. Apply (déploiement réel)
../scripts/deploy_dr_infra.sh apply

# 5. Validation depuis Databricks
../scripts/deploy_dr_infra.sh steps
```

---

## Ordre de création des ressources (dépendances)

```
Resource Group DR
    └─► Storage Account (public_network_access=Disabled)
            └─► Private Endpoint DFS  ──► DNS Zone DFS ──► VNet Link
            └─► Private Endpoint Blob ──► DNS Zone Blob ──► VNet Link
            └─► RBAC (Access Connector → Storage Blob Data Contributor)
                    └─► Storage Credential UC
                            └─► External Location UC
            └─► ADLS Container "dr-backup"  (après PE, car public access=Disabled)
```

⚠️ Le container ADLS doit être créé **après** le Private Endpoint, car l'accès public est désactivé. Terraform gère ça via `depends_on`.

---

## Points d'attention

### DNS — Le point le plus délicat
La Private DNS Zone doit être **liée au VNet Databricks**. Si les clusters résolvent encore l'IP publique (`nslookup <storage>.dfs.core.windows.net` → IP publique), la connexion sera refusée par le firewall du storage.

```bash
# Diagnostic depuis un notebook Databricks
%sh
nslookup stdbdrbackup.dfs.core.windows.net
# Attendu: 10.0.3.4 (ou l'IP privée de votre PE)
# Si IP publique → vérifier que la DNS Zone est liée au bon VNet
```

### Hub-Spoke : si votre réseau a un Hub
Si vous avez une architecture Hub-Spoke avec DNS centralisé (Azure Firewall / DNS forwarder dans le Hub) :
- Lier la Private DNS Zone au **Hub VNet** (pas au Spoke Databricks)
- Ou configurer le DNS forwarder pour résoudre `*.dfs.core.windows.net` via le DNS privé

### `shared_access_key_enabled = false`
Le storage account n'accepte que l'authentification par Managed Identity ou OAuth. Les clés de stockage sont désactivées. C'est intentionnel pour la sécurité — le backup DR passe exclusivement via l'Access Connector.

### `AzureServices` bypass
Le firewall network_rules inclut `bypass = ["AzureServices"]`. Cela permet à l'Access Connector (Managed Identity Azure) d'accéder au storage même sans passer par le PE. C'est nécessaire pour que Terraform puisse créer le container ADLS en apply.

---

## Destruction

```bash
../scripts/deploy_dr_infra.sh destroy
```

⚠️ Cela supprime le storage DR et tous les backups. À n'utiliser qu'en environnement de test.

---

## Coûts estimés

| Ressource | Coût estimé |
|---|---|
| Storage Account ZRS (Standard) | ~0.02 €/GB/mois |
| Private Endpoint DFS | ~0.01 €/heure (~7 €/mois) |
| Private Endpoint Blob | ~0.01 €/heure (~7 €/mois) |
| Transfert de données PE | ~0.01 €/GB après 10 TB |

Pour 1 TB de backup journalier : **~35-50 €/mois** (PE + storage).
