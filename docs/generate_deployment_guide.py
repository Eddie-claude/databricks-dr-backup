"""Génère DR_Backup_Guide_Deploiement_v4.1.docx.

Adapté de old/generate_guide.py (v3, 3 juin 2026) pour conserver la charte des guides
livrés au client. Reflète l'état du code après les correctifs de juillet et d'août :
weekly abandonné (Option C), retain_daily=30, VACUUM hebdomadaire et parallélisé,
cluster daily multi-worker, reprise d'un jour sur l'autre, archivage des logs,
notebooks de restauration 07 à 11, annexes dépannage.

La charte est dans docs/docx_style.py, partagée avec generate_release_notes.py.

Usage : python docs/generate_deployment_guide.py
"""

from docx.shared import Pt, RGBColor, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
import datetime
import os

from docx_style import (
    BLUE,
    new_document,
    add_heading,
    add_code,
    add_table,
    add_note,
    add_warning,
    bullet,
    numbered,
)

doc = new_document()


# ── Page de titre ─────────────────────────────────────────────────────────────

doc.add_paragraph()
title = doc.add_paragraph()
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = title.add_run("DR Backup Databricks")
run.font.size = Pt(28)
run.font.bold = True
run.font.color.rgb = RGBColor(*BLUE)

subtitle = doc.add_paragraph()
subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = subtitle.add_run("Guide de déploiement et de configuration")
run.font.size = Pt(16)
run.font.color.rgb = RGBColor(0x40, 0x40, 0x40)

version = doc.add_paragraph()
version.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = version.add_run("Version 4.1")
run.font.size = Pt(13)
run.font.bold = True
run.font.color.rgb = RGBColor(0x40, 0x40, 0x40)

doc.add_paragraph()
meta = doc.add_paragraph()
meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = meta.add_run(f"KeyIT — {datetime.date.today().strftime('%d/%m/%Y')}")
run.font.size = Pt(11)
run.font.color.rgb = RGBColor(0x80, 0x80, 0x80)

doc.add_paragraph()
doc.add_paragraph()
changelog = doc.add_paragraph()
changelog.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = changelog.add_run("Nouveautés de la version 4.1")
run.font.size = Pt(11)
run.font.bold = True
doc.add_paragraph()

add_table(
    doc,
    ["Changement", "Section"],
    [
        ["VACUUM parallélisé et ramené à une exécution hebdomadaire", "§6.3"],
        ["Reprise d'un backup interrompu d'un jour sur l'autre", "§5.3"],
        ["Archivage automatique des logs de cluster", "§4.6"],
        ["Cluster du backup quotidien dimensionné en multi-worker", "§5.1"],
        ["Snapshot hebdomadaire abandonné, rétention quotidienne portée à 30 jours", "§6"],
        ["Étape VACUUM explicite ajoutée à la politique de rétention", "§6.3"],
        ["Procédure de validation post-déploiement", "§7"],
        ["Notebooks de restauration 09, 10 et 11", "§9"],
        ["Annexe de dépannage des erreurs rencontrées en production", "Annexe A"],
        ["Authentification OAuth utilisateur pour un déploiement de test", "Annexe B"],
    ],
    col_widths=[13, 3],
)

doc.add_page_break()


# ── 1. Contexte ───────────────────────────────────────────────────────────────

add_heading(doc, "1. Contexte et objectifs", 1)

doc.add_paragraph(
    "Ce document décrit la procédure complète de déploiement de la solution DR Backup "
    "Databricks sur un workspace Azure Databricks gouverné par Unity Catalog. Il couvre "
    "l'environnement Azure à créer, le déploiement du code, le premier backup et sa "
    "validation."
)

doc.add_paragraph(
    "La solution sauvegarde les données et la configuration d'un workspace vers un compte "
    "de stockage ADLS Gen2, avec une rétention à deux niveaux et une restauration "
    "point-in-time. Elle est entièrement déployée par Databricks Asset Bundles (DAB) : "
    "aucune infrastructure externe, aucun agent, aucun accès sortant n'est requis."
)

add_heading(doc, "1.1 Ce que la solution sauvegarde", 2)

add_table(
    doc,
    ["Périmètre", "Contenu", "Notebook"],
    [
        ["Métadonnées Unity Catalog", "DDL des catalogs, schémas, tables, vues, volumes ; permissions (GRANT)", "01_uc_metadata"],
        ["Données des tables Delta", "Copie complète par DEEP CLONE, puis incrémentale", "02_data_clone"],
        ["Configuration du workspace", "Définitions de jobs, notebooks sources, ACL, repos Git", "05_workspace_config"],
        ["Rapport et différentiel", "Comparaison avec la veille, rapport HTML", "03_diff, 04_report"],
    ],
    col_widths=[4.5, 8.5, 3.5],
)

add_heading(doc, "1.2 Ce que la solution ne sauvegarde pas", 2)

bullet(doc, "Les tables non Delta (Parquet externe, CSV, formats propriétaires) — ignorées avec un message explicite")
bullet(doc, "Les vues matérialisées et les Streaming Tables (non clonables)")
bullet(doc, "Les secrets et les scopes de secrets Databricks")
bullet(doc, "Les catalogs système : hive_metastore, system, samples")
bullet(doc, "Les données hors Unity Catalog (DBFS racine, montages legacy)")

add_note(
    doc,
    "la solution sauvegarde vers un compte de stockage distinct du stockage de production. "
    "Elle protège contre la suppression accidentelle, la corruption logique et l'erreur "
    "humaine. Elle ne remplace pas une réplication géo-redondante contre la perte d'une "
    "région Azure : pour cela, activer GRS ou RA-GRS sur le compte de stockage de backup.",
)

add_heading(doc, "1.3 Architecture du stockage de backup", 2)

doc.add_paragraph("Après déploiement, l'arborescence du compte de stockage est la suivante :")

add_code(
    doc,
    "backup/\n"
    "  incremental/                      Tables Delta, mises a jour chaque jour\n"
    "    {catalog}/{schema}/{table}/      (seul le delta est ecrit)\n"
    "    _manifests/{AAAA-MM-JJ}.json     resultat de chaque run\n"
    "    _checkpoints/{AAAA-MM-JJ}.json   reprise apres interruption\n"
    "    _last_versions.json              versions Delta deja sauvegardees\n"
    "  snapshots/\n"
    "    monthly/{AAAA-MM}/               archive mensuelle independante\n"
    "  {AAAA-MM-JJ}/                     metadonnees UC, jobs, notebooks, rapport\n"
    "  latest.json                       pointeur vers le dernier backup reussi",
)

doc.add_paragraph(
    "Le dossier incremental/ est la source de la restauration point-in-time : l'historique "
    "Delta y conserve 30 jours de versions. Le dossier snapshots/monthly/ contient une copie "
    "complète et autonome, indépendante de incremental/."
)

doc.add_page_break()


# ── 2. Prérequis ──────────────────────────────────────────────────────────────

add_heading(doc, "2. Prérequis", 1)

add_heading(doc, "2.1 Informations à préparer", 2)

add_table(
    doc,
    ["Information", "Exemple", "Où la trouver"],
    [
        ["URL du workspace Databricks", "https://adb-1234567890123456.7.azuredatabricks.net", "Portail Azure, ressource Databricks, champ URL"],
        ["Nom du compte de stockage de backup", "stclientdrbackup", "À créer, voir §2.2"],
        ["Nom du container", "uc-backup", "À créer, voir §2.2"],
        ["Nom de l'External Location UC", "drbackup-location", "À créer, voir §2.4"],
        ["Email de notification d'échec", "exploitation@client.ch", "Choix client"],
        ["Application ID du Service Principal", "GUID", "À créer, voir §2.6"],
    ],
    col_widths=[4.5, 6, 6],
)

add_warning(
    doc,
    "l'URL du workspace Databricks et l'URL du compte de stockage ADLS sont deux choses "
    "différentes. Renseigner l'une à la place de l'autre dans la configuration de la CLI est "
    "une erreur fréquente et le message d'erreur associé n'est pas explicite. Voir Annexe A.3.",
)

add_heading(doc, "2.2 Créer le compte de stockage et le container", 2)

doc.add_paragraph("Dans le portail Azure :")
numbered(doc, "Créer un compte de stockage de type StorageV2, avec Hierarchical namespace activé (obligatoire : ADLS Gen2).")
numbered(doc, "Choisir la même région que le workspace Databricks, pour éviter les frais de transfert inter-région.")
numbered(doc, "Redondance : GRS recommandé si la protection contre la perte d'une région est un objectif ; LRS suffit sinon.")
numbered(doc, "Créer un container, par exemple uc-backup.")

add_note(
    doc,
    "ne pas réutiliser le container qui héberge les données de production. L'intérêt d'un "
    "compte séparé est qu'une suppression ou une erreur de droits côté production ne touche "
    "pas le backup.",
)

add_heading(doc, "2.3 Créer le Storage Credential dans Unity Catalog", 2)

doc.add_paragraph(
    "Unity Catalog accède au stockage via un Storage Credential adossé à une Access "
    "Connector for Azure Databricks (identité managée). C'est la méthode recommandée par "
    "Microsoft : aucune clé ni secret n'est stocké."
)

numbered(doc, "Dans le portail Azure, créer une ressource Access Connector for Azure Databricks, dans la même région.")
numbered(doc, "Sur le compte de stockage, onglet Contrôle d'accès (IAM), attribuer le rôle Contributeur aux données Blob du stockage à l'identité managée de l'Access Connector.")
numbered(doc, "Dans Databricks, menu Catalog, External Data, Credentials, Create credential. Type : Azure Managed Identity. Renseigner l'ID de ressource de l'Access Connector.")

add_code(
    doc,
    "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/\n"
    "  Microsoft.Databricks/accessConnectors/<nom-connector>",
)

add_warning(
    doc,
    "l'attribution du rôle IAM peut prendre jusqu'à 10 minutes pour se propager. Un échec "
    "d'accès immédiatement après la création n'est pas nécessairement une erreur de "
    "configuration : réessayer après quelques minutes avant d'investiguer.",
)

add_heading(doc, "2.4 Créer l'External Location", 2)

doc.add_paragraph(
    "L'External Location associe un chemin de stockage au Storage Credential créé à l'étape "
    "précédente. Sans elle, les notebooks ne peuvent pas écrire dans le container."
)

add_code(
    doc,
    "CREATE EXTERNAL LOCATION IF NOT EXISTS `drbackup-location`\n"
    "URL 'abfss://uc-backup@stclientdrbackup.dfs.core.windows.net/'\n"
    "WITH (STORAGE CREDENTIAL `drbackup-credential`);",
)

doc.add_paragraph("Vérifier immédiatement que l'accès fonctionne, depuis un notebook :")

add_code(
    doc,
    'dbutils.fs.ls("abfss://uc-backup@stclientdrbackup.dfs.core.windows.net/")',
)

add_heading(doc, "2.5 Accorder les permissions Unity Catalog", 2)

doc.add_paragraph(
    "Le principal qui exécute les jobs (Service Principal, voir §2.6) doit disposer des "
    "droits suivants :"
)

add_table(
    doc,
    ["Objet", "Permission", "Pourquoi"],
    [
        ["External Location de backup", "CREATE EXTERNAL TABLE, READ FILES, WRITE FILES", "Écrire les clones Delta"],
        ["Catalogs à sauvegarder", "USE CATALOG, USE SCHEMA, SELECT", "Lire les tables sources"],
        ["Metastore", "Aucun droit particulier", "La lecture d'information_schema suffit"],
    ],
    col_widths=[5, 6.5, 5],
)

add_code(
    doc,
    "GRANT CREATE EXTERNAL TABLE, READ FILES, WRITE FILES\n"
    "  ON EXTERNAL LOCATION `drbackup-location` TO `<application-id-du-sp>`;\n\n"
    "GRANT USE CATALOG, USE SCHEMA, SELECT\n"
    "  ON CATALOG `<catalog-a-sauvegarder>` TO `<application-id-du-sp>`;",
)

add_note(
    doc,
    "répéter le second GRANT pour chaque catalog à sauvegarder. Un catalog auquel le "
    "principal n'a pas accès est simplement absent du backup, sans erreur bloquante — d'où "
    "l'importance de la vérification du §7.",
)

add_heading(doc, "2.6 Créer le Service Principal", 2)

doc.add_paragraph(
    "Les jobs planifiés ne doivent pas dépendre d'un compte nominatif : le départ de la "
    "personne concernée interromprait les sauvegardes. Un Service Principal est donc requis."
)

doc.add_paragraph(
    "Deux types de Service Principal conviennent. Le choix détermine le secret utilisé, et "
    "donc la configuration de la CLI au §4.3 — les deux ne se configurent pas de la même "
    "manière."
)

add_table(
    doc,
    ["Type", "Créé depuis", "Droits requis"],
    [
        ["Géré par Databricks", "Console de compte Databricks", "Administrateur Databricks uniquement"],
        ["Adossé à Microsoft Entra ID", "Portail Azure, App registration", "Administrateur Microsoft Entra ID"],
    ],
    col_widths=[4, 6, 6],
)

add_note(
    doc,
    "un Service Principal géré par Databricks suffit ici, et évite de dépendre d'un "
    "administrateur Azure. L'accès au stockage ne passe pas par l'identité du Service "
    "Principal mais par le Storage Credential adossé à l'Access Connector (§2.3) : le Service "
    "Principal n'a donc besoin d'aucune identité côté Azure.",
)

add_heading(doc, "A — Option 1 : Service Principal géré par Databricks", 3)
numbered(doc, "Console de compte Databricks (accounts.azuredatabricks.net), onglet User management, Service principals, Add service principal.")
numbered(doc, "Choisir Databricks managed. Nom, par exemple sp-databricks-dr-backup.")
numbered(doc, "Ouvrir le Service Principal créé, onglet Secrets, Generate secret. Relever le Client ID et le Secret : le secret n'est affiché qu'une seule fois et ne peut pas être relu ensuite.")
numbered(doc, "Onglet Workspaces, affecter le Service Principal au workspace concerné.")

add_note(
    doc,
    "ce couple Client ID / Secret se configure avec les champs client_id et client_secret "
    "(§4.3, variante A).",
)

add_heading(doc, "B — Option 2 : Service Principal adossé à Microsoft Entra ID", 3)
numbered(doc, "Portail Azure, Microsoft Entra ID, App registrations, New registration.")
numbered(doc, "Nom, par exemple sp-databricks-dr-backup. Type de compte : single tenant.")
numbered(doc, "Relever l'Application (client) ID et le Directory (tenant) ID.")
numbered(doc, "Onglet Certificates & secrets, New client secret. Relever la valeur immédiatement : elle n'est plus affichée ensuite.")
numbered(doc, "Dans Databricks, console d'administration du workspace, Identity and access, Service principals, Add service principal, puis renseigner l'Application ID.")

add_note(
    doc,
    "ce triplet se configure avec les champs azure_client_id, azure_client_secret et "
    "azure_tenant_id (§4.3, variante B), et non avec client_id / client_secret.",
)

add_heading(doc, "C — Droits à accorder, quelle que soit l'option retenue", 3)
numbered(doc, "Entitlement Workspace access sur le Service Principal, dans la console d'administration du workspace.")
numbered(doc, "Permission Can manage sur les jobs après déploiement, ou déclarer le Service Principal propriétaire des jobs.")
numbered(doc, "GRANT du §2.5 sur l'External Location et sur chaque catalog à sauvegarder, en utilisant le Client ID (ou l'Application ID) comme principal.")

add_note(
    doc,
    "si le Service Principal n'est pas encore disponible côté Azure, il est possible de "
    "réaliser un déploiement de test avec une authentification utilisateur : voir Annexe B. "
    "Cette voie est acceptable pour valider un environnement, pas pour un usage planifié.",
)

add_heading(doc, "2.7 Installer la CLI Databricks", 2)

add_code(
    doc,
    "# Windows\nwinget install Databricks.DatabricksCLI\n\n"
    "# macOS / Linux\nbrew tap databricks/tap && brew install databricks\n\n"
    "# Verification : version 0.2xx ou superieure requise pour les bundles\ndatabricks --version",
)

add_warning(
    doc,
    "les versions 0.17 et antérieures de la CLI (ancienne implémentation Python) ne "
    "supportent pas les Asset Bundles. La commande databricks bundle est absente : c'est le "
    "symptôme d'une CLI trop ancienne.",
)

doc.add_page_break()


# ── 3. Phase 1 — Audit ────────────────────────────────────────────────────────

add_heading(doc, "3. Phase 1 — Audit de l'environnement", 1)

doc.add_paragraph(
    "Cette phase est en lecture seule. Elle mesure le volume à sauvegarder et, surtout, "
    "identifie les tables qui doivent être optimisées avant le premier backup. Elle "
    "conditionne la réussite de la suite : sur un environnement volumineux et fragmenté, un "
    "premier backup lancé sans optimisation préalable ne se termine pas dans la fenêtre "
    "allouée."
)

add_heading(doc, "3.1 Importer et exécuter le notebook d'audit", 2)

numbered(doc, "Importer diag_01_audit.py dans le workspace (menu Workspace, Import, File).")
numbered(doc, "Attacher un cluster en mode d'accès Standard ou Dédié, runtime 14.3 LTS ou supérieur.")
numbered(doc, "Exécuter le notebook. Les paramètres par défaut conviennent dans la majorité des cas.")

add_table(
    doc,
    ["Paramètre", "Défaut", "Rôle"],
    [
        ["max_parallel", "8", "Threads parallèles pour l'inventaire"],
        ["optimize_files_per_gb", "50", "Seuil de fichiers par Go au-delà duquel OPTIMIZE est recommandé"],
        ["optimize_files_min", "100", "Nombre de fichiers minimum, évite les faux positifs sur petites tables"],
        ["output_json", "false", "Sortie JSON complète"],
    ],
    col_widths=[5, 2.5, 9],
)

add_heading(doc, "3.2 Interpréter le ratio fichiers par Go", 2)

doc.add_paragraph(
    "Le DEEP CLONE copie fichier par fichier. Sa durée dépend donc davantage du nombre de "
    "fichiers que du volume total. C'est le point le plus important de cette phase."
)

add_table(
    doc,
    ["Ratio fichiers/Go", "Diagnostic", "Action"],
    [
        ["4 à 10", "Optimal (fichiers d'environ 128 Mo)", "Aucune"],
        ["10 à 50", "Acceptable", "OPTIMIZE souhaitable, non bloquant"],
        ["Supérieur à 50", "Critique", "OPTIMIZE obligatoire avant le premier backup"],
    ],
    col_widths=[4, 6, 6],
)

doc.add_paragraph("Mesure réelle relevée sur une table de 28 Go fortement fragmentée :")

add_table(
    doc,
    ["", "Avant OPTIMIZE", "Après OPTIMIZE"],
    [
        ["Nombre de fichiers", "346 397", "6 570 (-98 %)"],
        ["Durée du DEEP CLONE", "Plusieurs heures", "134 secondes"],
    ],
    col_widths=[5, 5.5, 5.5],
)

add_heading(doc, "3.3 Optimiser les tables signalées", 2)

add_code(
    doc,
    "OPTIMIZE `catalog`.`schema`.`table`;\n\n"
    "-- Pour eviter la refragmentation sur les tables actives :\n"
    "ALTER TABLE `catalog`.`schema`.`table` SET TBLPROPERTIES (\n"
    "  'delta.autoOptimize.optimizeWrite' = 'true',\n"
    "  'delta.autoOptimize.autoCompact'   = 'true'\n"
    ");",
)

add_note(
    doc,
    "l'OPTIMIZE est une opération coûteuse mais qui ne se paie qu'une fois, et qui bénéficie "
    "aussi aux requêtes de production. Le planifier sur un créneau creux, idéalement le "
    "dimanche soir.",
)

doc.add_page_break()


# ── 4. Phase 2 — Déploiement ──────────────────────────────────────────────────

add_heading(doc, "4. Phase 2 — Déploiement de la solution", 1)

add_heading(doc, "4.1 Extraire la livraison", 2)

doc.add_paragraph("L'archive fournie par KeyIT contient :")

add_code(
    doc,
    "databricks.yml            definition des jobs (Asset Bundle)\n"
    "notebooks/                pipeline de backup et de restauration\n"
    "lib/                      modules partages (diff, rapport, templates)\n"
    "scripts/                  utilitaires de restauration hors Databricks\n"
    "docs/                     guides de deploiement et de restauration",
)

add_warning(
    doc,
    "le dossier lib/ est indispensable : les notebooks 03 et 04 en dépendent. Une extraction "
    "partielle de l'archive provoque un échec de l'étape de rapport.",
)

add_heading(doc, "4.2 Adapter databricks.yml", 2)

doc.add_paragraph(
    "Quatre valeurs sont à adapter à l'environnement cible. Tout le reste peut rester en "
    "configuration par défaut."
)

add_table(
    doc,
    ["Variable", "Valeur à renseigner"],
    [
        ["backup_root", "abfss://<container>@<compte>.dfs.core.windows.net/backup"],
        ["lib_path", "Chemin workspace du dossier lib/ après déploiement"],
        ["notification_email", "Adresse destinataire des alertes d'échec"],
        ["workspace.host (par cible)", "URL du workspace Databricks"],
    ],
    col_widths=[5, 11],
)

add_code(
    doc,
    "variables:\n"
    "  backup_root:\n"
    "    default: \"abfss://uc-backup@stclientdrbackup.dfs.core.windows.net/backup\"\n"
    "  notification_email:\n"
    "    default: \"exploitation@client.ch\"\n\n"
    "targets:\n"
    "  prod:\n"
    "    mode: production\n"
    "    workspace:\n"
    "      host: https://adb-1234567890123456.7.azuredatabricks.net",
)

add_heading(doc, "4.3 Configurer l'authentification de la CLI", 2)

doc.add_paragraph(
    "La CLI lit ses identifiants dans un fichier de profils. Chaque profil est une section "
    "indépendante : ajouter un profil ne modifie jamais les autres."
)

add_code(doc, "# Emplacement\n# Linux / macOS : ~/.databrickscfg\n# Windows       : %USERPROFILE%\\.databrickscfg")

add_warning(
    doc,
    "les champs à renseigner dépendent du type de Service Principal choisi au §2.6. Un secret "
    "Microsoft Entra ID placé dans les champs client_id / client_secret produit une erreur 401 "
    "à l'authentification. Voir Annexe A.7.",
)

add_heading(doc, "Variante A — Service Principal géré par Databricks (§2.6, option 1)", 3)

add_code(
    doc,
    "[prod]\n"
    "host          = https://adb-1234567890123456.7.azuredatabricks.net\n"
    "client_id     = <client-id-du-service-principal>\n"
    "client_secret = <secret-genere-dans-databricks>",
)

add_heading(doc, "Variante B — Service Principal Microsoft Entra ID (§2.6, option 2)", 3)

add_code(
    doc,
    "[prod]\n"
    "host                = https://adb-1234567890123456.7.azuredatabricks.net\n"
    "azure_client_id     = <application-id>\n"
    "azure_client_secret = <client-secret-entra-id>\n"
    "azure_tenant_id     = <directory-tenant-id>",
)

add_warning(
    doc,
    "le champ host attend l'URL du workspace Databricks, jamais celle du compte de stockage. "
    "Voir Annexe A.3.",
)

add_heading(doc, "Vérifier la configuration", 3)

add_code(
    doc,
    "databricks auth profiles                    # le profil doit apparaitre Valid : YES\n"
    "databricks current-user me --profile prod\n"
    "databricks jobs list --profile prod         # confirme l'acces effectif au workspace",
)

doc.add_paragraph(
    "Pour un Service Principal, la réponse ne contient pas d'adresse e-mail : le champ "
    "userName vaut le Client ID. C'est le comportement attendu."
)

add_code(
    doc,
    "{\n"
    '  "active": true,\n'
    '  "displayName": "sp-databricks-dr-backup",\n'
    '  "userName": "12345678-90ab-cdef-1234-567890abcdef"\n'
    "}",
)

add_heading(doc, "Gérer plusieurs environnements sans risque d'erreur", 3)

doc.add_paragraph(
    "Lorsque plusieurs workspaces sont administrés depuis le même poste, le risque n'est pas "
    "que les profils interfèrent entre eux, mais d'exécuter une commande sur le mauvais. Trois "
    "précautions suffisent :"
)

bullet(doc, "Nommer les profils par environnement explicite (client-prod, client-test) plutôt que prod ou dev, ambigus dès le deuxième projet.")
bullet(doc, "Ne pas définir de profil par défaut : une commande sans --profile échouera franchement, au lieu de s'exécuter sur un workspace choisi implicitement.")
bullet(doc, "Associer le profil à la cible directement dans databricks.yml, ce qui rend le --profile inutile et protège contre l'erreur de cible.")

add_code(
    doc,
    "targets:\n"
    "  prod:\n"
    "    workspace:\n"
    "      host: https://adb-1234567890123456.7.azuredatabricks.net\n"
    "      profile: client-prod",
)

add_note(
    doc,
    "avec cette déclaration, la CLI refuse de déployer si le host du profil ne correspond pas "
    "à celui déclaré dans la cible. C'est le garde-fou le plus efficace contre un déploiement "
    "sur le mauvais workspace.",
)

add_warning(
    doc,
    "les variables d'environnement DATABRICKS_HOST et DATABRICKS_TOKEN sont prioritaires sur "
    "le fichier de profils. Définies de façon permanente, elles détournent silencieusement "
    "toutes les commandes, y compris celles des autres projets. Ne les utiliser que dans un "
    "terminal ponctuel.",
)

add_heading(doc, "4.4 Tester la connectivité de bout en bout", 2)

doc.add_paragraph(
    "Le notebook diag_02_connectivity.py valide la chaîne complète : accès à l'External "
    "Location, écriture, DEEP CLONE réel puis nettoyage. Il n'écrit que dans un sous-dossier "
    "_diag_test/ qu'il supprime en fin d'exécution."
)

add_table(
    doc,
    ["Paramètre", "Valeur"],
    [
        ["backup_root", "Obligatoire, même valeur que dans databricks.yml"],
        ["test_table", "Optionnel, vide pour sélection automatique de la plus petite table"],
    ],
    col_widths=[4, 12],
)

add_warning(
    doc,
    "ce notebook exige un cluster en mode d'accès Dédié (Single User). En mode Standard, "
    "l'écriture vers l'External Location échoue : le passage d'identité vers Unity Catalog "
    "n'est pas disponible dans ce mode.",
)

add_heading(doc, "4.5 Déployer le bundle", 2)

add_code(
    doc,
    "databricks bundle validate --target prod --profile prod\n"
    "databricks bundle deploy   --target prod --profile prod",
)

doc.add_paragraph("Le déploiement crée deux jobs, tous deux en pause :")

add_table(
    doc,
    ["Job", "Rôle", "Planification"],
    [
        ["dr-backup-daily", "Sauvegarde quotidienne incrémentale, rétention, VACUUM", "01:00 UTC, en pause"],
        ["dr-backup-monthly", "Archive mensuelle complète par DEEP CLONE", "1er du mois, 02:00 UTC, en pause"],
    ],
    col_widths=[4.5, 8, 3.5],
)

add_note(
    doc,
    "les jobs sont livrés en pause volontairement. Ils ne se déclencheront pas d'eux-mêmes "
    "avant l'activation décrite au §8, ce qui laisse le temps de valider le premier backup "
    "manuellement.",
)

add_warning(
    doc,
    "si bundle deploy échoue avec une erreur de connexion à registry.terraform.io, le "
    "problème est réseau et non lié à la solution. Un contournement est décrit en Annexe A.1.",
)

add_heading(doc, "4.6 Archivage des logs de cluster", 2)

doc.add_paragraph(
    "Les clusters des jobs sont créés à chaque exécution puis détruits : leurs logs "
    "disparaissent avec eux, et la sortie affichée dans l'interface est tronquée au-delà d'un "
    "certain volume. Pour permettre un diagnostic après coup, les deux jobs archivent "
    "automatiquement leurs logs."
)

add_code(
    doc,
    "cluster_log_conf:\n"
    "  dbfs:\n"
    "    destination: dbfs:/cluster-logs/dr-backup/daily",
)

add_table(
    doc,
    ["Job", "Destination"],
    [
        ["dr-backup-daily", "dbfs:/cluster-logs/dr-backup/daily"],
        ["dr-backup-monthly", "dbfs:/cluster-logs/dr-backup/monthly"],
    ],
    col_widths=[5, 11],
)

doc.add_paragraph(
    "Chaque exécution y dépose un sous-dossier daté contenant les journaux du driver "
    "(stdout, stderr, log4j). Consultation depuis un notebook :"
)

add_code(doc, 'dbutils.fs.ls("dbfs:/cluster-logs/dr-backup/daily")')

add_warning(
    doc,
    "cette destination doit être un chemin DBFS : le paramètre cluster_log_conf n'accepte pas "
    "d'URI abfss://. Si la politique du workspace interdit l'accès à DBFS, retirer les blocs "
    "cluster_log_conf de databricks.yml — les jobs fonctionnent sans, seul le diagnostic "
    "a posteriori est perdu.",
)

add_note(
    doc,
    "ces logs ne sont pas purgés automatiquement. Prévoir un nettoyage périodique si l'espace "
    "occupé devient un sujet.",
)

doc.add_page_break()


# ── 5. Premier backup ─────────────────────────────────────────────────────────

add_heading(doc, "5. Premier backup", 1)

add_heading(doc, "5.1 Dimensionner le cluster", 2)

doc.add_paragraph(
    "Le DEEP CLONE se parallélise sur les exécuteurs Spark. Le dimensionnement du cluster est "
    "donc le facteur déterminant de la durée du backup, en particulier lors du premier "
    "passage où toutes les tables sont copiées intégralement."
)

doc.add_paragraph("Mesures relevées sur un environnement de production comportant environ 4 800 tables :")

add_table(
    doc,
    ["Configuration", "Débit observé", "Conclusion"],
    [
        ["Single Node (aucun worker)", "environ 17 s par table", "1 072 tables traitées en 5 h, dépassement de la fenêtre"],
        ["4 workers Standard_DS4_v2", "environ 1,3 s par table", "2 352 tables traitées en 50 min"],
    ],
    col_widths=[5, 4, 7],
)

doc.add_paragraph(
    "La configuration livrée par défaut applique donc 4 workers Standard_DS4_v2 sur le job "
    "quotidien. Elle peut être réduite sur un environnement de petite taille :"
)

add_table(
    doc,
    ["Nombre de tables", "Configuration recommandée"],
    [
        ["Moins de 500", "Single Node acceptable"],
        ["500 à 5 000", "4 workers (configuration livrée)"],
        ["Plus de 5 000", "4 à 8 workers, à valider par le premier run"],
    ],
    col_widths=[5, 11],
)

add_warning(
    doc,
    "si le nombre de workers est ramené à 0, il faut aussi rétablir les paramètres de cluster "
    "mono-nœud (spark.master en local, profil singleNode, tag ResourceClass). Un cluster "
    "déclaré à 0 worker sans ces paramètres ne démarre pas.",
)

add_heading(doc, "5.2 Durées attendues", 2)

add_table(
    doc,
    ["Exécution", "Ce qui est traité", "Durée typique"],
    [
        ["Premier run (J1)", "Toutes les tables, copie intégrale", "Plusieurs heures, proportionnel au volume"],
        ["Runs suivants (J2 et au-delà)", "Uniquement les tables modifiées depuis la veille", "30 min à 2 h"],
    ],
    col_widths=[4, 6, 6],
)

doc.add_paragraph(
    "L'écart entre les deux vient d'un mécanisme de détection de changement : avant chaque "
    "clone, la version Delta de la table source est comparée à celle enregistrée lors du "
    "dernier backup réussi. Si elle est identique, la table est ignorée. Sur un environnement "
    "où peu de tables changent, la réduction est considérable — un cycle mesuré sur 18 tables "
    "inchangées est passé de 2 minutes à 6 secondes."
)

add_heading(doc, "5.3 Comportement en cas d'interruption", 2)

doc.add_paragraph(
    "Un dépassement du délai maximal du job n'est pas une perte de travail. Deux mécanismes "
    "distincts assurent la reprise, selon le moment de la relance."
)

add_table(
    doc,
    ["Relance", "Mécanisme", "Effet"],
    [
        ["Le même jour", "Checkpoint écrit toutes les 5 tables dans incremental/_checkpoints/{date}.json", "Reprise à la table près"],
        ["Un jour plus tard", "Versions Delta déjà sauvegardées, enregistrées dans incremental/_last_versions.json", "Les tables déjà traitées sont ignorées"],
    ],
    col_widths=[3, 7.5, 5.5],
)

doc.add_paragraph(
    "Sur un environnement volumineux, un premier backup peut donc être étalé délibérément sur "
    "plusieurs nuits successives, sans jamais recloner ce qui est déjà sauvegardé."
)

add_heading(doc, "Forcer une reprise exacte", 3)

doc.add_paragraph(
    "Le checkpoint est indexé par date. Pour reprendre un run interrompu avec la précision de "
    "la table, relancer le job en renseignant la date de ce run plutôt que celle du jour, via "
    "« Run now with different parameters » :"
)

add_code(doc, "backup_date = 2026-08-06      # la date du run interrompu")

add_note(
    doc,
    "laissé vide, ce paramètre vaut la date du jour, ce qui est le comportement normal d'une "
    "exécution planifiée.",
)

add_warning(
    doc,
    "une archive mensuelle ne sert pas de point de départ à la sauvegarde quotidienne. Les "
    "deux écrivent dans des arborescences distinctes (snapshots/monthly/ et incremental/), et "
    "c'est cette indépendance qui protège l'archive d'une corruption de l'incrémental. Lancer "
    "un mensuel n'accélère donc pas le premier passage quotidien : seuls l'OPTIMIZE préalable "
    "(§3) et le dimensionnement du cluster (§5.1) agissent dessus.",
)

add_heading(doc, "5.4 Lancer le premier backup", 2)

add_code(doc, "databricks bundle run dr_backup_daily --target prod --profile prod")

doc.add_paragraph("Ou depuis l'interface : menu Workflows, job dr-backup-daily, Run now.")

add_table(
    doc,
    ["Paramètre", "Défaut", "Rôle"],
    [
        ["backup_root", "valeur de databricks.yml", "Racine de stockage du backup"],
        ["retain_daily", "30", "Fenêtre de restauration point-in-time, en jours"],
        ["retain_monthly", "3", "Nombre d'archives mensuelles conservées"],
        ["max_parallel", "8", "Opérations simultanées : clones et VACUUM"],
        ["vacuum_dow", "7", "Jour du VACUUM, 7 = dimanche (voir §6.3)"],
        ["backup_date", "vide", "Vide = date du jour. À renseigner pour reprendre un run interrompu (voir §5.3)"],
        ["dry_run", "false", "Simulation, aucune écriture ni suppression"],
    ],
    col_widths=[4, 3.5, 8.5],
)

doc.add_page_break()


# ── 6. Rétention ──────────────────────────────────────────────────────────────

add_heading(doc, "6. Politique de rétention", 1)

add_heading(doc, "6.1 Niveaux configurés", 2)

add_table(
    doc,
    ["Niveau", "Mécanisme", "Rétention par défaut", "Restauration possible"],
    [
        ["Quotidien", "Historique Delta de incremental/", "30 jours", "N'importe quel instant des 30 derniers jours"],
        ["Mensuel", "DEEP CLONE vers snapshots/monthly/", "3 mois", "État du 1er du mois concerné"],
    ],
    col_widths=[3, 4.5, 3.5, 5],
)

add_heading(doc, "6.2 Pourquoi il n'y a pas de niveau hebdomadaire", 2)

doc.add_paragraph(
    "Un niveau hebdomadaire existait dans les versions antérieures, sous forme de SHALLOW "
    "CLONE. Unity Catalog restreint désormais cette opération aux tables managées par le "
    "catalog, ce qui exclut par construction les tables de backup, référencées par chemin de "
    "stockage. Le message d'erreur correspondant est :"
)

add_code(doc, "CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE_AS_SOURCE_OR_TARGET")

doc.add_paragraph(
    "Le remplacer par un DEEP CLONE aurait représenté une copie complète chaque semaine, soit "
    "un coût de calcul du même ordre que l'archive mensuelle. Le niveau hebdomadaire a donc "
    "été supprimé et la rétention quotidienne portée de 15 à 30 jours pour compenser la "
    "granularité perdue. La couverture réelle de restauration est inchangée, voire meilleure."
)

add_note(
    doc,
    "seul le niveau hebdomadaire était concerné. L'incrémental quotidien, l'archive mensuelle "
    "et la restauration reposent sur DEEP CLONE, non soumis à cette restriction.",
)

add_heading(doc, "6.3 Purge effective des fichiers (VACUUM)", 2)

doc.add_paragraph(
    "Les propriétés de rétention Delta définissent un seuil de sécurité : elles empêchent la "
    "suppression de fichiers encore nécessaires, mais ne suppriment rien d'elles-mêmes. Sans "
    "commande VACUUM, le stockage croît indéfiniment. L'étape 6.5 du notebook de rétention "
    "exécute donc un VACUUM sur chaque table de incremental/."
)

doc.add_paragraph("Le VACUUM est appelé sans clause RETAIN explicite, afin de toujours respecter le seuil configuré et de ne jamais raccourcir la fenêtre de restauration annoncée.")

add_heading(doc, "Fréquence : hebdomadaire, et pourquoi", 3)

doc.add_paragraph(
    "Le VACUUM doit lister récursivement tous les fichiers de chaque table pour les comparer au "
    "journal des transactions. Ce coût est payé intégralement même lorsque rien n'est à "
    "supprimer. Mesure relevée en production avant ajustement :"
)

add_table(
    doc,
    ["Mesure", "Valeur"],
    [
        ["Tables traitées", "2 398"],
        ["Durée de l'étape", "2 h 40, soit plus de la moitié du backup quotidien"],
        ["Fichiers effectivement supprimés", "0"],
    ],
    col_widths=[6, 10],
)

doc.add_paragraph(
    "Un VACUUM hebdomadaire purge exactement autant qu'un VACUUM quotidien : il rattrape "
    "plusieurs jours en une fois. La fréquence est donc réglée sur une exécution par semaine, "
    "et l'étape est parallélisée sur max_parallel."
)

add_table(
    doc,
    ["Paramètre", "Défaut", "Rôle"],
    [
        ["vacuum_dow", "7", "Jour d'exécution : 1 = lundi … 7 = dimanche. 0 = tous les jours"],
        ["enable_vacuum", "true", "Désactivation complète. Vaut false dans le job mensuel, qui ne doit pas dupliquer le VACUUM du quotidien"],
    ],
    col_widths=[4, 2.5, 9.5],
)

add_warning(
    doc,
    "ne pas chercher à restreindre le VACUUM aux seules tables modifiées dans la journée. Un "
    "fichier devient éligible à la suppression lorsqu'il dépasse le seuil de rétention, pas "
    "lorsque la table change : une table restée statique depuis 40 jours a des fichiers qui "
    "franchissent le seuil aujourd'hui sans qu'elle ait bougé. Ils ne seraient alors jamais "
    "purgés.",
)

add_note(
    doc,
    "les six autres nuits, l'étape affiche « VACUUM ignoré » suivi du jour planifié. C'est le "
    "comportement attendu, pas une étape manquante.",
)

add_warning(
    doc,
    "lors des premières exécutions, le VACUUM rapporte 0 fichier supprimé. C'est le résultat "
    "attendu et non un dysfonctionnement : aucun fichier n'est encore sorti de la fenêtre de "
    "30 jours. Des suppressions n'apparaîtront qu'après 30 jours de fonctionnement.",
)

add_heading(doc, "6.4 Vérifier que la rétention est réellement appliquée", 2)

doc.add_paragraph(
    "Les propriétés de rétention sont posées sur chaque table de backup au moment du clone. "
    "Pour les contrôler :"
)

add_code(
    doc,
    "DESCRIBE DETAIL delta.`abfss://uc-backup@stclientdrbackup.dfs.core.windows.net/\n"
    "  backup/incremental/<catalog>/<schema>/<table>`;",
)

doc.add_paragraph("Valeurs attendues pour retain_daily = 30 :")

add_table(
    doc,
    ["Propriété", "Valeur attendue"],
    [
        ["delta.logRetentionDuration", "interval 33 days"],
        ["delta.deletedFileRetentionDuration", "interval 32 days"],
    ],
    col_widths=[7, 9],
)

add_note(
    doc,
    "Delta impose que la rétention du journal des transactions couvre au moins celle des "
    "fichiers de données, d'où le jour d'écart. Ces propriétés sont écrites lors du clone : "
    "une table ignorée parce qu'inchangée conserve les valeurs posées lors de son dernier "
    "clone effectif.",
)

doc.add_page_break()


# ── 7. Validation ─────────────────────────────────────────────────────────────

add_heading(doc, "7. Validation post-déploiement", 1)

doc.add_paragraph(
    "À exécuter après le premier backup, avant l'activation des planifications. Chaque point "
    "indique la sortie attendue dans les journaux d'exécution du job."
)

add_table(
    doc,
    ["#", "Vérification", "Où regarder", "Sortie attendue"],
    [
        ["1", "Métadonnées UC exportées", "Journal 01_uc_metadata", "Nombre de catalogs, schémas et tables cohérent avec l'audit du §3"],
        ["2", "Tables clonées sans erreur", "Journal 02_data_clone", "Aucune ligne ERREUR ; les tables ignorées le sont pour un motif explicite (vue, format non clonable)"],
        ["3", "Détection de changement active", "Journal 02_data_clone, second run", "Mention d'une version Delta inchangée sur les tables non modifiées"],
        ["4", "Reprise sur interruption", "Stockage, incremental/_checkpoints/", "Un fichier daté du jour, contenant les tables déjà traitées"],
        ["5", "Configuration du workspace", "Journal 00_orchestrator", "Nombre de jobs et de notebooks chargés dans le manifeste, sans avertissement"],
        ["6", "Rétention effectivement posée", "Requête DESCRIBE DETAIL du §6.4", "interval 33 days et interval 32 days"],
        ["7", "VACUUM exécuté", "Journal 06_retention, étape 6.5", "Le jour planifié : une ligne par table, 0 fichier supprimé est normal avant 30 jours. Les autres jours : « VACUUM ignoré »"],
        ["8", "Restauration fonctionnelle", "Notebook 07_restore en simulation", "Les commandes de restauration sont affichées, aucune n'est exécutée"],
    ],
    col_widths=[1, 4, 4.5, 6.5],
)

add_heading(doc, "7.1 À ajuster selon les résultats", 2)

doc.add_paragraph(
    "Le paramètre max_parallel vaut 8 par défaut. Cette valeur a été calibrée sur une "
    "configuration mono-nœud ; sur un cluster multi-worker, le parallélisme utile provient "
    "des exécuteurs Spark et non des threads du pilote. Si le premier run montre un pilote "
    "saturé alors que les workers sont peu chargés, réduire max_parallel à 4 et comparer."
)

doc.add_page_break()


# ── 8. Schedules ──────────────────────────────────────────────────────────────

add_heading(doc, "8. Activer les planifications", 1)

doc.add_paragraph(
    "À faire uniquement après validation du §7. Éditer databricks.yml, puis redéployer."
)

add_code(
    doc,
    "schedule:\n"
    "  quartz_cron_expression: \"0 0 1 * * ?\"\n"
    "  timezone_id: UTC\n"
    "  pause_status: UNPAUSED      # etait PAUSED",
)

add_code(doc, "databricks bundle deploy --target prod --profile prod")

add_table(
    doc,
    ["Job", "Planification par défaut", "Délai maximal"],
    [
        ["dr-backup-daily", "Chaque nuit à 01:00 UTC", "8 heures"],
        ["dr-backup-monthly", "Le 1er de chaque mois à 02:00 UTC", "24 heures"],
    ],
    col_widths=[4.5, 7, 4.5],
)

add_note(
    doc,
    "les notifications d'échec sont envoyées à l'adresse configurée dans notification_email. "
    "Il est recommandé de provoquer volontairement un échec une fois (par exemple avec un "
    "chemin de stockage erroné) pour vérifier que l'alerte arrive bien, plutôt que de le "
    "découvrir lors d'un incident réel.",
)

doc.add_page_break()


# ── 9. Restauration ───────────────────────────────────────────────────────────

add_heading(doc, "9. Restauration", 1)

doc.add_paragraph(
    "La procédure détaillée figure dans le document DR Backup — Guide de restauration, livré "
    "avec cette solution. Ce chapitre en donne la vue d'ensemble."
)

add_table(
    doc,
    ["Notebook", "Restaure", "Portée"],
    [
        ["07_restore", "Tables Delta", "Une table, un schéma, un catalog, ou l'intégralité"],
        ["08_restore_workspace", "Notebooks sources, ACL, repos Git", "Sélectif par chemin"],
        ["09_restore_jobs", "Définitions de jobs", "Tous les jobs sauvegardés"],
        ["10_restore_orchestrator", "Plusieurs périmètres à la fois", "Orchestration de 07, 08, 09 et 11"],
        ["11_restore_grants", "Permissions Unity Catalog", "Catalog, schéma, table"],
    ],
    col_widths=[4.5, 5, 6.5],
)

add_warning(
    doc,
    "tous ces notebooks démarrent en mode simulation (dry_run à true) et n'écrivent rien "
    "tant que ce paramètre n'est pas explicitement passé à false. Toujours exécuter une "
    "simulation d'abord et lire les commandes proposées.",
)

add_heading(doc, "9.1 Restauration ponctuelle sans notebook", 2)

doc.add_paragraph("Pour une table isolée, la restauration peut se faire directement en SQL :")

add_code(
    doc,
    "-- Depuis l'incremental, a une date donnee\n"
    "CREATE OR REPLACE TABLE `catalog`.`schema`.`table`\n"
    "DEEP CLONE delta.`abfss://.../backup/incremental/catalog/schema/table`\n"
    "  TIMESTAMP AS OF '2026-07-15';\n\n"
    "-- Depuis une archive mensuelle\n"
    "CREATE OR REPLACE TABLE `catalog`.`schema`.`table`\n"
    "DEEP CLONE delta.`abfss://.../backup/snapshots/monthly/2026-07/catalog/schema/table`;",
)

add_note(
    doc,
    "restaurer vers un catalog temporaire plutôt que d'écraser la table de production permet "
    "de comparer avant de basculer. Les notebooks de restauration créent automatiquement le "
    "schéma cible s'il n'existe pas.",
)

doc.add_page_break()


# ── 10. Checklist ─────────────────────────────────────────────────────────────

add_heading(doc, "10. Checklist de déploiement", 1)

add_table(
    doc,
    ["Fait", "Étape", "Section"],
    [
        ["", "Compte de stockage et container créés", "2.2"],
        ["", "Storage Credential créé et rôle IAM attribué", "2.3"],
        ["", "External Location créée et accès vérifié", "2.4"],
        ["", "GRANT accordés sur l'External Location et les catalogs", "2.5"],
        ["", "Service Principal créé et déclaré dans Databricks", "2.6"],
        ["", "CLI Databricks installée, version 0.2xx ou supérieure", "2.7"],
        ["", "Audit exécuté, tables à optimiser identifiées", "3.1"],
        ["", "OPTIMIZE réalisé sur les tables signalées", "3.3"],
        ["", "databricks.yml adapté à l'environnement", "4.2"],
        ["", "Profil CLI configuré et connexion vérifiée", "4.3"],
        ["", "Test de connectivité de bout en bout réussi", "4.4"],
        ["", "Bundle déployé, deux jobs visibles dans Workflows", "4.5"],
        ["", "Cluster dimensionné selon le nombre de tables", "5.1"],
        ["", "Premier backup exécuté manuellement", "5.4"],
        ["", "Les 8 points de validation sont vérifiés", "7"],
        ["", "Alerte d'échec testée volontairement", "8"],
        ["", "Restauration testée en simulation", "9"],
        ["", "Planifications activées", "8"],
    ],
    col_widths=[1.5, 11, 3.5],
)

doc.add_page_break()


# ── Annexe A — Dépannage ──────────────────────────────────────────────────────

add_heading(doc, "Annexe A — Dépannage", 1)

doc.add_paragraph(
    "Les erreurs suivantes ont toutes été rencontrées lors de déploiements réels. Elles sont "
    "documentées avec leur message exact pour permettre une recherche directe."
)

add_heading(doc, "A.1 bundle deploy échoue sur registry.terraform.io", 2)

add_code(doc, "Error: terraform init: could not connect to registry.terraform.io")

doc.add_paragraph(
    "La CLI télécharge un fournisseur Terraform lors du premier déploiement. Un proxy ou un "
    "pare-feu d'entreprise peut bloquer cet accès, y compris lorsque le même hôte répond "
    "correctement à un curl : le client Terraform n'utilise pas nécessairement la même "
    "configuration de proxy que le système."
)

doc.add_paragraph("Contournement, si le déblocage réseau n'est pas immédiat : importer les notebooks directement, sans passer par Terraform.")

add_code(
    doc,
    "databricks workspace import /Workspace/Shared/dr-backup/notebooks/02_data_clone \\\n"
    "  --file notebooks/02_data_clone.py --language PYTHON \\\n"
    "  --format SOURCE --overwrite --profile prod",
)

add_note(
    doc,
    "sous Git Bash pour Windows, préfixer la commande par MSYS_NO_PATHCONV=1, sans quoi le "
    "chemin /Workspace/... est réécrit en chemin Windows local et l'import échoue.",
)

add_warning(
    doc,
    "ce contournement pousse le code mais ne crée pas les jobs. Il permet de corriger un "
    "notebook sur un déploiement existant ; il ne remplace pas un bundle deploy initial.",
)

add_heading(doc, "A.2 Erreur 403 lors de l'utilisation d'un token personnel", 2)

add_code(doc, "Error: 403 Forbidden — insufficient scope: all-apis")

doc.add_paragraph(
    "Un token d'accès personnel créé avec une restriction de périmètre ne couvre pas "
    "l'ensemble des API nécessaires au déploiement. Recréer le token sans restriction de "
    "périmètre, ou utiliser un Service Principal (§2.6)."
)

add_heading(doc, "A.3 URL de stockage renseignée à la place de l'URL du workspace", 2)

doc.add_paragraph(
    "Erreur fréquente lors de la configuration de la CLI. Le champ host attend l'URL du "
    "workspace Databricks."
)

add_table(
    doc,
    ["Champ", "Valeur attendue"],
    [
        ["host (profil CLI)", "https://adb-<id>.<n>.azuredatabricks.net"],
        ["backup_root (databricks.yml)", "abfss://<container>@<compte>.dfs.core.windows.net/backup"],
    ],
    col_widths=[5, 11],
)

add_heading(doc, "A.4 SCHEMA_NOT_FOUND lors d'une restauration", 2)

add_code(doc, "[SCHEMA_NOT_FOUND] The schema `<catalog>`.`<schema>` cannot be found")

doc.add_paragraph(
    "Se produit lors d'une restauration vers un catalog neuf : un DEEP CLONE ne crée pas le "
    "schéma cible. Les notebooks de restauration exécutent désormais un CREATE SCHEMA IF NOT "
    "EXISTS au préalable. En cas de restauration manuelle en SQL, créer le schéma avant le "
    "clone."
)

add_heading(doc, "A.5 Échec d'écriture vers l'External Location", 2)

doc.add_paragraph("Trois causes possibles, à vérifier dans cet ordre :")

numbered(doc, "Le cluster n'est pas en mode d'accès Dédié (Single User). C'est la cause la plus fréquente : le passage d'identité vers Unity Catalog n'existe pas en mode Standard.")
numbered(doc, "Les GRANT du §2.5 n'ont pas été accordés au principal qui exécute le job.")
numbered(doc, "Le rôle IAM sur le compte de stockage n'est pas encore propagé. Attendre 10 minutes et réessayer.")

add_heading(doc, "A.6 Une table est absente du backup sans message d'erreur", 2)

doc.add_paragraph("Comportement attendu dans les cas suivants :")

bullet(doc, "Vue, vue matérialisée ou Streaming Table : non clonable par DEEP CLONE")
bullet(doc, "Table dans un format autre que Delta")
bullet(doc, "Catalog auquel le principal du job n'a pas accès en lecture")
bullet(doc, "Catalog exclu par conception : hive_metastore, system, samples, information_schema")

doc.add_paragraph(
    "Le point 3 est le seul réellement problématique et le seul silencieux. C'est la raison "
    "pour laquelle le point 1 de la validation du §7 consiste à recouper le nombre de tables "
    "sauvegardées avec celui relevé lors de l'audit."
)

add_heading(doc, "A.7 Erreur 401 à l'authentification du Service Principal", 2)

add_code(doc, "Error: 401 Unauthorized")

doc.add_paragraph(
    "Cause la plus fréquente : le secret renseigné ne correspond pas au type de champ utilisé. "
    "Un Service Principal peut détenir deux secrets de nature différente, qui ne se "
    "configurent pas avec les mêmes clés."
)

add_table(
    doc,
    ["Origine du secret", "Champs à utiliser"],
    [
        ["Généré dans Databricks (console de compte, onglet Secrets)", "client_id, client_secret"],
        ["Généré dans Microsoft Entra ID (App registration)", "azure_client_id, azure_client_secret, azure_tenant_id"],
    ],
    col_widths=[8, 8],
)

doc.add_paragraph("Autres causes, à vérifier ensuite :")

bullet(doc, "Secret expiré : un secret Entra ID a une durée de validité définie à sa création.")
bullet(doc, "Secret tronqué à la copie, ou espace résiduel en fin de ligne dans le fichier de profils.")
bullet(doc, "Fichier enregistré sous un nom incorrect. Sous Windows, le Bloc-notes ajoute une extension .txt si le nom n'est pas saisi entre guillemets.")

add_note(
    doc,
    "une erreur 401 signifie que l'identité n'a pas été reconnue. Une erreur 403 signifie "
    "l'inverse : l'identité est valide mais ne dispose pas des droits nécessaires. Distinguer "
    "les deux oriente immédiatement le diagnostic.",
)

doc.add_page_break()


# ── Annexe B — OAuth ──────────────────────────────────────────────────────────

add_heading(doc, "Annexe B — Déploiement de test avec authentification utilisateur", 1)

doc.add_paragraph(
    "La création d'un Service Principal requiert des droits d'administration sur Microsoft "
    "Entra ID, qui ne sont pas toujours disponibles immédiatement. Pour valider un "
    "environnement sans attendre, la CLI accepte une authentification utilisateur par OAuth."
)

add_code(
    doc,
    "databricks auth login \\\n"
    "  --host https://adb-1234567890123456.7.azuredatabricks.net \\\n"
    "  --profile test\n\n"
    "databricks current-user me --profile test\n"
    "databricks bundle deploy --target dev --profile test",
)

doc.add_paragraph(
    "Le navigateur s'ouvre pour l'authentification, puis le jeton est mis en cache "
    "localement. Aucun secret n'est stocké en clair."
)

add_warning(
    doc,
    "cette méthode convient à la validation d'un environnement, pas à un usage planifié. Les "
    "jobs s'exécuteraient sous une identité nominative : les sauvegardes s'interrompraient au "
    "départ de la personne ou à la révocation de son accès. Basculer sur le Service Principal "
    "(§2.6) avant activation des planifications du §8.",
)

add_heading(doc, "B.1 Cibles de déploiement", 2)

doc.add_paragraph(
    "Le bundle définit deux cibles, ce qui permet de valider sur un environnement de test "
    "sans toucher à la production. Vérifier que backup_root et workspace.host de la cible dev "
    "pointent bien vers l'environnement de test avant tout déploiement."
)

add_table(
    doc,
    ["Cible", "Usage", "Commande"],
    [
        ["dev", "Validation, tests", "databricks bundle deploy --target dev"],
        ["prod", "Production", "databricks bundle deploy --target prod"],
    ],
    col_widths=[2.5, 4.5, 9],
)

add_note(
    doc,
    "en cible dev, les jobs sont préfixés du nom de l'utilisateur et restent en pause, ce qui "
    "évite toute confusion avec les jobs de production dans la liste des Workflows.",
)


# ── Écriture ──────────────────────────────────────────────────────────────────

out_dir = os.path.dirname(os.path.abspath(__file__))
out_path = os.path.join(out_dir, "DR_Backup_Guide_Deploiement_v4.1.docx")
doc.save(out_path)
print(f"[OK] Guide genere : {out_path}")
