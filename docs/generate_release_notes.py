"""Génère DR_Backup_Notes_de_Version_v4.2.docx.

Document d'accompagnement d'une relivraison : ce qui change, ce que le client va constater,
ce qu'il doit faire. La charte est partagée avec le guide de déploiement via docs/docx_style.py.
Les notes de la v4.1 restent dans l'historique git et dans le .docx déjà livré.

Usage : python docs/generate_release_notes.py
"""

import datetime
import os

from docx_style import (
    new_document,
    add_heading,
    add_code,
    add_table,
    add_note,
    add_warning,
    bullet,
    numbered,
    add_title_page,
)

doc = new_document()

add_title_page(
    doc,
    title="DR Backup Databricks",
    subtitle="Notes de version",
    version="Version 4.2",
    date_str=datetime.date.today().strftime("%d/%m/%Y"),
)


# ── 1. Résumé ─────────────────────────────────────────────────────────────────

add_heading(doc, "1. En résumé", 1)

doc.add_paragraph(
    "Cette version élargit le périmètre sauvegardé aux volumes et fonctions Unity Catalog, à "
    "l'ensemble du workspace (tous les dossiers, notebooks et fichiers) et aux pipelines ; elle "
    "corrige la cause de l'échec quotidien du premier run du backup et ajoute un rapport qui "
    "indique, objet par objet, ce qui est restaurable et par quel moyen."
)

add_note(
    doc,
    "aucun impact sur vos données ni sur le backup existant. La mise à jour touche les "
    "notebooks, le dossier lib/ et la définition du cluster du job quotidien. Aucun re-clonage, "
    "aucune perte de fenêtre de restauration.",
)

add_heading(doc, "1.1 Ce que vous allez constater", 2)

add_table(
    doc,
    ["Constat", "Explication"],
    [
        ["Le backup quotidien réussit dès la première tentative",
         "Le driver du cluster passe en Standard_E8s_v3 : sa mémoire ne sature plus (§2.1)"],
        ["Deux nouveaux fichiers dans uc_metadata/ : 05_volumes.sql, 06_functions.sql",
         "Définitions des volumes et des fonctions (§3.1)"],
        ["Des lignes [SKIP] Catalog … (FOREIGN_CATALOG) dans la sortie de 01_uc_metadata",
         "Catalogs fédérés et Delta Sharing désormais exclus par conception (§2.2)"],
        ["Plus de traces Java de plusieurs centaines de lignes dans les sorties",
         "Les avertissements n'affichent plus que le message utile (§2.3)"],
        ["Un nouveau périmètre uc_objects dans l'orchestrateur de restauration",
         "Restauration des volumes et fonctions (§3.2)"],
        ["Les notebooks de tous les dossiers (/Users, dossiers projet) sont sauvegardés, avec les fichiers",
         "Export complet du workspace (§3.4) ; /Repos reste exclu, son contenu est dans Git"],
        ["Un dossier workspace/ et un dossier pipelines/ dans chaque backup daté",
         "Nouveau format d'export et sauvegarde des pipelines (§3.4, §3.5)"],
        ["L'étape workspace_config peut durer plus longtemps",
         "Elle parcourt désormais tout le workspace ; les appels sont parallélisés"],
    ],
    col_widths=[6.5, 9.5],
)

doc.add_page_break()


# ── 2. Corrections ────────────────────────────────────────────────────────────

add_heading(doc, "2. Corrections", 1)

add_heading(doc, "2.1 Échec quotidien du premier run (mémoire du driver)", 2)

doc.add_paragraph(
    "Chaque nuit, la première tentative du job dr-backup-daily s'arrêtait avec l'erreur "
    "GC overhead limit exceeded après environ deux heures, et seule une seconde tentative "
    "terminait le backup. Les métriques montrent une mémoire Java du driver saturée (16,8 Go "
    "sur Standard_DS4_v2) peu après le début de la copie des tables."
)

doc.add_paragraph(
    "La mémoire du driver croît avec le nombre de tables traitées dans un même run, pas avec "
    "leur volume : la seconde tentative aboutissait parce qu'elle repartait d'un driver neuf avec "
    "seulement les tables restantes. Le driver passe en Standard_E8s_v3 (64 Go, environ 40 Go de "
    "mémoire Java) ; les workers restent inchangés."
)

add_table(
    doc,
    ["Élément", "Avant", "Après"],
    [
        ["Driver", "Standard_DS4_v2 (28 Go)", "Standard_E8s_v3 (64 Go)"],
        ["Workers", "4 × Standard_DS4_v2", "inchangé"],
        ["Coût du driver", "—", "quelques centimes de plus par heure"],
        ["Coût total attendu", "run en échec (~2 h de cluster complet) + seconde tentative", "un seul run"],
    ],
    col_widths=[3.5, 6, 6.5],
)

doc.add_paragraph(
    "La cause a été identifiée et corrigée dans le notebook de copie : chaque table traitée "
    "laissait dans la session un état Delta mis en cache, jamais libéré. Ce cache est désormais "
    "vidé toutes les 50 tables, sans effet sur le résultat de la copie. Mesure sur 200 tables :"
)

add_table(
    doc,
    ["200 tables traitées", "Avant", "Après"],
    [
        ["États Delta restés en cache", "200 (un par table)", "0"],
        ["Mémoire du driver", "en hausse continue (+620 Mo)", "stable (~500 Mo)"],
    ],
    col_widths=[6, 5, 5],
)

add_note(
    doc,
    "le driver Standard_E8s_v3 reste recommandé comme marge de sécurité : la correction supprime "
    "la croissance, la mémoire supplémentaire absorbe les pics.",
)

add_heading(doc, "2.2 Catalogs fédérés et Delta Sharing", 2)

doc.add_paragraph(
    "Les catalogs Lakehouse Federation (par exemple vers SQL Server) étaient traités comme des "
    "catalogs standards : le backup exportait un CREATE CATALOG simple qui, rejoué, aurait créé "
    "un catalog vide à la place du catalog fédéré, et interrogeait la base source à chaque "
    "exécution (erreur FAILED_JDBC si la connexion est cassée). Ces catalogs, ainsi que ceux de "
    "Delta Sharing, sont désormais exclus : leurs données ne sont pas dans Databricks et leur "
    "définition (connexion, partage) relève de l'infrastructure as code."
)

add_heading(doc, "2.3 Sorties de notebooks tronquées", 2)

doc.add_paragraph(
    "Un catalog inaccessible faisait afficher l'exception complète, avec une trace Java de "
    "plusieurs centaines de lignes. La sortie dépassait la limite de l'interface et la synthèse "
    "finale était perdue. Les avertissements n'affichent plus que le message, par exemple :"
)

add_code(doc, "[WARN] Volumes de db_demo non listables : [INSUFFICIENT_PERMISSIONS] "
              "User does not have USE CATALOG on Catalog 'db_demo'.")

add_heading(doc, "2.4 Permissions héritées exportées comme permissions directes", 2)

doc.add_paragraph(
    "La commande SHOW GRANTS sur un objet renvoie aussi les droits hérités de son catalog et "
    "de son schéma. L'export les réécrivait tous comme des droits posés sur l'objet lui-même : "
    "une restauration les aurait matérialisés en droits explicites, objet par objet. Un droit "
    "retiré plus tard au niveau du catalog serait alors resté actif sur chaque table. Seuls les "
    "droits posés sur l'objet lui-même sont désormais exportés ; sur l'environnement de test, "
    "le fichier des permissions est passé de 308 à 150 lignes."
)

add_warning(
    doc,
    "les fichiers 04_grants.sql produits par les versions précédentes contiennent ces droits "
    "hérités. Pour une restauration des permissions, utiliser de préférence un backup réalisé "
    "avec la version 4.2.",
)

add_heading(doc, "2.5 Robustesse de la copie des tables", 2)

bullet(doc, "Le point de reprise est écrit toutes les 50 tables ou toutes les 2 minutes, au lieu de "
            "toutes les 5 tables : environ 900 écritures de moins par run. Après une interruption, au "
            "plus 50 tables sont recopiées, sans conséquence (la copie est idempotente).")
bullet(doc, "Un accès concurrent entre threads au suivi de progression pouvait interrompre le run "
            "(« dictionary changed size during iteration ») ; il est protégé.")
bullet(doc, "Les messages d'erreur stockés dans le suivi et le manifest sont bornés, sans trace Java.")

add_heading(doc, "2.6 Fonctions Python", 2)

doc.add_paragraph(
    "Le corps d'une fonction Python gagnait une ligne vide au début et à la fin à chaque cycle "
    "de sauvegarde et de restauration. Il est désormais restauré à l'identique."
)

add_heading(doc, "2.7 Emplacement du dossier lib/", 2)

doc.add_paragraph(
    "Les jobs cherchaient les modules partagés dans un chemin fixe, la variable lib_path de "
    "databricks.yml, alors que databricks bundle deploy dépose lib/ dans le dossier du bundle. "
    "Si ce chemin pointait vers une copie ancienne ou absente, la sauvegarde des volumes et "
    "fonctions était sautée (simple avertissement) et les restaurations qui utilisent lib/ "
    "échouaient. Les jobs utilisent désormais le lib/ déployé par le bundle, toujours à jour "
    "avec les notebooks, et un notebook lancé à la main trouve le lib/ situé à côté de lui. "
    "La variable lib_path disparaît."
)

doc.add_page_break()


# ── 3. Ajouts ─────────────────────────────────────────────────────────────────

add_heading(doc, "3. Ajouts", 1)

add_heading(doc, "3.1 Volumes et fonctions Unity Catalog", 2)

add_table(
    doc,
    ["Objet", "Sauvegardé", "Restauré par"],
    [
        ["Volumes (managés et externes)", "Définition + permissions", "12_restore_uc_objects (scope uc_objects)"],
        ["Fichiers des volumes managés", "Copie incrémentale + index quotidien", "15_restore_volume_files (scope volume_files)"],
        ["Fonctions SQL, Python, fonctions table", "DDL complet + permissions", "12_restore_uc_objects (scope uc_objects)"],
    ],
    col_widths=[5, 5, 6],
)

doc.add_paragraph(
    "Le contenu des volumes managés est sauvegardé par la nouvelle étape 14_volume_files : seuls "
    "les fichiers nouveaux ou modifiés sont copiés chaque jour, et un index conserve l'état complet "
    "de chaque volume jour par jour. 15_restore_volume_files (scope volume_files) restaure un "
    "volume dans l'état d'une date donnée, sur la rétention quotidienne, plus un état par mois."
)

add_warning(
    doc,
    "les fichiers des volumes externes ne sont pas copiés : ils restent sur leur compte de "
    "stockage, dont la protection (soft delete, versioning, réplication) relève de sa "
    "configuration Azure.",
)

add_heading(doc, "3.2 Restauration", 2)

bullet(doc, "Nouveau scope uc_objects dans 10_restore_orchestrator, exécuté entre les tables et les permissions.")
bullet(doc, "11_restore_grants restaure aussi les permissions des volumes et des fonctions.")
bullet(doc, "scripts/restore_uc.py rejoue les fichiers dans le bon ordre (permissions en dernier), "
            "retente une fois les vues et fonctions dépendantes, et accepte --only-grants.")

add_heading(doc, "3.3 Diagnostic", 2)

bullet(doc, "diag_01_audit mesure le contenu des volumes managés (fichiers, taille, changements sur "
            "24 h) et liste les catalogs inaccessibles à l'identité qui l'exécute.")
bullet(doc, "Nouveau diag_03_backup_coverage : rapport HTML + Excel de tous les objets du metastore "
            "et du workspace, avec pour chacun le moyen de restauration (backup, as code, autre) "
            "ou son absence, les droits du compte de backup et le résultat du dernier backup.")

add_heading(doc, "3.4 Export complet du workspace", 2)

doc.add_paragraph(
    "Seuls les notebooks de /Shared, sur quatre niveaux de dossiers, étaient sauvegardés. Le "
    "workspace entier l'est désormais, sans limite de profondeur : notebooks dans leur format "
    "de stockage (source, ou .ipynb si le workspace stocke les notebooks en Jupyter), fichiers (.sh, .yml, .whl…) et dashboards. "
    "Le contenu est écrit par lots dans workspace/objects/, accompagné d'un inventaire "
    "(workspace/manifest.json). La restauration recrée chaque objet avec son type d'origine."
)

add_table(
    doc,
    ["Paramètre (05_workspace_config)", "Défaut", "Rôle"],
    [
        ["workspace_paths", "/", "Dossiers exportés"],
        ["exclude_paths", "/Repos", "Dossiers exclus ; le contenu des repos Git est dans Git"],
        ["export_pipelines", "true", "Sauvegarde des pipelines"],
        ["max_parallel", "16", "Appels API simultanés"],
    ],
    col_widths=[5, 2.5, 8.5],
)

add_warning(
    doc,
    "pour lire les dossiers personnels (/Users), l'identité du job doit être administrateur du "
    "workspace. Les dossiers qu'elle ne peut pas lire sont listés dans la sortie de l'étape "
    "workspace_config et ne sont pas sauvegardés.",
)

add_heading(doc, "3.5 Pipelines", 2)

doc.add_paragraph(
    "La définition complète de chaque pipeline et ses permissions sont sauvegardées dans "
    "pipelines/pipelines_all.json. Le nouveau notebook 13_restore_pipelines (scope pipelines de "
    "l'orchestrateur, exécuté après les notebooks) les recrée ; les pipelines déployés par un "
    "bundle sont ignorés par défaut et se redéploient avec le bundle."
)

add_note(
    doc,
    "les tables produites par un pipeline (vues matérialisées, streaming tables) ne sont pas "
    "copiées : elles sont recalculées par le pipeline, dont les sources doivent avoir conservé "
    "assez d'historique. Un pipeline continu démarre dès sa recréation.",
)

add_heading(doc, "3.6 Synchronisation des droits du compte de backup", 2)

doc.add_paragraph(
    "Le nouveau job dr-backup-grants-sync (en pause par défaut, à 00:30 UTC) compare chaque jour "
    "les droits du compte de backup à ceux requis, catalog par catalog, et sur l'External "
    "Location du backup. Les droits accordés sur un catalog couvrant tout son contenu futur, il "
    "rattrape surtout les nouveaux catalogs. En mode report (défaut), il échoue et notifie en cas "
    "d'écart ; en mode apply, il accorde les droits manquants. Il s'exécute avec l'identité qui "
    "déploie le bundle, qui doit être admin du metastore."
)

add_heading(doc, "3.7 Permissions des jobs", 2)

doc.add_paragraph(
    "Les permissions de chaque job sont sauvegardées (jobs/jobs_permissions.json) et réappliquées "
    "par 09_restore_jobs à la recréation du job. La propriété du job revient à l'identité qui "
    "restaure ; un transfert de propriété reste manuel."
)

doc.add_page_break()


# ── 4. Mise à jour ────────────────────────────────────────────────────────────

add_heading(doc, "4. Procédure de mise à jour", 1)

numbered(doc, "Extraire l'archive, en conservant le dossier lib/ : la plupart des notebooks en dépendent (seuls 07_restore et 11_restore_grants sont autonomes).")
numbered(doc, "Reporter dans databricks.yml vos valeurs (backup_root, host de chaque cible, notification_email, rétentions) et le nouveau bloc driver_node_type_id du job quotidien.")
numbered(doc, "Dans votre databricks.yml : supprimer la variable lib_path (bloc variables et cibles), "
              "remplacer ${var.lib_path} par ${workspace.file_path}/lib dans les paramètres des jobs, "
              "et ajouter lib_path: ${workspace.file_path}/lib aux paramètres du job dr-backup-monthly.")
numbered(doc, "Vérifier que le quota de vCPU de la famille ESv3 permet 8 vCPU supplémentaires dans la région, et qu'aucune politique de cluster n'interdit Standard_E8s_v3.")
numbered(doc, "databricks bundle validate --target <cible> — vérifier les lignes Host: et User:.")
numbered(doc, "databricks bundle deploy --target <cible>.")
numbered(doc, "Lancer une exécution manuelle de contrôle avant de réactiver la planification.")

add_warning(
    doc,
    "ne pas remplacer votre databricks.yml par celui de l'archive : vous perdriez vos chemins "
    "de stockage et votre adresse de notification. Reporter uniquement les blocs modifiés.",
)

add_note(
    doc,
    "lib/ est déployé par databricks bundle deploy avec les notebooks : aucun chemin à "
    "renseigner. Une copie de lib/ faite à la main dans le workspace n'est plus utilisée et "
    "peut être supprimée.",
)

add_heading(doc, "4.1 Vérification après mise à jour", 2)

bullet(doc, "Statut du run : réussi dès la première tentative.")
bullet(doc, "Metrics du driver : pic de JVM heap usage nettement sous 40 Go, pauses GC basses.")
bullet(doc, "Dossier uc_metadata/ du jour : présence de 05_volumes.sql et 06_functions.sql.")
bullet(doc, "Sortie de 01_uc_metadata : aucun avertissement « lib/uc_ddl.py introuvable ».")
bullet(doc, "Sortie de 01_uc_metadata : les lignes [SKIP] correspondent bien à vos catalogs fédérés.")

add_note(
    doc,
    "la transmission des graphiques mémoire du driver après le premier run nous permettra de "
    "confirmer la marge disponible sur votre volume de tables.",
)


# ── Écriture ──────────────────────────────────────────────────────────────────

out_dir = os.path.dirname(os.path.abspath(__file__))
out_path = os.path.join(out_dir, "DR_Backup_Notes_de_Version_v4.2.docx")
doc.save(out_path)
print(f"[OK] Notes de version generees : {out_path}")
