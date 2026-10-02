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
    "Cette version élargit le périmètre sauvegardé aux volumes et aux fonctions Unity Catalog, "
    "corrige l'échec quotidien du premier run du backup et ajoute un rapport qui indique, objet "
    "par objet, ce qui est restaurable et par quel moyen."
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

add_note(
    doc,
    "ce dimensionnement est un ajustement de capacité. Une optimisation du notebook de copie, "
    "pour que la mémoire du driver ne dépende plus du nombre de tables, est prévue dans une "
    "version ultérieure.",
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

doc.add_page_break()


# ── 3. Ajouts ─────────────────────────────────────────────────────────────────

add_heading(doc, "3. Ajouts", 1)

add_heading(doc, "3.1 Volumes et fonctions Unity Catalog", 2)

add_table(
    doc,
    ["Objet", "Sauvegardé", "Restauré par"],
    [
        ["Volumes (managés et externes)", "Définition + permissions", "12_restore_uc_objects (scope uc_objects)"],
        ["Fonctions SQL, Python, fonctions table", "DDL complet + permissions", "12_restore_uc_objects (scope uc_objects)"],
    ],
    col_widths=[5, 5, 6],
)

add_warning(
    doc,
    "le contenu des volumes managés (fichiers) n'est pas sauvegardé : un volume managé est "
    "recréé vide. Les fichiers des volumes externes restent sur leur compte de stockage, dont la "
    "protection (soft delete, versioning, réplication) relève de sa configuration Azure.",
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

doc.add_page_break()


# ── 4. Mise à jour ────────────────────────────────────────────────────────────

add_heading(doc, "4. Procédure de mise à jour", 1)

numbered(doc, "Extraire l'archive, en conservant le dossier lib/ : 01_uc_metadata et 12_restore_uc_objects en dépendent.")
numbered(doc, "Reporter dans databricks.yml vos valeurs (backup_root, host de chaque cible, notification_email, rétentions) et le nouveau bloc driver_node_type_id du job quotidien.")
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
    "si lib/ n'est pas redéployé, le backup ne s'arrête pas : seule la sauvegarde des volumes et "
    "fonctions est sautée, avec un avertissement dans la sortie de 01_uc_metadata.",
)

add_heading(doc, "4.1 Vérification après mise à jour", 2)

bullet(doc, "Statut du run : réussi dès la première tentative.")
bullet(doc, "Metrics du driver : pic de JVM heap usage nettement sous 40 Go, pauses GC basses.")
bullet(doc, "Dossier uc_metadata/ du jour : présence de 05_volumes.sql et 06_functions.sql.")
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
