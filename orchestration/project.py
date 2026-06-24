"""Bootstrap commun : chargement de `.env` et déclaration du projet dbt.

Importé en premier par les autres modules d'orchestration, ce module garantit que
l'environnement (`GCP_PROJECT_ID`, credentials, etc.) est chargé AVANT que dbt ou
l'ingestion ne s'exécutent — le profil dbt (`profiles.yml`) lit ces variables via
`env_var()`.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster_dbt import DbtProject
from dotenv import load_dotenv

# Racine du dépôt = dossier qui contient dbt_project.yml et profiles.yml.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Charge .env dans l'environnement du process Dagster (hérité par le sous-process dbt).
load_dotenv(REPO_ROOT / ".env")
# dbt cherche profiles.yml ici (équivalent de `--profiles-dir .`).
os.environ.setdefault("DBT_PROFILES_DIR", str(REPO_ROOT))

# Projet dbt : en mode `dagster dev`, régénère le manifest si besoin (`dbt parse`).
dbt_project = DbtProject(project_dir=REPO_ROOT)
dbt_project.prepare_if_dev()
