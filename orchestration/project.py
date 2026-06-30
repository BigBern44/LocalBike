"""Bootstrap commun : chargement de `.env` et déclaration du projet dbt.

Importé en premier par les autres modules d'orchestration, ce module garantit que
l'environnement (`GCP_PROJECT_ID`, credentials, etc.) est chargé AVANT que dbt ou
l'ingestion ne s'exécutent — le profil dbt (`profiles.yml`) lit ces variables via
`env_var()`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dagster_dbt import DbtProject
from dotenv import load_dotenv

# Racine du dépôt = dossier qui contient dbt_project.yml et profiles.yml.
REPO_ROOT = Path(__file__).resolve().parent.parent

# Rends `dbt` (et les autres console scripts du venv) découvrable par les
# sous-process lancés par Dagster. `make dagster` appelle `.venv/bin/dagster`
# par chemin absolu sans activer le venv : le code server hérite donc d'un PATH
# sans `.venv/bin`. Or `DbtProject.prepare_if_dev()` instancie en interne un
# `DbtCliResource` qui résout l'exécutable `dbt` via le PATH -> échec si absent.
VENV_BIN = str(Path(sys.executable).parent)
if VENV_BIN not in os.environ.get("PATH", "").split(os.pathsep):
    os.environ["PATH"] = VENV_BIN + os.pathsep + os.environ.get("PATH", "")

# Charge .env dans l'environnement du process Dagster (hérité par le sous-process dbt).
load_dotenv(REPO_ROOT / ".env")
# dbt cherche profiles.yml ici (équivalent de `--profiles-dir .`).
os.environ.setdefault("DBT_PROFILES_DIR", str(REPO_ROOT))

# Projet dbt : en mode `dagster dev`, régénère le manifest si besoin (`dbt parse`).
dbt_project = DbtProject(project_dir=REPO_ROOT)
dbt_project.prepare_if_dev()
