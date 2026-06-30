"""Câblage Dagster : assets + ressource dbt + job + schedule.

Point d'entrée chargé par `dagster dev -m orchestration` (via `orchestration/__init__.py`).
"""

from __future__ import annotations

import sys
from pathlib import Path

from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource

from orchestration.dbt import local_bike_dbt_assets
from orchestration.ingestion import raw_supabase_tables
from orchestration.project import dbt_project

# Exécutable dbt du venv courant (indépendant du PATH / de l'activation du venv).
DBT_EXECUTABLE = str(Path(sys.executable).parent / "dbt")

# Job global : ingestion dlt (Supabase -> local_bike_raw) -> tout le DAG dbt
# (run + tests via `dbt build`).
local_bike_pipeline = define_asset_job(
    name="local_bike_pipeline",
    selection=AssetSelection.all(),
)

# Rafraîchissement quotidien à 05:00 (heure de Paris).
daily_refresh = ScheduleDefinition(
    name="daily_refresh",
    job=local_bike_pipeline,
    cron_schedule="0 5 * * *",
    execution_timezone="Europe/Paris",
)

defs = Definitions(
    assets=[raw_supabase_tables, local_bike_dbt_assets],
    jobs=[local_bike_pipeline],
    schedules=[daily_refresh],
    resources={
        # profiles.yml localisé via DBT_PROFILES_DIR (cf. orchestration/project.py).
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE),
    },
)
