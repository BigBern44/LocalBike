"""Câblage Dagster : assets + ressource dbt + jobs + schedule + sensor.

Deux modes de déclenchement cohabitent :
  - `daily_refresh` (schedule, *temps*)      -> `local_bike_pipeline` : tout le DAG ;
  - `stock_events_sensor` (sensor, *événement*, désactivé par défaut)
                                             -> `stock_refresh` : branche stocks seule.

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
from orchestration.sensors import (
    STOCK_REFRESH_JOB_NAME,
    STOCK_REFRESH_SELECTION,
    stock_events_sensor,
)

# Exécutable dbt du venv courant (indépendant du PATH / de l'activation du venv).
DBT_EXECUTABLE = str(Path(sys.executable).parent / "dbt")

# Job global : ingestion dlt (Supabase -> local_bike_raw) -> tout le DAG dbt
# (run + tests via `dbt build`).
local_bike_pipeline = define_asset_job(
    name="local_bike_pipeline",
    selection=AssetSelection.all(),
)

# Job ciblé déclenché par le sensor événementiel : ingestion (1 passe dlt) puis
# la seule branche stocks du DAG dbt (stg_stocks -> fct_stocks -> rpt_stocks).
# Hors périmètre v1 : rien ne l'exécute tant que `stock_events_sensor` est à l'arrêt.
stock_refresh = define_asset_job(
    name=STOCK_REFRESH_JOB_NAME,
    selection=STOCK_REFRESH_SELECTION,
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
    jobs=[local_bike_pipeline, stock_refresh],
    schedules=[daily_refresh],
    # Déclenchement événementiel — livré désactivé (cf. orchestration/sensors.py).
    sensors=[stock_events_sensor],
    resources={
        # profiles.yml localisé via DBT_PROFILES_DIR (cf. orchestration/project.py).
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE),
    },
)
