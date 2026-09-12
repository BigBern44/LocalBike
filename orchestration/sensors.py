"""Sensor Dagster : déclenchement **événementiel** de la branche stocks.

> ⚠️ **Hors périmètre du pipeline v1.** Ce sensor est livré **désactivé**
> (`DefaultSensorStatus.STOPPED`) : le fonctionnement nominal reste le schedule
> quotidien `daily_refresh`. Il est là pour démontrer le second mode de
> déclenchement d'un orchestrateur et prépare une v2 (cf. README).

Complément du schedule (*temps*) : ici le déclencheur est un **événement**.

    UPDATE public.stocks
      -> trigger pg_notify -> listener Python (`make stream`)
      -> local_bike_raw.stock_events_stream          <- le sensor surveille CETTE table
      -> RunRequest -> job `stock_refresh`
      -> ingestion dlt de `stocks` seule + stg_stocks -> fct_stocks -> rpt_stocks

Pourquoi ne pas se contenter de rejouer les modèles dbt ? Le flux temps réel
n'écrit **que** dans `stock_events_stream` ; `local_bike_raw.public_stocks` reste
figé jusqu'au batch suivant. Rebâtir `stg_stocks` sans ré-ingérer relirait donc
des quantités périmées : le job doit repartir de l'ingestion.

La ré-ingestion est **ciblée sur la seule table `stocks`** : l'asset d'ingestion
déclare `can_subset=True` et répercute la sélection Dagster sur dlt
(`run_ingestion(only=["stocks"])`), au lieu de rejouer les 9 tables.

Le run reste donc minimal : 1 table extraite + 3 modèles dbt, au lieu des 9 tables
et 32 modèles du job complet.

Activation : `make dagster` -> onglet *Automation* -> `stock_events_sensor` sur **on**
(le daemon doit tourner).
"""

from __future__ import annotations

from datetime import datetime

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from ingestion.load_supabase_to_bq import (
    SOURCE_SCHEMA,
    bigquery_client,
    env,
)
from ingestion.stream_stock_alerts import EVENTS_TABLE
from orchestration.dbt import raw_asset_key

# Nom du job déclenché — défini dans definitions.py, référencé ici par son nom
# (et non par l'objet) pour éviter un import circulaire sensors <-> definitions.
STOCK_REFRESH_JOB_NAME = "stock_refresh"

# Tête de la branche stocks. Le translator dagster-dbt préfixe les asset keys par le
# sous-dossier du modèle : models/staging/stg_stocks.sql -> ["staging", "stg_stocks"].
STOCKS_BRANCH_ROOT = AssetKey(["staging", "stg_stocks"])

# Sélection ciblée : la seule table brute `stocks` (l'asset d'ingestion est subsettable)
# + la seule branche stocks du DAG dbt (stg_stocks -> fct_stocks -> rpt_stocks).
STOCK_REFRESH_SELECTION = AssetSelection.assets(
    raw_asset_key(f"{SOURCE_SCHEMA}_stocks")
) | AssetSelection.assets(STOCKS_BRANCH_ROOT).downstream()

# Fréquence d'évaluation : plancher anti-rafale. Une salve de mouvements de stock
# ne peut donc pas déclencher plus d'un run par minute (valeur confortable pour la
# démo ; en production, l'aligner sur la fraîcheur réellement exigée : 5-15 min).
MINIMUM_INTERVAL_SECONDS = 60

# Fenêtre de lecture des événements. `stock_events_stream` est partitionnée sur
# `event_ts` : ce filtre élague les partitions et borne le coût de l'évaluation,
# qui tourne en boucle. Au-delà de cette fenêtre, le batch quotidien fait foi.
LOOKBACK_DAYS = 7

# Un seul aller-retour BigQuery par tick : la borne de fraîcheur et le nombre
# d'événements postérieurs au curseur.
FRESHNESS_QUERY = """
select
    max(ingested_at)                                  as last_ingested_at,
    countif(@cursor is null or ingested_at > @cursor) as new_events
from `{table}`
where event_ts >= timestamp_sub(current_timestamp(), interval {lookback} day)
"""


def events_table_id(client: bigquery.Client) -> str:
    """Identifiant complet de la table d'événements alimentée par le flux temps réel."""
    dataset = env("GCP_DATASET_RAW", "local_bike_raw")
    return f"{client.project}.{dataset}.{EVENTS_TABLE}"


@sensor(
    name="stock_events_sensor",
    job_name=STOCK_REFRESH_JOB_NAME,
    minimum_interval_seconds=MINIMUM_INTERVAL_SECONDS,
    # Livré à l'arrêt : le pipeline nominal v1 reste le schedule quotidien.
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Déclenche le rafraîchissement ciblé de la branche stocks dès que le flux "
        "temps réel a inséré de nouveaux événements (extension v2, désactivée par défaut)."
    ),
)
def stock_events_sensor(context: SensorEvaluationContext):
    """Surveille la fraîcheur de `stock_events_stream` et déclenche `stock_refresh`.

    Le curseur Dagster mémorise le `ingested_at` du dernier événement vu : seuls
    les événements postérieurs déclenchent un run, ce qui rend le sensor idempotent
    d'un tick à l'autre (et à travers un redémarrage du daemon).
    """
    client = bigquery_client()
    table_id = events_table_id(client)
    cursor = datetime.fromisoformat(context.cursor) if context.cursor else None

    query = FRESHNESS_QUERY.format(table=table_id, lookback=LOOKBACK_DAYS)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cursor", "TIMESTAMP", cursor)]
    )
    try:
        row = next(iter(client.query(query, job_config=job_config).result()))
    except NotFound:
        # Le flux temps réel n'a encore jamais tourné : la table est créée par
        # `ensure_events_table()` au premier `make stream`.
        return SkipReason(f"Table {table_id} absente — lancer `make stream` au préalable.")

    last_ingested_at = row.last_ingested_at
    if last_ingested_at is None:
        return SkipReason(
            f"Aucun événement de stock depuis {LOOKBACK_DAYS} jours dans {EVENTS_TABLE}."
        )

    if cursor is None:
        # Premier tick : on s'aligne sur l'existant sans rejouer l'historique
        # (sinon le simple fait d'activer le sensor déclencherait un run).
        context.update_cursor(last_ingested_at.isoformat())
        return SkipReason(
            f"Amorçage du curseur sur {last_ingested_at:%Y-%m-%d %H:%M:%S} — "
            "le prochain mouvement de stock déclenchera un run."
        )

    if row.new_events == 0:
        return SkipReason(
            f"Aucun nouvel événement depuis {cursor:%Y-%m-%d %H:%M:%S}."
        )

    context.log.info(
        "%d nouvel(s) événement(s) de stock -> déclenchement de %s",
        row.new_events, STOCK_REFRESH_JOB_NAME,
    )
    context.update_cursor(last_ingested_at.isoformat())

    # `run_key` = borne de fraîcheur : Dagster déduplique, un même lot d'événements
    # ne peut pas produire deux runs (rejeu du tick, redémarrage du daemon).
    return RunRequest(
        run_key=last_ingested_at.isoformat(),
        tags={
            "trigger": "stock_events_stream",
            "new_events": str(row.new_events),
        },
    )
