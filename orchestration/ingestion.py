"""Asset d'ingestion : Supabase (schéma public) -> BigQuery (local_bike_raw), via dlt.

Réutilise le script `ingestion/load_supabase_to_bq.py`. Le chargement est
monolithique (une passe dlt, chargement hybride merge/replace des 9 tables) : on le
modélise donc en un `@multi_asset` qui matérialise les 9 tables brutes en une passe.

Après chargement, **deux AssetChecks par table** réconcilient la volumétrie de bout
en bout — `PostgreSQL == dlt == BigQuery` :
  - `postgres_row_count_matches_dlt_load` : COUNT(*) source (Supabase) == lignes
    émises par dlt (étape *normalize*) -> détecte une perte à l'extraction ;
  - `bigquery_row_count_matches_dlt_load` : lignes dlt == row_count BigQuery
    (`local_bike_raw.__TABLES__`) -> détecte une perte au chargement.
Toute divergence est remontée dans l'UI Dagster avec le détail (delta, stratégie).

Les asset keys reprennent le nommage BigQuery `public_<table>` (via `raw_asset_key`)
afin de coïncider avec les sources dbt correspondantes (cf. LocalBikeDbtTranslator).

Astuce : pour un wiring « natif », dlt expose aussi `@dlt_assets` (paquet `dagster-dlt`).
On reste ici sur un `@multi_asset` maison pour garder une ingestion en une passe et
éviter toute connexion à la source au moment de l'import.
"""

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)

from ingestion.load_supabase_to_bq import (
    MERGE_PRIMARY_KEYS,
    SOURCE_SCHEMA,
    SOURCE_TABLES,
    bigquery_row_counts,
    loaded_row_counts,
    source_row_counts,
)
from ingestion.load_supabase_to_bq import main as run_ingestion
from orchestration.dbt import raw_asset_key

# Réconciliation de volumétrie bout-à-bout PostgreSQL == dlt == BigQuery, en 2 checks :
#   - SOURCE : COUNT(*) Supabase  == lignes émises par dlt  (perte à l'extraction)
#   - DEST   : lignes émises dlt   == row_count BigQuery     (perte au chargement)
# Severity WARN — une divergence remonte dans l'UI mais ne bloque pas le DAG dbt aval ;
# passer à AssetCheckSeverity.ERROR pour en faire un gate dur.
ROW_COUNT_CHECK_SOURCE = "postgres_row_count_matches_dlt_load"
ROW_COUNT_CHECK_DEST = "bigquery_row_count_matches_dlt_load"
ROW_COUNT_CHECK_SEVERITY = AssetCheckSeverity.WARN


def _load_strategy(table: str) -> str:
    """Libellé de la stratégie dlt appliquée à la table (cf. MERGE_PRIMARY_KEYS)."""
    pk = MERGE_PRIMARY_KEYS.get(table)
    return f"merge (upsert sur {pk})" if pk is not None else "replace (full refresh)"


# Un asset par table source ; la key reprend le nom BigQuery `public_<table>` afin
# de coïncider avec la source dbt correspondante (cf. LocalBikeDbtTranslator).
raw_table_specs = [
    AssetSpec(
        key=raw_asset_key(f"{SOURCE_SCHEMA}_{table}"),
        group_name="ingestion",
        description=(
            f"Copie brute de Supabase {SOURCE_SCHEMA}.{table} -> BigQuery local_bike_raw "
            f"(dlt, {_load_strategy(table)} + métadonnées _dlt_load_id)."
        ),
        kinds={"dlt", "bigquery"},
    )
    for table in SOURCE_TABLES
]

# Deux AssetChecks par table : intégrité à l'extraction (source==dlt) et au chargement
# (dlt==BigQuery).
raw_check_specs = [
    spec
    for table in SOURCE_TABLES
    for spec in (
        AssetCheckSpec(
            name=ROW_COUNT_CHECK_SOURCE,
            asset=raw_asset_key(f"{SOURCE_SCHEMA}_{table}"),
            description=(
                "Intégrité d'extraction : COUNT(*) PostgreSQL (Supabase) == "
                "nombre de lignes chargées par dlt pour cette table."
            ),
        ),
        AssetCheckSpec(
            name=ROW_COUNT_CHECK_DEST,
            asset=raw_asset_key(f"{SOURCE_SCHEMA}_{table}"),
            description=(
                "Intégrité de chargement : nombre de lignes dlt == row_count "
                "BigQuery (__TABLES__) pour cette table."
            ),
        ),
    )
]


def _row_count_check(
    asset_key: AssetKey,
    check_name: str,
    ref_rows: int | None,
    dlt_rows: int | None,
    ref_label: str,
) -> tuple[AssetCheckResult, bool]:
    """Construit un AssetCheckResult comparant `ref_rows` (source ou BigQuery) à dlt.

    `asset_key` est explicite : dans un multi_asset à plusieurs assets, Dagster ne
    peut pas déduire l'asset cible d'un AssetCheckResult (même imbriqué dans un
    MaterializeResult), il faut donc le préciser ici.
    """
    matched = ref_rows is not None and dlt_rows is not None and ref_rows == dlt_rows
    metadata: dict[str, Any] = {}
    if ref_rows is not None:
        metadata[ref_label] = ref_rows
    if dlt_rows is not None:
        metadata["dlt_rows_loaded"] = dlt_rows
    if ref_rows is not None and dlt_rows is not None:
        metadata["delta"] = ref_rows - dlt_rows
    result = AssetCheckResult(
        asset_key=asset_key,
        check_name=check_name,
        passed=matched,
        severity=ROW_COUNT_CHECK_SEVERITY,
        metadata=metadata,
    )
    return result, matched


@multi_asset(specs=raw_table_specs, check_specs=raw_check_specs)
def raw_supabase_tables(context: AssetExecutionContext):
    """Extrait les 9 tables Supabase via dlt, les charge dans `local_bike_raw`, puis
    réconcilie la volumétrie bout-à-bout (PostgreSQL == dlt == BigQuery) en AssetChecks."""
    context.log.info("Ingestion dlt Supabase -> BigQuery (local_bike_raw)…")
    pipeline = run_ingestion()  # charge les 9 tables en une passe (merge / replace)

    source = source_row_counts()        # {public_<table>: COUNT(*) PostgreSQL}
    loaded = loaded_row_counts(pipeline)  # {public_<table>: lignes émises par dlt}
    warehouse = bigquery_row_counts()   # {public_<table>: row_count BigQuery (__TABLES__)}

    for table in SOURCE_TABLES:
        bq_table = f"{SOURCE_SCHEMA}_{table}"
        asset_key = raw_asset_key(bq_table)
        src_rows = source.get(bq_table)
        dlt_rows = loaded.get(bq_table)
        bq_rows = warehouse.get(bq_table)

        src_check, src_ok = _row_count_check(
            asset_key, ROW_COUNT_CHECK_SOURCE, src_rows, dlt_rows, "postgres_row_count"
        )
        dest_check, dest_ok = _row_count_check(
            asset_key, ROW_COUNT_CHECK_DEST, bq_rows, dlt_rows, "bigquery_row_count"
        )

        context.log.info(
            "%s : postgres=%s, dlt=%s, bigquery=%s -> extraction %s | chargement %s",
            bq_table, src_rows, dlt_rows, bq_rows,
            "OK" if src_ok else "DIVERGENCE",
            "OK" if dest_ok else "DIVERGENCE",
        )

        yield MaterializeResult(
            asset_key=asset_key,
            metadata={
                "load_strategy": _load_strategy(table),
                "postgres_row_count": src_rows,
                "dlt_rows_loaded": dlt_rows,
                "bigquery_row_count": bq_rows,
            },
            check_results=[src_check, dest_check],
        )
