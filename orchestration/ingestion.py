"""Asset d'ingestion : Supabase (schéma public) -> BigQuery (local_bike_raw).

Réutilise le script existant `ingestion/load_supabase_to_bq.py`. Le chargement est
monolithique (une connexion Supabase, full refresh des 9 tables) : on le modélise
donc en un `@multi_asset` qui matérialise les 9 tables brutes en une passe.
"""

from dagster import AssetExecutionContext, AssetSpec, MaterializeResult, multi_asset

from ingestion.load_supabase_to_bq import SOURCE_TABLES
from ingestion.load_supabase_to_bq import main as run_ingestion
from orchestration.dbt import raw_asset_key

# Un asset par table source ; la key reprend le nom BigQuery `public_<table>` afin
# de coïncider avec la source dbt correspondante (cf. LocalBikeDbtTranslator).
raw_table_specs = [
    AssetSpec(
        key=raw_asset_key(f"{schema}_{table}"),
        group_name="ingestion",
        description=f"Copie brute 1:1 de Supabase {schema}.{table} -> BigQuery (full refresh).",
        kinds={"python", "bigquery"},
    )
    for schema, table in SOURCE_TABLES
]


@multi_asset(specs=raw_table_specs)
def raw_supabase_tables(context: AssetExecutionContext):
    """Extrait les 9 tables Supabase et les charge dans `local_bike_raw` (WRITE_TRUNCATE)."""
    context.log.info("Ingestion Supabase -> BigQuery (local_bike_raw)…")
    run_ingestion()  # charge les 9 tables en une seule passe
    for schema, table in SOURCE_TABLES:
        yield MaterializeResult(asset_key=raw_asset_key(f"{schema}_{table}"))
