"""Assets dbt : chaque modèle (`stg_*`, `int_*`, `dim_*`, `fct_*`) devient un asset.

Un translator custom mappe les *sources* dbt (`local_bike_raw.public_*`) sur les
asset keys produits par l'ingestion, de sorte que le DAG soit continu :
ingestion -> sources -> staging -> intermediate -> marts.
"""

from collections.abc import Mapping
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from orchestration.project import dbt_project

# Dataset BigQuery brut + 1er segment des asset keys de la couche d'ingestion.
RAW_DATASET = "local_bike_raw"


def raw_asset_key(table: str) -> AssetKey:
    """Asset key d'une table brute (ex. 'public_customers' -> local_bike_raw/public_customers)."""
    return AssetKey([RAW_DATASET, table])


class LocalBikeDbtTranslator(DagsterDbtTranslator):
    """Aligne les sources dbt sur les assets matérialisés par l'ingestion."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return raw_asset_key(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LocalBikeDbtTranslator(),
)
def local_bike_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # `dbt build` = run + test : les modèles ET leurs tests sont exécutés ensemble.
    yield from dbt.cli(["build"], context=context).stream()
