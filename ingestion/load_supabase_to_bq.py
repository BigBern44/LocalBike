"""Ingestion EL : Supabase (PostgreSQL) -> BigQuery (dataset RAW), via dlt.

Extrait les 9 tables du schéma `public` de Supabase et les charge **brutes**
dans le dataset `local_bike_raw` de BigQuery. L'Extract-Load est entièrement
délégué à **dlt** (data load tool) :

  - source `sql_database` (SQLAlchemy/psycopg2, SSL imposé par Supabase) ;
  - destination `bigquery` (authentifiée via la clé service account du `.env`) ;
  - chargement hybride par table (cf. `MERGE_PRIMARY_KEYS`) :
      * `merge` (upsert sur clé primaire) pour les tables transactionnelles
        (customers, orders, order_items) -> capte inserts + updates, sans doublon ;
      * `replace` (full refresh) pour les référentiels / snapshot (les 6 autres) ;
    les deux restent idempotents à chaque run ;
  - inférence + évolution de schéma gérées par dlt (types Postgres préservés) ;
  - métadonnées de chargement (`_dlt_load_id` / `_dlt_id` + tables `_dlt_loads`)
    pour la traçabilité / freshness.

Les tables de destination reprennent le nommage `public_<table>` (1:1 avec la
source), consommé tel quel par les sources dbt (`local_bike_raw.public_*`).

Usage :
    python ingestion/load_supabase_to_bq.py            # les 9 tables
    python ingestion/load_supabase_to_bq.py customers  # une seule table
"""

from __future__ import annotations

import json
import logging
import os
import sys
from urllib.parse import quote

import dlt
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingestion")

SOURCE_SCHEMA = "public"
# Les 9 tables BikeStores (le nom BigQuery sera "public_<table>").
SOURCE_TABLES: list[str] = [
    "customers",
    "orders",
    "order_items",
    "staffs",
    "stores",
    "categories",
    "products",
    "stocks",
    "brands",
]

# Tables transactionnelles chargées en `merge` (upsert sur la clé primaire) :
# l'incrémental capte les nouvelles lignes ET les mises à jour, sans doublon.
# Les tables absentes de ce mapping (référentiels + snapshot stocks) restent en
# `replace` (full refresh). NB : faute de colonne `updated_at` dans la source, on
# ré-extrait l'intégralité de la table à chaque run (volumétrie ~9k lignes => coût
# négligeable) ; le `merge` garantit l'idempotence côté destination.
MERGE_PRIMARY_KEYS: dict[str, str | list[str]] = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": ["order_id", "item_id"],
}


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    """Lit une variable d'environnement, avec erreur explicite si manquante."""
    value = os.getenv(name, default)
    if required and not value:
        log.error("Variable d'environnement manquante : %s (voir .env.example)", name)
        sys.exit(1)
    return value or ""


def build_source_uri() -> str:
    """URI SQLAlchemy/psycopg2 vers Supabase (SSL imposé, mot de passe URL-encodé)."""
    user = quote(env("SUPABASE_USER", required=True))
    password = quote(env("SUPABASE_PASSWORD", required=True))
    host = env("SUPABASE_HOST", required=True)
    port = env("SUPABASE_PORT", "5432")
    db = env("SUPABASE_DB", "postgres")
    sslmode = env("SUPABASE_SSLMODE", "require")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}?sslmode={sslmode}"


# --------------------------------------------------------------------------- #
# Source / Destination dlt
# --------------------------------------------------------------------------- #

def build_source(only: list[str] | None = None):
    """Source dlt `sql_database` sur le schéma public, renommée en `public_<table>`."""
    source = sql_database(
        credentials=build_source_uri(),
        schema=SOURCE_SCHEMA,
        table_names=SOURCE_TABLES,
    )
    # Par table : nom de destination `public_<table>` + write_disposition adaptée
    # (merge sur PK pour les transactionnelles, replace pour les référentiels).
    for table in SOURCE_TABLES:
        primary_key = MERGE_PRIMARY_KEYS.get(table)
        if primary_key is not None:
            source.resources[table].apply_hints(
                table_name=f"{SOURCE_SCHEMA}_{table}",
                write_disposition="merge",
                primary_key=primary_key,
            )
        else:
            source.resources[table].apply_hints(
                table_name=f"{SOURCE_SCHEMA}_{table}",
                write_disposition="replace",
            )

    if only:
        wanted = [t for t in SOURCE_TABLES if t in {o.lower() for o in only}]
        if not wanted:
            log.error("Aucune table connue parmi : %s", ", ".join(only))
            sys.exit(1)
        source = source.with_resources(*wanted)
    return source


def build_destination(location: str):
    """Destination BigQuery dlt, authentifiée via la clé service account du `.env`."""
    sa_path = env("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        with open(sa_path, encoding="utf-8") as fh:
            credentials = json.load(fh)
        return dlt.destinations.bigquery(credentials=credentials, location=location)
    log.warning(
        "GOOGLE_APPLICATION_CREDENTIALS absent/introuvable : "
        "tentative via les credentials GCP par défaut (ADC)."
    )
    return dlt.destinations.bigquery(location=location)


# --------------------------------------------------------------------------- #
# Contrôle de volumétrie (réconciliation source -> dlt -> BigQuery)
# --------------------------------------------------------------------------- #

def source_row_counts(only: list[str] | None = None) -> dict[str, int]:
    """Comptage exact des lignes côté Supabase/PostgreSQL (schéma `public`), par table.

    Renvoie `{public_<table>: count}` (même nommage que la destination) en un seul
    aller-retour (`UNION ALL` de `COUNT(*)`). SSL imposé via `build_source_uri`.

    NB : ce comptage suppose une **extraction full-refresh** (chaque run ré-extrait
    toute la table). Sous incrémental réel, il ne correspondrait plus aux lignes d'un
    run dlt et la réconciliation source/dlt devrait être revue.
    """
    from sqlalchemy import create_engine, text

    tables = (
        SOURCE_TABLES
        if not only
        else [t for t in SOURCE_TABLES if t in {o.lower() for o in only}]
    )
    if not tables:
        return {}

    # Identifiants issus de notre constante SOURCE_TABLES (pas d'entrée externe).
    union = " UNION ALL ".join(
        f"SELECT '{t}' AS table_name, COUNT(*) AS n FROM \"{SOURCE_SCHEMA}\".\"{t}\""
        for t in tables
    )
    engine = create_engine(build_source_uri())
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(union)).all()
    finally:
        engine.dispose()
    return {f"{SOURCE_SCHEMA}_{name}": int(n) for name, n in rows}


def loaded_row_counts(pipeline: "dlt.Pipeline") -> dict[str, int]:
    """Lignes émises par dlt à la dernière passe, par table de destination `public_*`.

    Source : métadonnées de l'étape *normalize* du dernier run
    (`last_trace.last_normalize_info.row_counts`). On ne garde que les 9 tables
    métier (`public_<table>`), en ignorant les tables internes dlt (`_dlt_*`).
    """
    trace = pipeline.last_trace
    normalize_info = getattr(trace, "last_normalize_info", None) if trace else None
    counts = getattr(normalize_info, "row_counts", None) or {}
    return {
        name: int(n)
        for name, n in counts.items()
        if name.startswith(f"{SOURCE_SCHEMA}_")
    }


def _bigquery_client():
    """Client BigQuery authentifié via la même clé service account que la destination dlt."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    project = env("GCP_PROJECT_ID", required=True)
    location = env("GCP_LOCATION", "EU")
    sa_path = env("GOOGLE_APPLICATION_CREDENTIALS")
    if sa_path and os.path.exists(sa_path):
        credentials = service_account.Credentials.from_service_account_file(sa_path)
        return bigquery.Client(project=project, credentials=credentials, location=location)
    log.warning("GOOGLE_APPLICATION_CREDENTIALS absent : client BigQuery via ADC.")
    return bigquery.Client(project=project, location=location)


def bigquery_row_counts(dataset: str | None = None) -> dict[str, int]:
    """Volumétrie réelle côté BigQuery, lue dans les métadonnées `<dataset>.__TABLES__`.

    Renvoie `{table_id: row_count}` pour toutes les tables du dataset RAW : c'est la
    « source de vérité » après chargement, comparée aux lignes émises par dlt.
    """
    project = env("GCP_PROJECT_ID", required=True)
    dataset = dataset or env("GCP_DATASET_RAW", "local_bike_raw")
    client = _bigquery_client()
    query = f"SELECT table_id, row_count FROM `{project}.{dataset}.__TABLES__`"
    return {row.table_id: int(row.row_count) for row in client.query(query).result()}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(only: list[str] | None = None) -> "dlt.Pipeline":
    """Charge les tables Supabase vers BigQuery (`local_bike_raw`) via dlt.

    Retourne le `dlt.Pipeline` exécuté : son `last_trace` porte les métriques de
    chargement (cf. `loaded_row_counts`), exploitées par le contrôle de volumétrie.
    """
    load_dotenv()

    env("GCP_PROJECT_ID", required=True)  # validation explicite (utilisé par les credentials)
    dataset = env("GCP_DATASET_RAW", "local_bike_raw")
    location = env("GCP_LOCATION", "EU")

    source = build_source(only)
    pipeline = dlt.pipeline(
        pipeline_name="local_bike",
        destination=build_destination(location),
        dataset_name=dataset,
    )

    log.info("Ingestion dlt : Supabase(%s) -> BigQuery %s …", SOURCE_SCHEMA, dataset)
    # write_disposition fixée par table via apply_hints (merge / replace), pas en global.
    info = pipeline.run(source)
    log.info("Terminé : %s", info)
    return pipeline


if __name__ == "__main__":
    main(only=sys.argv[1:] or None)
