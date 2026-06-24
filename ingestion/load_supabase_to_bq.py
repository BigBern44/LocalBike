"""Ingestion EL : Supabase (PostgreSQL) -> BigQuery (dataset RAW).

Extrait les 9 tables source des schémas `sales` et `production`, et les charge
**brutes (1:1)** dans le dataset `local_bike_raw` de BigQuery.

Stack :
  - Lecture  : Polars via ADBC (driver PostgreSQL = libpq -> gère le sslmode=require de Supabase)
  - Écriture : Polars -> Parquet (buffer mémoire) -> BigQuery load job

Idempotent : chaque table est chargée en `WRITE_TRUNCATE` (full refresh).

Usage :
    python ingestion/load_supabase_to_bq.py            # toutes les tables
    python ingestion/load_supabase_to_bq.py customers  # seulement sales.customers
"""

from __future__ import annotations

import io
import logging
import os
import sys
from urllib.parse import quote

import adbc_driver_postgresql.dbapi as pg_dbapi
import polars as pl
from dotenv import load_dotenv
from google.cloud import bigquery

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingestion")

# Tables source : (schéma postgres, table). Le nom BigQuery sera "<schema>_<table>".
SOURCE_TABLES: list[tuple[str, str]] = [
    ("public", "customers"),
    ("public", "orders"),
    ("public", "order_items"),
    ("public", "staffs"),
    ("public", "stores"),
    ("public", "categories"),
    ("public", "products"),
    ("public", "stocks"),
    ("public", "brands"),
]


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    """Lit une variable d'environnement, avec erreur explicite si manquante."""
    value = os.getenv(name, default)
    if required and not value:
        log.error("Variable d'environnement manquante : %s (voir .env.example)", name)
        sys.exit(1)
    return value or ""


def build_source_uri() -> str:
    """Construit l'URI de connexion Supabase (libpq), mot de passe URL-encodé."""
    user = quote(env("SUPABASE_USER", required=True))
    password = quote(env("SUPABASE_PASSWORD", required=True))
    host = env("SUPABASE_HOST", required=True)
    port = env("SUPABASE_PORT", "5432")
    db = env("SUPABASE_DB", "postgres")
    sslmode = env("SUPABASE_SSLMODE", "require")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode={sslmode}"


# --------------------------------------------------------------------------- #
# Extract / Load
# --------------------------------------------------------------------------- #

def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    """Crée le dataset RAW s'il n'existe pas déjà."""
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    log.info("Dataset prêt : %s.%s (%s)", client.project, dataset_id, location)


def extract(conn, schema: str, table: str) -> pl.DataFrame:
    """Lit une table source complète dans un DataFrame Polars."""
    query = f'SELECT * FROM "{schema}"."{table}"'
    df = pl.read_database(query=query, connection=conn)
    log.info("  extract %-25s -> %d lignes, %d colonnes", f"{schema}.{table}", df.height, df.width)
    return df


def load(client: bigquery.Client, df: pl.DataFrame, dataset_id: str, bq_table: str) -> None:
    """Charge un DataFrame Polars dans BigQuery via Parquet (full refresh)."""
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    table_id = f"{client.project}.{dataset_id}.{bq_table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(buffer, table_id, job_config=job_config)
    job.result()  # attend la fin du job
    log.info("  load    %-25s -> %s", bq_table, table_id)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(only: list[str] | None = None) -> None:
    load_dotenv()

    project = env("GCP_PROJECT_ID", required=True)
    dataset_id = env("GCP_DATASET_RAW", "local_bike_raw")
    location = env("GCP_LOCATION", "EU")

    if not env("GOOGLE_APPLICATION_CREDENTIALS"):
        log.warning("GOOGLE_APPLICATION_CREDENTIALS non défini : l'auth GCP risque d'échouer.")

    tables = SOURCE_TABLES
    if only:
        wanted = {t.lower() for t in only}
        tables = [(s, t) for (s, t) in SOURCE_TABLES if t in wanted]
        if not tables:
            log.error("Aucune table connue parmi : %s", ", ".join(only))
            sys.exit(1)

    client = bigquery.Client(project=project)
    ensure_dataset(client, dataset_id, location)

    uri = build_source_uri()
    log.info("Connexion à Supabase...")
    total = 0
    with pg_dbapi.connect(uri) as conn:
        for schema, table in tables:
            log.info("Table %s.%s", schema, table)
            df = extract(conn, schema, table)
            load(client, df, dataset_id, bq_table=f"{schema}_{table}")
            total += df.height

    log.info("Terminé : %d tables, %d lignes chargées dans %s.%s",
             len(tables), total, project, dataset_id)


if __name__ == "__main__":
    main(only=sys.argv[1:] or None)
