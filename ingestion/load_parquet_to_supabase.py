"""Chargement des exports Parquet (`data_raw/`) vers Supabase (PostgreSQL).

Reconstruit la base source BikeStores dans le schéma `public` de Supabase à
partir des 9 fichiers `data_raw/export_parquet_public_<table>.parquet` (export
dlt du dataset BigQuery `local_bike_raw`). C'est l'inverse du flux nominal
(`load_supabase_to_bq.py`) : on repeuple la **source** pour repartir d'un projet
Supabase neuf, le pipeline ELT Supabase -> BigQuery restant inchangé ensuite.

Le chargement est idempotent et rejouable :

  - DDL appliquée depuis `ingestion/sql/bikestores_schema.sql` (`create if not exists`) ;
  - `truncate ... restart identity cascade` sur les 9 tables avant insertion ;
  - insertion par `COPY ... FROM STDIN` (CSV en mémoire), dans l'ordre des
    dépendances de clés étrangères ;
  - la contrainte auto-référente `staffs.manager_id` est posée **après** le
    chargement (l'ordre des lignes du Parquet n'est pas topologique).

Deux nettoyages sont appliqués aux données brutes :

  - les colonnes techniques dlt (`_dlt_load_id`, `_dlt_id`) sont écartées ;
  - le sentinelle textuelle `'NULL'` produite par l'export BigQuery est
    reconvertie en vrai NULL SQL (`customers.phone`, `orders.shipped_date`,
    `staffs.manager_id`).

Usage :
    python ingestion/load_parquet_to_supabase.py            # les 9 tables
    python ingestion/load_parquet_to_supabase.py customers  # une seule table
    python ingestion/load_parquet_to_supabase.py --dry-run  # lit/valide sans écrire
"""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq
from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load-parquet")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data_raw"
SCHEMA_DDL = PROJECT_ROOT / "ingestion" / "sql" / "bikestores_schema.sql"

TARGET_SCHEMA = "public"

# Ordre de chargement = ordre des dépendances de clés étrangères (parents d'abord).
LOAD_ORDER: list[str] = [
    "brands",
    "categories",
    "products",
    "stores",
    "staffs",
    "customers",
    "orders",
    "order_items",
    "stocks",
]

# Colonnes techniques ajoutées par dlt lors de l'export : hors modèle métier.
DLT_COLUMNS = {"_dlt_load_id", "_dlt_id"}

# Sentinelle textuelle laissée par l'export BigQuery à la place d'un NULL.
NULL_SENTINEL = "NULL"


def env(name: str, default: str | None = None, *, required: bool = False) -> str:
    """Lit une variable d'environnement, avec erreur explicite si manquante."""
    value = os.getenv(name, default)
    if required and not value:
        log.error("Variable d'environnement manquante : %s (voir .env.example)", name)
        sys.exit(1)
    return value or ""


def connection_params() -> dict[str, str | int]:
    """Paramètres de connexion Supabase (SSL imposé), lus depuis le `.env`."""
    return {
        "host": env("SUPABASE_HOST", required=True),
        "port": int(env("SUPABASE_PORT", "5432")),
        "user": env("SUPABASE_USER", required=True),
        "password": env("SUPABASE_PASSWORD", required=True),
        "dbname": env("SUPABASE_DB", "postgres"),
        "sslmode": env("SUPABASE_SSLMODE", "require"),
    }


# --------------------------------------------------------------------------- #
# Lecture / nettoyage des Parquet
# --------------------------------------------------------------------------- #

def parquet_path(table: str) -> Path:
    """Chemin du fichier Parquet exporté pour une table source."""
    return DATA_DIR / f"export_parquet_{TARGET_SCHEMA}_{table}.parquet"


def read_table(table: str) -> tuple[list[str], list[list[object]]]:
    """Lit un Parquet et renvoie `(colonnes métier, lignes nettoyées)`.

    Écarte les colonnes techniques dlt et reconvertit la sentinelle `'NULL'`
    (export BigQuery) en vrai NULL.
    """
    path = parquet_path(table)
    if not path.exists():
        log.error("Fichier Parquet introuvable : %s", path)
        sys.exit(1)

    arrow_table = pq.read_table(path)
    columns = [c for c in arrow_table.schema.names if c not in DLT_COLUMNS]
    data = {c: arrow_table[c].to_pylist() for c in columns}

    rows = [
        [None if data[c][i] == NULL_SENTINEL else data[c][i] for c in columns]
        for i in range(arrow_table.num_rows)
    ]
    return columns, rows


def to_csv_buffer(rows: list[list[object]]) -> io.StringIO:
    """Sérialise les lignes en CSV pour `COPY FROM STDIN` (NULL = chaîne vide)."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow(["" if v is None else v for v in row])
    buffer.seek(0)
    return buffer


# --------------------------------------------------------------------------- #
# Écriture vers PostgreSQL
# --------------------------------------------------------------------------- #

def apply_schema(cursor) -> None:
    """Applique la DDL BikeStores (idempotente) au schéma `public`."""
    log.info("Application de la DDL : %s", SCHEMA_DDL.relative_to(PROJECT_ROOT))
    cursor.execute(SCHEMA_DDL.read_text(encoding="utf-8"))


def truncate(cursor, tables: list[str]) -> None:
    """Vide les tables cibles en une passe (cascade : ordre des FK indifférent)."""
    targets = ", ".join(f'"{TARGET_SCHEMA}"."{t}"' for t in tables)
    cursor.execute(f"truncate table {targets} restart identity cascade")


def copy_table(cursor, table: str, columns: list[str], rows: list[list[object]]) -> None:
    """Insère les lignes d'une table via `COPY ... FROM STDIN` (format CSV)."""
    column_list = ", ".join(f'"{c}"' for c in columns)
    # Identifiants issus du schéma Parquet + LOAD_ORDER (pas d'entrée utilisateur).
    sql = (
        f'copy "{TARGET_SCHEMA}"."{table}" ({column_list}) '
        f"from stdin with (format csv, null '')"
    )
    cursor.copy_expert(sql, to_csv_buffer(rows))


def add_self_reference(cursor) -> None:
    """Pose la FK auto-référente `staffs.manager_id -> staffs.staff_id`.

    Créée après le chargement : l'ordre des lignes du Parquet ne garantit pas
    qu'un manager soit inséré avant ses subordonnés.
    """
    cursor.execute(
        """
        do $$
        begin
            if not exists (
                select 1 from pg_constraint where conname = 'staffs_manager_id_fkey'
            ) then
                alter table public.staffs
                    add constraint staffs_manager_id_fkey
                    foreign key (manager_id) references public.staffs (staff_id);
            end if;
        end $$;
        """
    )


def verify(cursor, tables: list[str]) -> dict[str, int]:
    """Recompte les lignes côté PostgreSQL après chargement, par table."""
    union = " union all ".join(
        f"select '{t}' as table_name, count(*) as n from \"{TARGET_SCHEMA}\".\"{t}\""
        for t in tables
    )
    cursor.execute(union)
    return {name: int(n) for name, n in cursor.fetchall()}


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(only: list[str] | None = None, *, dry_run: bool = False) -> None:
    """Recharge les Parquet de `data_raw/` dans le schéma `public` de Supabase."""
    load_dotenv()

    tables = LOAD_ORDER
    if only:
        wanted = {o.lower() for o in only}
        tables = [t for t in LOAD_ORDER if t in wanted]
        if not tables:
            log.error("Aucune table connue parmi : %s", ", ".join(only))
            sys.exit(1)

    # Lecture/validation des Parquet avant toute écriture : un fichier manquant
    # ou illisible interrompt le run sans avoir touché à la base.
    payload = {t: read_table(t) for t in tables}
    for table, (columns, rows) in payload.items():
        log.info("Lu %-12s : %5d lignes, %d colonnes", table, len(rows), len(columns))

    if dry_run:
        log.info("--dry-run : aucune écriture, %d tables validées.", len(payload))
        return

    params = connection_params()
    log.info(
        "Connexion Supabase %s@%s:%s/%s (sslmode=%s) …",
        params["user"], params["host"], params["port"], params["dbname"], params["sslmode"],
    )

    # Une seule transaction : en cas d'erreur, la base reste dans son état initial.
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cur:
            apply_schema(cur)
            truncate(cur, tables)
            for table in tables:
                columns, rows = payload[table]
                copy_table(cur, table, columns, rows)
                log.info("Chargé %-12s : %5d lignes", table, len(rows))
            if "staffs" in tables:
                add_self_reference(cur)
            counts = verify(cur, tables)

    log.info("Volumétrie finale dans Supabase (schéma %s) :", TARGET_SCHEMA)
    for table in tables:
        expected = len(payload[table][1])
        actual = counts.get(table, 0)
        status = "OK" if expected == actual else "ECART"
        log.info("  %-12s %5d attendu / %5d en base  [%s]", table, expected, actual, status)
    log.info("Total : %d lignes.", sum(counts.values()))


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    main(only=args or None, dry_run="--dry-run" in sys.argv[1:])
