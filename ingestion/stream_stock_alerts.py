"""Pipeline temps réel : mouvements de stock Supabase -> BigQuery (streaming).

Complément **événementiel** du batch dlt (`load_supabase_to_bq.py`, quotidien 05:00) :
au lieu d'attendre le run du lendemain, chaque changement sur `public.stocks` est
capté en moins d'une seconde et poussé dans `local_bike_raw.stock_events_stream`.

Chaîne complète :
    trigger PostgreSQL -> pg_notify sur le canal `stock_events`
      -> LISTEN psycopg2, réveillé par la boucle asyncio (aucun polling)
      -> BigQuery streaming insert (`insert_rows_json`, pas de job de chargement)
      -> vue dbt `local_bike_reporting.rpt_stock_alerts_live` -> widget Looker Studio

Ce script n'écrit **que** dans la table d'événements : l'agrégat d'alertes en aval est
une *vue* dbt (`models/marts/reporting/rpt_stock_alerts_live.sql`), recalculée à chaque
lecture du dashboard. Rien à rafraîchir périodiquement — réécrire une table toutes les
n secondes est le contre-emploi de BigQuery (réécriture complète à chaque passage,
quota de modifications de table), là où le streaming insert est la primitive prévue.

**Supervision** : le process expose ses métriques Prometheus sur
`http://localhost:9101/metrics` (cf. `ingestion/metrics.py`) — latence de bout en bout,
débit, rejets BigQuery, profondeur de file. Étant long-vivant, il se prête au modèle
*pull* de Prometheus : l'échec du scrape vaut détection de panne (`up == 0`), sans
table de heartbeat à maintenir. Stack de supervision : `monitoring/`.

Le transport est natif PostgreSQL : la connexion est celle de l'ingestion
(`SUPABASE_*` du `.env`, SSL imposé), donc **aucune clé API Supabase** n'est requise.
La connexion directe (`db.<ref>.supabase.co`) convient ; si l'on repasse par le
pooler, il faut le *Session mode* (port 5432) — le mode transaction ne supporte
pas `LISTEN`.

Prérequis, une seule fois côté Supabase : cf. `ingestion/sql/stock_events_trigger.sql`.

La table `public.stocks` ne portant pas de seuil de réapprovisionnement, celui-ci est
lu dans l'environnement (`STOCK_ALERT_THRESHOLD`) et **stocké sur chaque événement**,
pour que la couche SQL en aval reste cohérente même si le seuil évolue.

Usage :
    make stream                              # recommandé (charge .env)
    python -m ingestion.stream_stock_alerts  # équivalent direct
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion import metrics
from ingestion.load_supabase_to_bq import bigquery_client, env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stream")

CHANNEL = "stock_events"
EVENTS_TABLE = "stock_events_stream"
# Les événements sont bufferisés puis insérés par lot : le callback de notification
# rend la main immédiatement (pas d'appel réseau BigQuery dans la boucle d'écoute).
FLUSH_INTERVAL_SECONDS = 1.0

EVENTS_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("store_id", "INT64"),
    bigquery.SchemaField("product_id", "INT64"),
    bigquery.SchemaField("quantity_before", "INT64"),
    bigquery.SchemaField("quantity_after", "INT64"),
    bigquery.SchemaField("alert_threshold", "INT64"),
    bigquery.SchemaField("is_below_threshold", "BOOL"),
]


def listen_connection() -> psycopg2.extensions.connection:
    """Connexion PostgreSQL dédiée à l'écoute (autocommit : requis par LISTEN)."""
    conn = psycopg2.connect(
        host=env("SUPABASE_HOST", required=True),
        port=env("SUPABASE_PORT", "5432"),
        user=env("SUPABASE_USER", required=True),
        password=env("SUPABASE_PASSWORD", required=True),
        dbname=env("SUPABASE_DB", "postgres"),
        sslmode=env("SUPABASE_SSLMODE", "require"),
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute(f"LISTEN {CHANNEL};")
    return conn


def ensure_events_table(client: bigquery.Client) -> str:
    """Crée si besoin la table d'événements (partitionnée par jour, clusterisée)."""
    dataset = env("GCP_DATASET_RAW", "local_bike_raw")
    table_id = f"{client.project}.{dataset}.{EVENTS_TABLE}"
    table = bigquery.Table(table_id, schema=EVENTS_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="event_ts")
    table.clustering_fields = ["store_id", "product_id"]
    client.create_table(table, exists_ok=True)
    return table_id


def build_event_row(payload: str, threshold: int) -> dict[str, Any] | None:
    """Traduit la charge utile JSON du trigger en ligne BigQuery.

    Retourne `None` si la charge utile est illisible — JSON invalide, tronqué, ou
    champ obligatoire absent. Le flux n'a pas de reprise : une exception remontant
    d'ici ferait perdre tous les mouvements suivants jusqu'au redémarrage du
    process. Un événement isolément corrompu est donc **compté puis ignoré**, ce
    que la série `local_bike_stream_parse_errors_total` rend visible à la
    supervision (cf. scénario S-05 du cahier de recettes).
    """
    try:
        event = json.loads(payload)
        quantity_after = event["quantity_after"]
        return {
            "event_id": str(uuid.uuid4()),
            "event_ts": event["event_ts"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "event_type": event["event_type"],
            "store_id": event["store_id"],
            "product_id": event["product_id"],
            "quantity_before": event["quantity_before"],
            "quantity_after": quantity_after,
            "alert_threshold": threshold,
            "is_below_threshold": quantity_after <= threshold,
        }
    except (json.JSONDecodeError, KeyError, TypeError) as erreur:
        metrics.PARSE_ERRORS.inc()
        log.warning("Charge utile pg_notify illisible, événement ignoré : %s", erreur)
        return None


def drain(events: queue.SimpleQueue) -> list[dict[str, Any]]:
    """Vide la file des événements accumulés depuis le dernier flush."""
    rows: list[dict[str, Any]] = []
    while not events.empty():
        rows.append(events.get_nowait())
    return rows


def flush(client: bigquery.Client, table_id: str, rows: list[dict[str, Any]]) -> None:
    """Insère un lot d'événements en streaming (insertAll : lisible immédiatement)."""
    if not rows:
        return
    errors = client.insert_rows_json(table_id, rows)
    # `insert_rows_json` renvoie une entrée par ligne rejetée : un lot peut être
    # partiellement accepté, d'où le décompte séparé remonté à Prometheus.
    rejected = len(errors)
    metrics.observe_flush(rows_ok=len(rows) - rejected, rows_rejected=rejected)
    if errors:
        log.error("Rejets BigQuery : %s", errors)
    else:
        log.info("%d événement(s) -> %s", len(rows), table_id)


async def flush_loop(client: bigquery.Client, table_id: str, events: queue.SimpleQueue) -> None:
    """Writer asynchrone : flush périodique, l'insert BigQuery (bloquant) part en thread."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        # Mesurée avant vidage : c'est le retard accumulé pendant l'intervalle.
        # Elle ne monte durablement que si BigQuery n'absorbe plus le débit entrant.
        metrics.QUEUE_DEPTH.set(events.qsize())
        rows = drain(events)
        if rows:
            await asyncio.to_thread(flush, client, table_id, rows)


async def main() -> None:
    load_dotenv()

    threshold = int(env("STOCK_ALERT_THRESHOLD", "5"))
    # Démarré en premier : l'endpoint répond même si Supabase ou BigQuery est
    # injoignable, ce qui distingue « process mort » de « dépendance en panne ».
    metrics.start_metrics_server(threshold)
    client = bigquery_client()
    table_id = ensure_events_table(client)
    events: queue.SimpleQueue = queue.SimpleQueue()
    conn = listen_connection()

    def on_notify() -> None:
        """Appelé par la boucle asyncio dès que la socket PostgreSQL a des données."""
        conn.poll()
        while conn.notifies:
            notification = conn.notifies.pop(0)
            row = build_event_row(notification.payload, threshold)
            if row is None:
                continue  # charge utile illisible : déjà comptée et journalisée
            events.put(row)
            metrics.observe_event(
                event_ts=row["event_ts"],
                ingested_at=row["ingested_at"],
                event_type=row["event_type"],
                is_alert=row["is_below_threshold"],
            )
            alert = " ⚠ ALERTE" if row["is_below_threshold"] else ""
            log.info(
                "magasin=%s produit=%s : %s -> %s%s",
                row["store_id"], row["product_id"],
                row["quantity_before"], row["quantity_after"], alert,
            )

    loop = asyncio.get_running_loop()
    loop.add_reader(conn.fileno(), on_notify)
    log.info("Écoute du canal %s (seuil=%s, Ctrl+C pour arrêter) …", CHANNEL, threshold)

    try:
        await flush_loop(client, table_id, events)
    finally:
        loop.remove_reader(conn.fileno())
        flush(client, table_id, drain(events))
        conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Arrêt demandé.")
