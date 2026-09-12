"""Métriques Prometheus du flux temps réel (supervision de l'infrastructure).

Prometheus fonctionne en **pull** : ce module expose un endpoint HTTP `/metrics`
que le serveur Prometheus scrape périodiquement. Le listener
(`ingestion/stream_stock_alerts.py`) étant un process **long-vivant**, c'est le
modèle natif — inutile de pousser quoi que ce soit.

Bénéfice direct : la *liveness* devient gratuite. Si le process meurt, le scrape
échoue et Prometheus passe la série `up{job="stock_stream"}` à 0, ce qui suffit à
déclencher l'alerte. Aucune table de heartbeat à maintenir.

Répartition des rôles (cf. `monitoring/README.md`) :
  - **Prometheus** = santé de l'infrastructure (le flux tourne-t-il ? à quelle
    latence ? avec quel débit ? combien de rejets ?) — haute fréquence, rétention courte ;
  - **BigQuery** (`local_bike_monitoring`) = santé des *données* (fraîcheur,
    volumétrie, tests dbt) — basse fréquence, rétention longue ;
  - **Grafana** = façade unique, branchée sur les deux.

Cardinalité : aucun label à forte cardinalité (pas de `product_id`, ~320 valeurs,
ni de `store_id`) — le niveau de stock par produit est une donnée *métier*, sa
place est dans BigQuery (`rpt_stock_alerts_live`), pas dans une série temporelle
d'infrastructure.

Désactivation : `STREAM_METRICS_PORT=0` dans le `.env`.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger("stream.metrics")

# Préfixe commun à toutes les séries — `local_bike_stream_*`.
PREFIX = "local_bike_stream"

# Port d'exposition par défaut (scrapé par Prometheus, cf. monitoring/prometheus/prometheus.yml).
DEFAULT_PORT = 9101


# ----- Compteurs (monotones : seul leur taux de croissance a du sens) -----

EVENTS_RECEIVED = Counter(
    f"{PREFIX}_events_total",
    "Mouvements de stock reçus sur le canal PostgreSQL `stock_events`.",
    ["event_type"],  # INSERT / UPDATE — 2 valeurs, cardinalité négligeable
)

ALERTS_RAISED = Counter(
    f"{PREFIX}_alerts_total",
    "Mouvements laissant la quantité sous le seuil d'alerte (is_below_threshold).",
)

ROWS_FLUSHED = Counter(
    f"{PREFIX}_rows_flushed_total",
    "Lignes insérées avec succès dans BigQuery (streaming insert).",
)

INSERT_ERRORS = Counter(
    f"{PREFIX}_insert_errors_total",
    "Lignes rejetées par BigQuery lors d'un streaming insert.",
)

PARSE_ERRORS = Counter(
    f"{PREFIX}_parse_errors_total",
    "Charges utiles pg_notify illisibles (JSON invalide ou champ manquant).",
)


# ----- Histogramme : distribution de la latence de bout en bout -----

# Mesure `ingested_at - event_ts`, soit le délai entre le COMMIT côté PostgreSQL et
# la réception par le listener. Les bornes sont resserrées sous la seconde : c'est
# l'ordre de grandeur attendu (pg_notify est quasi instantané), et l'intérêt est de
# détecter une dérive, pas de mesurer des minutes.
EVENT_LATENCY = Histogram(
    f"{PREFIX}_event_latency_seconds",
    "Latence entre l'émission PostgreSQL (event_ts) et la réception (ingested_at).",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


# ----- Jauges (valeur instantanée) -----

QUEUE_DEPTH = Gauge(
    f"{PREFIX}_queue_depth",
    "Événements en attente dans la file, entre deux flushs BigQuery.",
)

LAST_EVENT_TIMESTAMP = Gauge(
    f"{PREFIX}_last_event_timestamp_seconds",
    "Horodatage (epoch) du dernier événement reçu — `time() - valeur` = silence du flux.",
)

LAST_FLUSH_TIMESTAMP = Gauge(
    f"{PREFIX}_last_flush_timestamp_seconds",
    "Horodatage (epoch) du dernier flush BigQuery réussi.",
)

ALERT_THRESHOLD = Gauge(
    f"{PREFIX}_alert_threshold",
    "Seuil d'alerte de stock appliqué par le listener (STOCK_ALERT_THRESHOLD).",
)

START_TIMESTAMP = Gauge(
    f"{PREFIX}_start_timestamp_seconds",
    "Horodatage (epoch) du démarrage du listener — `time() - valeur` = uptime.",
)


def observe_event(event_ts: str, ingested_at: str, event_type: str, is_alert: bool) -> None:
    """Enregistre un événement reçu : compteurs, latence et fraîcheur.

    `event_ts` et `ingested_at` sont les chaînes ISO déjà présentes sur la ligne
    BigQuery — on ne recalcule rien, on réutilise ce qui est écrit en base pour que
    Prometheus et BigQuery mesurent exactement la même chose.

    La latence n'est observée que si les deux horodatages sont exploitables : une
    charge utile malformée ne doit jamais interrompre l'écoute.
    """
    EVENTS_RECEIVED.labels(event_type=event_type or "UNKNOWN").inc()
    if is_alert:
        ALERTS_RAISED.inc()

    emitted = _parse_timestamp(event_ts)
    received = _parse_timestamp(ingested_at)
    if emitted is None or received is None:
        return

    LAST_EVENT_TIMESTAMP.set(received.timestamp())
    # Une horloge source en avance (dérive NTP) donnerait une latence négative,
    # refusée par un histogramme : on la ramène à 0.
    EVENT_LATENCY.observe(max((received - emitted).total_seconds(), 0.0))


def observe_flush(rows_ok: int, rows_rejected: int = 0) -> None:
    """Enregistre le résultat d'un flush BigQuery."""
    if rows_ok:
        ROWS_FLUSHED.inc(rows_ok)
        LAST_FLUSH_TIMESTAMP.set_to_current_time()
    if rows_rejected:
        INSERT_ERRORS.inc(rows_rejected)


def _parse_timestamp(value: str) -> datetime | None:
    """Parse un horodatage ISO 8601, en supposant UTC s'il est naïf.

    `event_ts` provient de `current_timestamp` PostgreSQL sérialisé par
    `json_build_object` : normalement avec décalage (`+00:00`). Le fallback couvre
    une session Supabase configurée autrement.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        PARSE_ERRORS.inc()
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def start_metrics_server(threshold: int) -> int | None:
    """Démarre l'endpoint `/metrics` et amorce les jauges de contexte.

    Retourne le port écouté, ou `None` si la supervision est désactivée
    (`STREAM_METRICS_PORT=0`) ou si le port est déjà pris — dans les deux cas le
    flux continue : la supervision ne doit jamais faire tomber ce qu'elle observe.
    """
    port = int(os.getenv("STREAM_METRICS_PORT", str(DEFAULT_PORT)) or 0)
    if port <= 0:
        log.info("Métriques Prometheus désactivées (STREAM_METRICS_PORT=0).")
        return None

    try:
        start_http_server(port)
    except OSError as exc:
        log.warning("Endpoint /metrics indisponible sur le port %d (%s) — flux maintenu.", port, exc)
        return None

    ALERT_THRESHOLD.set(threshold)
    START_TIMESTAMP.set_to_current_time()
    log.info("Métriques Prometheus exposées sur http://0.0.0.0:%d/metrics", port)
    return port
