"""S-04 — Métriques Prometheus du flux temps réel.

Ces séries constituent un **contrat** entre trois fichiers qui ne se relisent pas
entre eux : le code qui les émet (`ingestion/metrics.py`), la configuration qui les
scrape (`monitoring/prometheus/prometheus.yml`) et les règles qui les alertent
(`monitoring/grafana/alerting/rules.yml.tmpl`).

Renommer une métrique ne casse rien visiblement : le code tourne, le scrape réussit,
et l'alerte cesse simplement de se déclencher — une panne de supervision silencieuse,
c'est-à-dire le pire cas. Ces tests verrouillent le contrat.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from prometheus_client import REGISTRY

from ingestion import metrics

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REGLES_ALERTES = PROJECT_ROOT / "monitoring/grafana/alerting/rules.yml.tmpl"
CONFIG_PROMETHEUS = PROJECT_ROOT / "monitoring/prometheus/prometheus.yml"


# Suffixes ajoutés par le format d'exposition Prometheus à une même famille de
# métriques (`_total` pour un compteur, `_bucket`/`_count`/`_sum` pour un
# histogramme). On raisonne sur la famille : un compteur à labels n'expose aucun
# échantillon tant qu'aucune valeur de label n'a été utilisée.
SUFFIXES = ("_total", "_bucket", "_count", "_sum", "_created")


def _famille(serie: str) -> str:
    for suffixe in SUFFIXES:
        if serie.endswith(suffixe):
            return serie[: -len(suffixe)]
    return serie


def _familles_exposees() -> set[str]:
    """Familles de métriques déclarées par le listener, préfixe projet uniquement."""
    return {
        famille.name
        for famille in REGISTRY.collect()
        if famille.name.startswith(metrics.PREFIX)
    }


# --------------------------------------------------------------------------- #
# Nommage et exposition
# --------------------------------------------------------------------------- #

def test_toutes_les_series_portent_le_prefixe_du_projet():
    assert metrics.PREFIX == "local_bike_stream"
    assert _familles_exposees(), "aucune métrique déclarée : le module n'a pas été chargé"


def test_le_port_par_defaut_correspond_a_la_cible_prometheus():
    assert metrics.DEFAULT_PORT == 9101
    cible = f"host.docker.internal:{metrics.DEFAULT_PORT}"
    assert cible in CONFIG_PROMETHEUS.read_text(), (
        "le port exposé par le listener et la cible scrapée ont divergé"
    )


@pytest.mark.parametrize(
    "serie",
    [
        "local_bike_stream_events_total",
        "local_bike_stream_alerts_total",
        "local_bike_stream_rows_flushed_total",
        "local_bike_stream_insert_errors_total",
        "local_bike_stream_parse_errors_total",
        "local_bike_stream_event_latency_seconds_bucket",
        "local_bike_stream_queue_depth",
        "local_bike_stream_last_event_timestamp_seconds",
        "local_bike_stream_last_flush_timestamp_seconds",
        "local_bike_stream_alert_threshold",
        "local_bike_stream_start_timestamp_seconds",
    ],
)
def test_chaque_serie_attendue_est_exposee(serie):
    assert _famille(serie) in _familles_exposees()


def test_les_series_alertees_existent_toutes_dans_le_code():
    """Le test le plus utile du module : aucune règle d'alerte ne doit référencer
    une série que le code n'émet plus."""
    referencees = {
        _famille(serie)
        for serie in re.findall(r"local_bike_stream_[a-z_]+", REGLES_ALERTES.read_text())
    }
    assert referencees, "aucune série du flux référencée dans les règles d'alerte"
    manquantes = referencees - _familles_exposees()
    assert not manquantes, f"règles d'alerte orphelines : {sorted(manquantes)}"


# --------------------------------------------------------------------------- #
# Comportement des observations
# --------------------------------------------------------------------------- #

def _valeur(nom: str, **libelles) -> float:
    return REGISTRY.get_sample_value(nom, libelles or None) or 0.0


def test_un_evenement_incremente_le_compteur_et_la_latence():
    avant = _valeur("local_bike_stream_events_total", event_type="UPDATE")
    observations_avant = _valeur("local_bike_stream_event_latency_seconds_count")

    metrics.observe_event(
        event_ts="2026-09-11T10:00:00+00:00",
        ingested_at="2026-09-11T10:00:00.250000+00:00",
        event_type="UPDATE",
        is_alert=False,
    )

    assert _valeur("local_bike_stream_events_total", event_type="UPDATE") == avant + 1
    assert _valeur("local_bike_stream_event_latency_seconds_count") == observations_avant + 1


def test_un_evenement_sous_le_seuil_compte_comme_alerte():
    avant = _valeur("local_bike_stream_alerts_total")
    metrics.observe_event(
        event_ts="2026-09-11T10:00:00+00:00",
        ingested_at="2026-09-11T10:00:01+00:00",
        event_type="UPDATE",
        is_alert=True,
    )
    assert _valeur("local_bike_stream_alerts_total") == avant + 1


def test_une_horloge_source_en_avance_ne_produit_pas_de_latence_negative():
    """Dérive NTP : `ingested_at` antérieur à `event_ts`. Un histogramme refuse une
    valeur négative — l'observation doit être ramenée à 0, pas provoquer d'erreur."""
    avant = _valeur("local_bike_stream_event_latency_seconds_bucket", le="0.01")
    metrics.observe_event(
        event_ts="2026-09-11T10:00:05+00:00",
        ingested_at="2026-09-11T10:00:00+00:00",
        event_type="UPDATE",
        is_alert=False,
    )
    assert _valeur("local_bike_stream_event_latency_seconds_bucket", le="0.01") == avant + 1


def test_un_horodatage_illisible_est_compte_et_n_interrompt_rien():
    avant = _valeur("local_bike_stream_parse_errors_total")
    metrics.observe_event(
        event_ts="pas une date",
        ingested_at="2026-09-11T10:00:00+00:00",
        event_type="UPDATE",
        is_alert=False,
    )
    assert _valeur("local_bike_stream_parse_errors_total") > avant


def test_un_flush_partiellement_rejete_compte_les_deux_cotes():
    """`insert_rows_json` peut accepter une partie du lot : les lignes acceptées et
    les rejets alimentent deux séries distinctes (l'alerte porte sur les rejets)."""
    ok_avant = _valeur("local_bike_stream_rows_flushed_total")
    ko_avant = _valeur("local_bike_stream_insert_errors_total")

    metrics.observe_flush(rows_ok=8, rows_rejected=2)

    assert _valeur("local_bike_stream_rows_flushed_total") == ok_avant + 8
    assert _valeur("local_bike_stream_insert_errors_total") == ko_avant + 2
