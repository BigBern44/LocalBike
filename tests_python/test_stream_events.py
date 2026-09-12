"""S-05 — Robustesse du traitement d'un événement de stock.

`build_event_row` traduit la charge utile `pg_notify` émise par le trigger
PostgreSQL en ligne BigQuery. Elle est appelée depuis la boucle d'écoute, pour
chaque notification : c'est un point de passage obligé du flux temps réel.

Exigence du cahier : un événement isolément corrompu (JSON tronqué, champ manquant)
doit être **compté et ignoré**, jamais interrompre l'écoute. Le flux n'a pas de
reprise : une exception non traitée ici ferait perdre tous les mouvements suivants
jusqu'au redémarrage du process.
"""

from __future__ import annotations

import json

import pytest
from prometheus_client import REGISTRY

from ingestion import metrics
from ingestion.stream_stock_alerts import build_event_row

SEUIL = 5


def payload(**surcharges) -> str:
    """Charge utile nominale du trigger `stock_events`, surchargeable par test."""
    evenement = {
        "event_ts": "2026-09-11T10:00:00+00:00",
        "event_type": "UPDATE",
        "store_id": 1,
        "product_id": 42,
        "quantity_before": 10,
        "quantity_after": 8,
    }
    evenement.update(surcharges)
    return json.dumps(evenement)


def _parse_errors() -> float:
    return REGISTRY.get_sample_value("local_bike_stream_parse_errors_total") or 0.0


# --------------------------------------------------------------------------- #
# Traduction nominale
# --------------------------------------------------------------------------- #

def test_les_champs_du_trigger_sont_repris_tels_quels():
    ligne = build_event_row(payload(), SEUIL)

    assert ligne["event_ts"] == "2026-09-11T10:00:00+00:00"
    assert ligne["event_type"] == "UPDATE"
    assert ligne["store_id"] == 1
    assert ligne["product_id"] == 42
    assert ligne["quantity_before"] == 10
    assert ligne["quantity_after"] == 8


def test_chaque_evenement_recoit_un_identifiant_unique():
    assert build_event_row(payload(), SEUIL)["event_id"] != build_event_row(payload(), SEUIL)["event_id"]


def test_le_seuil_en_vigueur_est_porte_par_l_evenement():
    """Le seuil est stocké sur la ligne : la couche SQL en aval reste cohérente même
    si `STOCK_ALERT_THRESHOLD` évolue entre deux événements."""
    assert build_event_row(payload(), 3)["alert_threshold"] == 3


def test_l_horodatage_de_reception_est_ajoute():
    ligne = build_event_row(payload(), SEUIL)
    assert ligne["ingested_at"] > ligne["event_ts"], (
        "ingested_at doit suivre event_ts : c'est la base du calcul de latence"
    )


# --------------------------------------------------------------------------- #
# Seuil d'alerte
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    ("quantite", "en_alerte"),
    [(0, True), (4, True), (5, True), (6, False), (100, False)],
    ids=["rupture", "sous-le-seuil", "au-seuil", "juste-au-dessus", "confortable"],
)
def test_le_seuil_est_inclusif(quantite, en_alerte):
    """`quantity_after <= threshold` : une quantité ÉGALE au seuil déclenche
    l'alerte. C'est la règle que reprend la vue `rpt_stock_alerts_live`."""
    ligne = build_event_row(payload(quantity_after=quantite), SEUIL)
    assert ligne["is_below_threshold"] is en_alerte


# --------------------------------------------------------------------------- #
# Charges utiles illisibles
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "charge",
    [
        '{"event_ts": "2026-09-11T10:00:00+00:00", "event_type": "UPDATE"',  # JSON tronqué
        "pas du json du tout",
        "",
        "null",
        '{"event_type": "UPDATE", "store_id": 1}',  # champs obligatoires manquants
    ],
    ids=["json-tronque", "texte-libre", "vide", "null", "champs-manquants"],
)
def test_une_charge_illisible_est_ignoree_sans_exception(charge):
    assert build_event_row(charge, SEUIL) is None, (
        "une charge utile illisible doit être ignorée, pas propager une exception"
    )


def test_une_charge_illisible_est_comptee_pour_la_supervision():
    avant = _parse_errors()
    build_event_row("json invalide", SEUIL)
    assert _parse_errors() == avant + 1, (
        "la série local_bike_stream_parse_errors_total doit refléter le rejet"
    )


def test_un_evenement_valide_ne_compte_aucune_erreur():
    avant = _parse_errors()
    build_event_row(payload(), SEUIL)
    assert _parse_errors() == avant
