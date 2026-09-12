"""S-01 — Stratégie de chargement par table (cf. recette/scenarios/structurels.md).

Contrepartie structurelle des scénarios fonctionnels F-03 et F-04 : ceux-ci
vérifient l'idempotence *observée* après deux ingestions réelles, ceux-ci vérifient
que la *configuration* qui la produit n'a pas dérivé.

Enjeu : si une table transactionnelle sortait de `MERGE_PRIMARY_KEYS`, elle
basculerait silencieusement en `replace`. La volumétrie resterait correcte — donc
F-03 passerait toujours — mais l'historique des `_dlt_load_id` serait écrasé à
chaque run et la stratégie documentée ne décrirait plus le comportement réel.
"""

from __future__ import annotations

from ingestion.load_supabase_to_bq import (
    MERGE_PRIMARY_KEYS,
    SOURCE_SCHEMA,
    SOURCE_TABLES,
)

# Les 9 tables BikeStores attendues (cf. section 5 du CLAUDE.md).
TABLES_ATTENDUES = {
    "customers", "orders", "order_items", "staffs", "stores",
    "categories", "products", "stocks", "brands",
}
# Transactionnelles : chargées en merge (upsert) sur leur clé primaire.
TRANSACTIONNELLES = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": ["order_id", "item_id"],
}


def test_les_neuf_tables_source_sont_declarees():
    assert set(SOURCE_TABLES) == TABLES_ATTENDUES
    assert len(SOURCE_TABLES) == 9, "aucune table ne doit être ajoutée ou retirée en silence"


def test_schema_source_est_public():
    # Le nommage BigQuery `public_<table>` en dépend, comme les sources dbt.
    assert SOURCE_SCHEMA == "public"


def test_les_transactionnelles_sont_chargees_en_merge():
    assert MERGE_PRIMARY_KEYS == TRANSACTIONNELLES


def test_la_cle_composite_des_lignes_de_commande_est_respectee():
    # order_items a un grain (order_id, item_id) : un merge sur le seul order_id
    # écraserait toutes les lignes d'une commande sauf une.
    assert MERGE_PRIMARY_KEYS["order_items"] == ["order_id", "item_id"]


def test_les_referentiels_sont_charges_en_replace():
    referentiels = TABLES_ATTENDUES - set(TRANSACTIONNELLES)
    assert referentiels.isdisjoint(MERGE_PRIMARY_KEYS), (
        "un référentiel présent dans MERGE_PRIMARY_KEYS basculerait en merge"
    )
    assert len(referentiels) == 6


def test_le_libelle_de_strategie_reflete_la_configuration():
    """Le libellé alimente la description des assets Dagster : il doit rester vrai."""
    from orchestration.ingestion import _load_strategy

    assert _load_strategy("orders").startswith("merge")
    assert "order_id" in _load_strategy("orders")
    assert _load_strategy("brands").startswith("replace")
