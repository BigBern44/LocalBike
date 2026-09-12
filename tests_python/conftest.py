"""Configuration commune de la suite pytest (volet structurel du cahier de recettes).

Ces tests portent sur des **fonctions pures ou moquées** : aucun n'ouvre de
connexion à Supabase, BigQuery ou Prometheus. Ils doivent donc passer sur un poste
sans `.env` et dans une CI sans secret — c'est la propriété qui les rend utilisables
comme garde-fou à chaque Pull Request.

Le dossier est nommé `tests_python/` et non `tests/` : ce dernier est réservé aux
tests singuliers dbt (`test-paths` de dbt_project.yml).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Racine du dépôt sur le sys.path : les modules testés s'importent comme en
# production (`from ingestion...`), sans installer le projet en paquet.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def env_supabase(monkeypatch: pytest.MonkeyPatch):
    """Environnement Supabase complet et neutre, surchargeable par test.

    Neutralise d'abord toute variable héritée du `.env` du poste : un test ne doit
    jamais dépendre de la configuration locale de celui qui l'exécute.
    """
    valeurs = {
        "SUPABASE_USER": "postgres",
        "SUPABASE_PASSWORD": "motdepasse",
        "SUPABASE_HOST": "db.exemple.supabase.co",
        "SUPABASE_PORT": "5432",
        "SUPABASE_DB": "postgres",
        "SUPABASE_SSLMODE": "require",
    }
    for cle, valeur in valeurs.items():
        monkeypatch.setenv(cle, valeur)
    return valeurs
