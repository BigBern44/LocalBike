"""S-02 / S-03 — Construction de l'URI source et échec explicite.

S-02 touche directement à une règle de sécurité du projet (section 4 du CLAUDE.md) :
« Supabase impose le SSL ». Un `sslmode` perdu au fil d'un refactor ferait transiter
les identifiants en clair sans qu'aucun test de données ne s'en aperçoive.
"""

from __future__ import annotations

from urllib.parse import unquote, urlsplit

import pytest

from ingestion.load_supabase_to_bq import build_source_uri, env


# --------------------------------------------------------------------------- #
# S-02 — URI source
# --------------------------------------------------------------------------- #

def test_l_uri_impose_le_ssl(env_supabase):
    assert "sslmode=require" in build_source_uri()


def test_le_ssl_est_actif_meme_sans_variable(env_supabase, monkeypatch):
    """`SUPABASE_SSLMODE` absent ne doit pas dégrader la connexion en clair."""
    monkeypatch.delenv("SUPABASE_SSLMODE")
    assert "sslmode=require" in build_source_uri()


def test_le_pilote_et_les_defauts_sont_conformes(env_supabase, monkeypatch):
    monkeypatch.delenv("SUPABASE_PORT")
    monkeypatch.delenv("SUPABASE_DB")
    uri = build_source_uri()
    assert uri.startswith("postgresql+psycopg2://")
    parts = urlsplit(uri)
    assert parts.port == 5432
    assert parts.path == "/postgres"


@pytest.mark.parametrize(
    "motdepasse",
    ["av3c@rob@se", "barre/oblique", "deux:points", "diese#final", "P@ss/w:rd#42"],
    ids=["arobase", "slash", "deux-points", "diese", "combine"],
)
def test_le_mot_de_passe_est_url_encode(env_supabase, monkeypatch, motdepasse):
    """Un caractère réservé non encodé casserait le parsing de l'URI — ou pire,
    tronquerait silencieusement le mot de passe au premier `@`."""
    monkeypatch.setenv("SUPABASE_PASSWORD", motdepasse)
    uri = build_source_uri()

    parts = urlsplit(uri)
    assert parts.hostname == "db.exemple.supabase.co", "l'hôte doit rester intact"
    assert unquote(parts.password) == motdepasse, "le mot de passe doit être restituable"
    assert motdepasse not in uri, "la valeur brute ne doit pas apparaître telle quelle"


# --------------------------------------------------------------------------- #
# S-03 — Échec explicite sur variable manquante
# --------------------------------------------------------------------------- #

def test_une_variable_requise_absente_arrete_le_programme(monkeypatch):
    monkeypatch.delenv("VARIABLE_INEXISTANTE", raising=False)
    with pytest.raises(SystemExit) as arret:
        env("VARIABLE_INEXISTANTE", required=True)
    assert arret.value.code != 0, "l'arrêt doit être signalé par un code non nul"


def test_une_variable_requise_vide_est_traitee_comme_absente(monkeypatch):
    # Cas réel : une variable présente dans .env mais laissée sans valeur.
    monkeypatch.setenv("SUPABASE_PASSWORD", "")
    with pytest.raises(SystemExit):
        env("SUPABASE_PASSWORD", required=True)


def test_le_message_d_erreur_nomme_la_variable(monkeypatch, caplog):
    monkeypatch.delenv("SUPABASE_HOST", raising=False)
    with pytest.raises(SystemExit):
        env("SUPABASE_HOST", required=True)
    assert "SUPABASE_HOST" in caplog.text
    assert ".env.example" in caplog.text, "le message doit orienter vers la correction"


def test_aucun_secret_n_est_journalise(env_supabase, caplog):
    """Contrepartie de SEC-08 : un log d'erreur ne doit jamais porter de valeur."""
    build_source_uri()
    assert "motdepasse" not in caplog.text
