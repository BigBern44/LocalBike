"""Campagne fonctionnelle automatisée — scénarios F-01, F-03 à F-06 et F-11.

Exécute les scénarios fonctionnels du cahier de recettes qui peuvent l'être sans
jugement humain (cf. `recette/scenarios/fonctionnels.md`), et produit un rapport
Markdown horodaté dans `recette/resultats/`.

Ce que le script NE fait pas : déclencher les alertes, lire le dashboard, dérouler
le flux temps réel. Ces scénarios restent manuels par nature et sont tracés dans le
procès-verbal de campagne.

⚠️  Ces scénarios écrivent réellement :
      - ils relancent l'ingestion dlt vers `local_bike_raw` (opération nominale et
        idempotente — c'est précisément la propriété testée) ;
      - F-04 modifie une ligne de `public.brands` côté Supabase, puis **rétablit
        l'état initial** dans un bloc `finally`.
    Aucun modèle dbt n'est reconstruit : le script lit les marts, il ne les écrit pas.

Usage :
    python recette/run_fonctionnels.py                    # tous les scénarios
    python recette/run_fonctionnels.py --scenarios F-06   # une sélection
    python recette/run_fonctionnels.py --liste            # inventaire, sans rien exécuter
    python recette/run_fonctionnels.py --sans-rapport     # console uniquement
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

RACINE = Path(__file__).resolve().parents[1]
if str(RACINE) not in sys.path:
    sys.path.insert(0, str(RACINE))

from ingestion.load_supabase_to_bq import (  # noqa: E402
    MERGE_PRIMARY_KEYS,
    SOURCE_SCHEMA,
    SOURCE_TABLES,
    bigquery_client,
    build_source_uri,
    env,
)
from ingestion.load_supabase_to_bq import main as lancer_ingestion  # noqa: E402

DOSSIER_RESULTATS = RACINE / "recette" / "resultats"

# Table et colonne servant de cobaye à F-04 : un référentiel de 9 lignes, sans
# dépendance fonctionnelle en aval du libellé.
TABLE_COBAYE = "brands"
LIBELLE_RECETTE = "RECETTE-F04"


# --------------------------------------------------------------------------- #
# Harnais
# --------------------------------------------------------------------------- #

VERT, ROUGE, JAUNE, GRIS, RAZ = "\033[32m", "\033[31m", "\033[33m", "\033[90m", "\033[0m"


@dataclass
class Constat:
    """Une assertion élémentaire à l'intérieur d'un scénario."""

    ok: bool
    texte: str


@dataclass
class Resultat:
    identifiant: str
    titre: str
    criticite: str
    constats: list[Constat] = field(default_factory=list)
    statut: str = "conforme"  # conforme | non conforme | non joué
    justification: str = ""
    duree: float = 0.0

    def verifier(self, ok: bool, texte: str) -> bool:
        self.constats.append(Constat(bool(ok), texte))
        if not ok:
            self.statut = "non conforme"
        return bool(ok)

    def ignorer(self, justification: str) -> None:
        """Marque le scénario non joué : un prérequis manque, ce n'est pas un échec."""
        self.statut = "non joué"
        self.justification = justification


SCENARIOS: dict[str, tuple[str, str, Callable[[Resultat], None]]] = {}


def scenario(identifiant: str, titre: str, criticite: str):
    def enregistrer(fonction: Callable[[Resultat], None]):
        SCENARIOS[identifiant] = (titre, criticite, fonction)
        return fonction

    return enregistrer


# --------------------------------------------------------------------------- #
# Accès BigQuery
# --------------------------------------------------------------------------- #

def _projet() -> str:
    return env("GCP_PROJECT_ID", required=True)


def _dataset_raw() -> str:
    return env("GCP_DATASET_RAW", "local_bike_raw")


def _requete(sql: str) -> list:
    return list(bigquery_client().query(sql).result())


def _volumetrie(tables: Iterable[str]) -> dict[str, int]:
    """COUNT(*) exact par table, en un seul aller-retour.

    `__TABLES__.row_count` serait moins coûteux mais peut retarder d'un chargement :
    une recette d'idempotence ne peut pas se permettre une métadonnée en retard.
    """
    tables = list(tables)
    if not tables:
        return {}
    parties = [
        f"select '{t}' as tbl, count(*) as n "
        f"from `{_projet()}.{_dataset_raw()}.{SOURCE_SCHEMA}_{t}`"
        for t in tables
    ]
    return {ligne.tbl: int(ligne.n) for ligne in _requete(" union all ".join(parties))}


def _doublons_de_cle(table: str, cle: str | list[str]) -> int:
    colonnes = ", ".join(cle) if isinstance(cle, list) else cle
    sql = f"""
        select count(*) as n from (
            select {colonnes}
            from `{_projet()}.{_dataset_raw()}.{SOURCE_SCHEMA}_{table}`
            group by {colonnes}
            having count(*) > 1
        )
    """
    return int(_requete(sql)[0].n)


def _empreintes_de_chargement(tables: Iterable[str]) -> dict[str, str]:
    """Dernier `_dlt_load_id` par table : change si et seulement si la table a été rechargée."""
    tables = list(tables)
    parties = [
        f"select '{t}' as tbl, max(_dlt_load_id) as load_id "
        f"from `{_projet()}.{_dataset_raw()}.{SOURCE_SCHEMA}_{t}`"
        for t in tables
    ]
    return {ligne.tbl: str(ligne.load_id) for ligne in _requete(" union all ".join(parties))}


def _table_existe(dataset: str, table: str) -> bool:
    from google.cloud.exceptions import NotFound

    try:
        bigquery_client().get_table(f"{_projet()}.{dataset}.{table}")
        return True
    except NotFound:
        return False


# --------------------------------------------------------------------------- #
# F-01 / F-03 — extraction complète et idempotence du merge
# --------------------------------------------------------------------------- #

@scenario("F-01", "Extraction complète des 9 tables source", "🔴")
def f01(resultat: Resultat) -> None:
    volumetrie_bq = _volumetrie(SOURCE_TABLES)
    resultat.verifier(
        len(volumetrie_bq) == 9,
        f"les 9 tables `{SOURCE_SCHEMA}_*` sont présentes dans {_dataset_raw()} "
        f"({len(volumetrie_bq)} trouvée(s))",
    )

    from ingestion.load_supabase_to_bq import source_row_counts

    volumetrie_source = source_row_counts()
    for table in SOURCE_TABLES:
        attendu = volumetrie_source.get(f"{SOURCE_SCHEMA}_{table}")
        obtenu = volumetrie_bq.get(table)
        resultat.verifier(
            attendu == obtenu,
            f"{table} : Supabase {attendu} = BigQuery {obtenu}",
        )
    resultat.verifier(
        all(n > 0 for n in volumetrie_bq.values()),
        "aucune table brute n'est vide",
    )


@scenario("F-03", "Idempotence du chargement `merge`", "🔴")
def f03(resultat: Resultat) -> None:
    transactionnelles = list(MERGE_PRIMARY_KEYS)

    avant = _volumetrie(transactionnelles)
    empreintes_avant = _empreintes_de_chargement(transactionnelles)

    lancer_ingestion()  # seconde passe complète, sans aucune modification de la source

    apres = _volumetrie(transactionnelles)
    empreintes_apres = _empreintes_de_chargement(transactionnelles)

    for table in transactionnelles:
        resultat.verifier(
            avant[table] == apres[table],
            f"{table} : volumétrie inchangée après ré-ingestion "
            f"({avant[table]} -> {apres[table]})",
        )
        resultat.verifier(
            _doublons_de_cle(table, MERGE_PRIMARY_KEYS[table]) == 0,
            f"{table} : aucun doublon sur {MERGE_PRIMARY_KEYS[table]}",
        )

    # L'idempotence porte sur les DONNÉES, pas sur les métadonnées : un second run doit
    # bien avoir eu lieu, sinon le test passerait pour une mauvaise raison.
    resultat.verifier(
        any(empreintes_avant[t] != empreintes_apres[t] for t in transactionnelles),
        "un second chargement dlt a bien été exécuté (_dlt_load_id renouvelé)",
    )


# --------------------------------------------------------------------------- #
# F-04 — rafraîchissement du chargement replace
# --------------------------------------------------------------------------- #

@scenario("F-04", "Rafraîchissement du chargement `replace`", "🔴")
def f04(resultat: Resultat) -> None:
    from sqlalchemy import create_engine, text

    moteur = create_engine(build_source_uri())
    cle = "brand_id"
    colonne = "brand_name"

    with moteur.connect() as connexion:
        ligne = connexion.execute(
            text(f"select {cle}, {colonne} from {SOURCE_SCHEMA}.{TABLE_COBAYE} order by {cle} limit 1")
        ).one()
    identifiant, libelle_initial = ligne[0], ligne[1]

    lignes_avant = _volumetrie([TABLE_COBAYE])[TABLE_COBAYE]

    try:
        with moteur.begin() as connexion:
            connexion.execute(
                text(
                    f"update {SOURCE_SCHEMA}.{TABLE_COBAYE} set {colonne} = :v where {cle} = :k"
                ),
                {"v": LIBELLE_RECETTE, "k": identifiant},
            )

        lancer_ingestion(only=[TABLE_COBAYE])

        obtenu = _requete(
            f"select {colonne} as v from `{_projet()}.{_dataset_raw()}."
            f"{SOURCE_SCHEMA}_{TABLE_COBAYE}` where {cle} = {identifiant}"
        )[0].v
        resultat.verifier(
            obtenu == LIBELLE_RECETTE,
            f"la modification source est répercutée ({libelle_initial!r} -> {obtenu!r})",
        )
        resultat.verifier(
            _volumetrie([TABLE_COBAYE])[TABLE_COBAYE] == lignes_avant,
            f"{TABLE_COBAYE} : remplacement complet, pas d'accumulation "
            f"({lignes_avant} lignes)",
        )
    finally:
        # Remise en état, même si une assertion a échoué au-dessus.
        with moteur.begin() as connexion:
            connexion.execute(
                text(f"update {SOURCE_SCHEMA}.{TABLE_COBAYE} set {colonne} = :v where {cle} = :k"),
                {"v": libelle_initial, "k": identifiant},
            )
        lancer_ingestion(only=[TABLE_COBAYE])
        retabli = _requete(
            f"select {colonne} as v from `{_projet()}.{_dataset_raw()}."
            f"{SOURCE_SCHEMA}_{TABLE_COBAYE}` where {cle} = {identifiant}"
        )[0].v
        resultat.verifier(
            retabli == libelle_initial,
            f"état initial rétabli des deux côtés ({retabli!r})",
        )


# --------------------------------------------------------------------------- #
# F-05 — ingestion ciblée
# --------------------------------------------------------------------------- #

@scenario("F-05", "Ingestion ciblée sur une table", "🟠")
def f05(resultat: Resultat) -> None:
    cible = "customers"
    avant = _empreintes_de_chargement(SOURCE_TABLES)

    lancer_ingestion(only=[cible])

    apres = _empreintes_de_chargement(SOURCE_TABLES)

    resultat.verifier(
        avant[cible] != apres[cible],
        f"{cible} : rechargée (nouveau _dlt_load_id)",
    )
    intactes = [t for t in SOURCE_TABLES if t != cible and avant[t] == apres[t]]
    resultat.verifier(
        len(intactes) == 8,
        f"les 8 autres tables sont intactes ({len(intactes)}/8)",
    )


# --------------------------------------------------------------------------- #
# F-06 — échec explicite sur credential manquant
# --------------------------------------------------------------------------- #

@scenario("F-06", "Échec explicite sur credential manquant", "🟠")
def f06(resultat: Resultat) -> None:
    environnement = dict(os.environ)
    # Une variable présente mais vide : `load_dotenv` ne l'écrase pas (override=False),
    # le cas reproduit donc bien un `.env` incomplet.
    environnement["SUPABASE_PASSWORD"] = ""

    execution = subprocess.run(
        [sys.executable, "ingestion/load_supabase_to_bq.py"],
        cwd=RACINE,
        env=environnement,
        capture_output=True,
        text=True,
        timeout=300,
    )
    sortie = execution.stdout + execution.stderr

    resultat.verifier(
        execution.returncode != 0,
        f"code de sortie non nul ({execution.returncode})",
    )
    resultat.verifier(
        "SUPABASE_PASSWORD" in sortie,
        "le message nomme la variable manquante",
    )
    resultat.verifier(
        ".env.example" in sortie,
        "le message oriente vers la correction (.env.example)",
    )
    resultat.verifier(
        "Traceback" not in sortie,
        "arrêt maîtrisé, sans trace d'exception",
    )


# --------------------------------------------------------------------------- #
# F-11 — conservation du grain dans les marts
# --------------------------------------------------------------------------- #

@scenario("F-11", "Conservation du grain et exhaustivité des marts", "🔴")
def f11(resultat: Resultat) -> None:
    marts = env("GCP_DATASET_MARTS", "local_bike_marts")
    if not _table_existe(marts, "fct_order_items"):
        resultat.ignorer(
            f"dataset `{marts}` non construit — lancer `make build` avant cette campagne"
        )
        return

    projet, raw = _projet(), _dataset_raw()

    lignes_source = int(
        _requete(f"select count(*) as n from `{projet}.{raw}.{SOURCE_SCHEMA}_order_items`")[0].n
    )
    lignes_fait = int(
        _requete(f"select count(*) as n from `{projet}.{marts}.fct_order_items`")[0].n
    )
    resultat.verifier(
        lignes_source == lignes_fait,
        f"fct_order_items : {lignes_fait} lignes = {lignes_source} lignes source "
        "(aucune inflation de jointure)",
    )

    commandes_source = int(
        _requete(
            f"select count(distinct order_id) as n from `{projet}.{raw}.{SOURCE_SCHEMA}_orders`"
        )[0].n
    )
    commandes_fait = int(
        _requete(f"select count(*) as n from `{projet}.{marts}.fct_orders`")[0].n
    )
    resultat.verifier(
        commandes_source == commandes_fait,
        f"fct_orders : {commandes_fait} commandes = {commandes_source} commandes source",
    )


# --------------------------------------------------------------------------- #
# Exécution et rapport
# --------------------------------------------------------------------------- #

def executer(identifiants: list[str]) -> list[Resultat]:
    resultats: list[Resultat] = []
    for identifiant in identifiants:
        titre, criticite, fonction = SCENARIOS[identifiant]
        resultat = Resultat(identifiant, titre, criticite)
        print(f"\n{identifiant} — {titre} {criticite}")
        depart = time.monotonic()
        try:
            fonction(resultat)
        except Exception as erreur:  # noqa: BLE001 — un scénario en erreur ne doit pas
            resultat.verifier(False, f"exception pendant l'exécution : {erreur}")
        resultat.duree = time.monotonic() - depart

        for constat in resultat.constats:
            marque = f"{VERT}✓{RAZ}" if constat.ok else f"{ROUGE}✗{RAZ}"
            print(f"  {marque} {constat.texte}")
        if resultat.statut == "non joué":
            print(f"  {JAUNE}⊘{RAZ} non joué — {resultat.justification}")
        print(f"  {GRIS}{resultat.duree:.1f}s{RAZ}")
        resultats.append(resultat)
    return resultats


def ecrire_rapport(resultats: list[Resultat], chemin: Path) -> Path:
    horodatage = datetime.now().strftime("%d/%m/%Y %H:%M")
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=RACINE, capture_output=True, text=True, timeout=10,
        ).stdout.strip() or "inconnu"
    except Exception:  # noqa: BLE001
        commit = "inconnu"

    conformes = sum(r.statut == "conforme" for r in resultats)
    non_conformes = sum(r.statut == "non conforme" for r in resultats)
    non_joues = sum(r.statut == "non joué" for r in resultats)

    lignes = [
        "# Campagne fonctionnelle automatisée — Local Bike",
        "",
        "> Rapport généré par `recette/run_fonctionnels.py`. Il couvre les seuls scénarios",
        "> fonctionnels automatisables ; les scénarios manuels (alertes, dashboard, flux",
        "> temps réel) sont à consigner dans le procès-verbal de campagne",
        "> (`recette/resultats/_gabarit.md`).",
        "",
        "| Élément | Valeur |",
        "|---|---|",
        f"| Date | {horodatage} |",
        f"| Commit | `{commit}` |",
        f"| Dataset brut | `{_dataset_raw()}` |",
        f"| Conformes | {conformes} / {len(resultats)} |",
        f"| Non conformes | {non_conformes} |",
        f"| Non joués | {non_joues} |",
        "",
        "## Synthèse",
        "",
        "| ID | Scénario | Criticité | Statut | Durée |",
        "|---|---|---|---|---|",
    ]
    icones = {"conforme": "✅", "non conforme": "❌", "non joué": "⊘"}
    for r in resultats:
        lignes.append(
            f"| {r.identifiant} | {r.titre} | {r.criticite} | "
            f"{icones[r.statut]} {r.statut} | {r.duree:.1f}s |"
        )

    lignes += ["", "## Détail des constats", ""]
    for r in resultats:
        lignes.append(f"### {r.identifiant} — {r.titre}")
        lignes.append("")
        if r.statut == "non joué":
            lignes += [f"⊘ **Non joué** — {r.justification}", ""]
        for constat in r.constats:
            lignes.append(f"- {'✅' if constat.ok else '❌'} {constat.texte}")
        lignes.append("")

    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text("\n".join(lignes) + "\n", encoding="utf-8")
    return chemin


def main() -> int:
    analyseur = argparse.ArgumentParser(
        description="Campagne fonctionnelle automatisée du cahier de recettes."
    )
    analyseur.add_argument(
        "--scenarios",
        help="Sélection, séparée par des virgules (ex. F-03,F-06). Défaut : tous.",
    )
    analyseur.add_argument("--liste", action="store_true", help="Inventaire, sans exécution.")
    analyseur.add_argument(
        "--sans-rapport", action="store_true", help="Console uniquement, pas de fichier."
    )
    arguments = analyseur.parse_args()

    if arguments.liste:
        for identifiant, (titre, criticite, _) in SCENARIOS.items():
            print(f"{identifiant}  {criticite}  {titre}")
        return 0

    load_dotenv(RACINE / ".env")

    if arguments.scenarios:
        demandes = [s.strip().upper() for s in arguments.scenarios.split(",") if s.strip()]
        inconnus = [s for s in demandes if s not in SCENARIOS]
        if inconnus:
            print(f"Scénario(s) inconnu(s) : {', '.join(inconnus)}", file=sys.stderr)
            print(f"Disponibles : {', '.join(SCENARIOS)}", file=sys.stderr)
            return 2
        identifiants = demandes
    else:
        identifiants = list(SCENARIOS)

    print("Campagne fonctionnelle — cahier de recettes Local Bike")
    print(f"{len(identifiants)} scénario(s) : {', '.join(identifiants)}")

    resultats = executer(identifiants)

    conformes = sum(r.statut == "conforme" for r in resultats)
    non_conformes = [r for r in resultats if r.statut == "non conforme"]
    non_joues = [r for r in resultats if r.statut == "non joué"]

    print("\n" + "─" * 62)
    print(f"Conformes : {conformes}/{len(resultats)}")
    if non_joues:
        print(f"{JAUNE}Non joués : {', '.join(r.identifiant for r in non_joues)}{RAZ}")
    if non_conformes:
        print(f"{ROUGE}NON CONFORMES : {', '.join(r.identifiant for r in non_conformes)}{RAZ}")

    if not arguments.sans_rapport:
        chemin = DOSSIER_RESULTATS / (
            datetime.now().strftime("%Y-%m-%d_%H%M") + "_fonctionnels.md"
        )
        print(f"\nRapport : {ecrire_rapport(resultats, chemin).relative_to(RACINE)}")

    return 1 if non_conformes else 0


if __name__ == "__main__":
    sys.exit(main())
