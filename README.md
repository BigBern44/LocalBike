# Local Bike — Pipeline ELT (Neon → BigQuery → dbt → Looker Studio)

Pipeline data **ELT** pour Local Bike (chaîne de magasins de vélos, dataset *BikeStores*).
Objectif : fournir à l'équipe Opérations un premier tableau de bord pour **optimiser les ventes**
et **maximiser le revenu**.

```
Neon (PostgreSQL)          BigQuery                          dbt                         Dashboard
 sales + production  ──►  local_bike_raw (brut 1:1)  ──►  staging ─► intermediate ─► marts  ──►  Looker Studio
```

## Stack

| Brique          | Outil                          |
|-----------------|--------------------------------|
| Source          | Neon / PostgreSQL serverless   |
| Ingestion EL    | Python (`psycopg2` + `pandas-gbq`) |
| Entrepôt        | Google BigQuery                |
| Transformation  | dbt (`dbt-bigquery`)           |
| Tests / docs    | dbt (`dbt test`, `dbt docs`)   |
| Dashboard       | Looker Studio                  |
| Versioning      | Git / GitHub                   |

## Prérequis

- Python 3.12 (voir `.python-version`)
- Un projet GCP avec BigQuery activé + un service account (rôle *BigQuery Data Editor* + *Job User*)
- Une base Neon accessible (SSL requis)

## Installation

```bash
# venv + dépendances (uv)
uv venv .venv && source .venv/bin/activate
uv pip install -r requirements.txt

# configuration : copier l'exemple et renseigner les valeurs
cp .env.example .env
#   -> renseigner NEON_PASSWORD, GCP_PROJECT_ID, ...
#   -> créer le dossier secrets/ (ignoré par git) et y déposer la clé :
mkdir -p secrets   # puis copier la clé service account dans secrets/gcp-sa.json
```

## Utilisation

```bash
# 1. Ingestion : Neon -> BigQuery (dataset local_bike_raw)
python ingestion/load_neon_to_bq.py

# 2. Transformations dbt
dbt deps          # packages (dbt_utils)
dbt debug         # vérifier la connexion BigQuery
dbt run           # construire staging -> intermediate -> marts
dbt test          # lancer les tests
dbt docs generate && dbt docs serve
```

## Sécurité

- Aucun secret en clair dans le code ni dans git : tout passe par `.env` (ignoré).
- La clé service account vit dans `secrets/gcp-sa.json` (dossier `secrets/` entièrement ignoré).
- Connexion Neon en `sslmode=require`.

## Structure

```
.
├── ingestion/          # script d'extraction/chargement Neon -> BigQuery
├── models/             # modèles dbt (staging / intermediate / marts)
├── secrets/            # credentials locaux (ignoré par git)
├── .env.example        # variables d'environnement attendues
├── dbt_project.yml     # config dbt
└── requirements.txt
```

> Détails métier, axes d'analyse et schéma en étoile : voir `CLAUDE.md`.
