# Local Bike — Pipeline ELT (Supabase → BigQuery → dbt → Looker Studio)

Pipeline data **ELT** pour Local Bike (chaîne de magasins de vélos, dataset *BikeStores*).
Objectif : fournir à l'équipe Opérations un premier tableau de bord pour **optimiser les ventes**
et **maximiser le revenu**.

```
Supabase (PostgreSQL)      BigQuery                          dbt                         Dashboard
 schéma public (9 tbl) ─► local_bike_raw (brut 1:1)  ──►  staging ─► intermediate ─► marts  ──►  Looker Studio
```

## Stack

| Brique          | Outil                          |
|-----------------|--------------------------------|
| Source          | Supabase (PostgreSQL)          |
| Ingestion EL    | Python (`polars` + ADBC → Parquet → BigQuery) |
| Entrepôt        | Google BigQuery                |
| Transformation  | dbt (`dbt-bigquery`)           |
| Tests / docs    | dbt (`dbt test`, `dbt docs`)   |
| Dashboard       | Looker Studio                  |
| Versioning      | Git / GitHub                   |

## Prérequis

- Python 3.12 (voir `.python-version`)
- Un projet GCP avec BigQuery activé + un service account (rôles *BigQuery Data Editor* + *Job User*)
- Une base Supabase accessible via le **Connection Pooler** (SSL requis)

## Installation

```bash
# venv + dépendances (uv)
uv venv .venv && source .venv/bin/activate
uv pip install -r requirements.txt

# configuration : copier l'exemple et renseigner les valeurs
cp .env.example .env
#   -> renseigner SUPABASE_PASSWORD, GCP_PROJECT_ID, ...
#   -> créer le dossier secrets/ (ignoré par git) et y déposer la clé :
mkdir -p secrets   # puis copier la clé service account dans secrets/gcp-sa.json
```

### Configuration Supabase (`.env`)

On se connecte via le **Connection Pooler** (Session mode, port `5432`) — pas la connexion
directe `db.<ref>.supabase.co` qui est IPv6-only.

```ini
SUPABASE_HOST=aws-1-eu-west-1.pooler.supabase.com   # ⚠️ cluster "aws-1" (pas "aws-0")
SUPABASE_PORT=5432
SUPABASE_USER=postgres.<project_ref>                # format pooler : postgres.<ref>
SUPABASE_PASSWORD=<mot de passe>                     # uniquement dans .env (ignoré par git)
SUPABASE_DB=postgres
SUPABASE_SSLMODE=require
```

> Astuce : si tu obtiens `tenant/user ... not found`, c'est que le host ne pointe pas sur le bon
> cluster pooler — vérifier `aws-0` vs `aws-1` et la région dans le dashboard Supabase
> (Settings → Database → Connection pooling).

### Configuration BigQuery (`.env`)

```ini
GCP_PROJECT_ID=<ton-project-id>
GCP_DATASET_RAW=local_bike_raw
GCP_LOCATION=EU
GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp-sa.json
```

## Utilisation

```bash
# 1. Ingestion : Supabase (schéma public) -> BigQuery (dataset local_bike_raw)
python ingestion/load_supabase_to_bq.py            # les 9 tables
python ingestion/load_supabase_to_bq.py customers  # une seule table

# 2. Transformations dbt
dbt deps          # packages (dbt_utils)
dbt debug         # vérifier la connexion BigQuery
dbt run           # construire staging -> intermediate -> marts
dbt test          # lancer les tests
dbt docs generate && dbt docs serve
```

Les 9 tables source vivent dans le schéma **`public`** de Supabase
(`brands`, `categories`, `customers`, `order_items`, `orders`, `products`, `staffs`, `stocks`, `stores`)
et sont chargées dans `local_bike_raw` sous les noms `public_<table>` (ex. `public_customers`).

## Sécurité

- Aucun secret en clair dans le code ni dans git : tout passe par `.env` (ignoré).
- La clé service account vit dans `secrets/gcp-sa.json` (dossier `secrets/` entièrement ignoré).
- Connexion Supabase en `sslmode=require`.

## Structure

```
.
├── ingestion/          # script d'extraction/chargement Supabase -> BigQuery
├── models/             # modèles dbt (staging / intermediate / marts)
├── secrets/            # credentials locaux (ignoré par git)
├── .env.example        # variables d'environnement attendues
├── dbt_project.yml     # config dbt
└── requirements.txt
```

> Détails métier, axes d'analyse et schéma en étoile : voir `CLAUDE.md`.
