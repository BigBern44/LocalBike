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
make install        # = uv venv .venv  +  uv pip install -r requirements.txt

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

Le **Makefile** charge `.env` automatiquement et passe `--profiles-dir .` à dbt
(les commandes utilisent les binaires du venv, pas besoin de l'activer) :

```bash
make help            # liste toutes les commandes

make ingest          # 1. Ingestion Supabase (public) -> BigQuery (local_bike_raw)
make deps            # 2. Packages dbt (dbt_utils)
make debug           #    Vérifier la connexion BigQuery
make run             #    Construire les modèles (staging -> intermediate -> marts)
make test            #    Lancer les tests
make docs            #    Générer + servir la doc dbt
make dagster         #    Orchestrateur : UI Dagster sur http://127.0.0.1:3000
```

> ⚠️ dbt **ne lit pas `.env`** tout seul. Si tu lances dbt à la main (sans `make`),
> charge d'abord les variables et passe le dossier de profil :
>
> ```bash
> source .venv/bin/activate
> set -a && source .env && set +a
> dbt run --profiles-dir .
> ```

Les 9 tables source vivent dans le schéma **`public`** de Supabase
(`brands`, `categories`, `customers`, `order_items`, `orders`, `products`, `staffs`, `stocks`, `stores`)
et sont chargées dans `local_bike_raw` sous les noms `public_<table>` (ex. `public_customers`).

## Qualité des données (tests dbt)

Chaque couche est documentée **et** testée via son fichier YAML
(`_stg_local_bike.yml`, `_int_local_bike.yml`, `_marts.yml`) :

- **Tests génériques** : `unique`, `not_null`, `relationships` (intégrité référentielle
  entre tables), `accepted_values` (statuts de commande), `accepted_range`
  (quantités/prix/remises/revenus), `unique_combination_of_columns` (clés composites).
- **Tests singuliers (métier)** dans `tests/` :
  - `assert_shipped_after_order_date.sql` — une commande ne peut être expédiée avant d'être passée ;
  - `assert_order_revenue_reconciliation.sql` — `fct_orders.total_revenue` égale la somme des lignes de `fct_order_items` ;
  - `assert_revenue_formula.sql` — `revenue = quantity * list_price * (1 - discount)` (garde-fou de calcul).

```bash
make test                      # lance toute la suite de tests
make build                     # run + test (construction + validation)
# sélectif :
dbt test --profiles-dir . --select staging          # tests d'une couche
dbt test --profiles-dir . --select test_type:singular  # uniquement les tests métier
```

## Orchestration (Dagster)

Le pipeline complet (ingestion → staging → intermediate → marts → tests) est orchestré
avec **Dagster** (`dagster-dbt`). Chaque table brute et chaque modèle dbt devient un
*asset*, ce qui donne **une lineage continue** et une UI d'observabilité.

- **Ingestion** : un `@multi_asset` réutilise `ingestion/load_supabase_to_bq.py` et
  matérialise les 9 tables `local_bike_raw/public_*`.
- **dbt** : `@dbt_assets` charge tous les modèles ; un translator custom mappe les
  *sources* dbt sur les assets d'ingestion (le DAG est donc continu, sans rupture).
- **Job + schedule** : `local_bike_pipeline` (tous les assets) rafraîchi tous les jours
  à 05:00 (Europe/Paris) via `dbt build` (run + tests).

```bash
make dagster          # UI + daemon en local sur http://127.0.0.1:3000
```

> L'UI Dagster écoute sur `127.0.0.1` (jamais `0.0.0.0`). L'état local (runs, schedules)
> est stocké dans `.dagster_home/` (ignoré par git). Le code vit dans `orchestration/`
> et est découvert via `[tool.dagster]` de `pyproject.toml`.

## Sécurité

- Aucun secret en clair dans le code ni dans git : tout passe par `.env` (ignoré).
- La clé service account vit dans `secrets/gcp-sa.json` (dossier `secrets/` entièrement ignoré).
- Connexion Supabase en `sslmode=require`.

## Structure

```
.
├── ingestion/          # script d'extraction/chargement Supabase -> BigQuery
├── orchestration/      # code Dagster (assets ingestion + dbt, job, schedule)
├── models/             # modèles dbt + YAML doc/tests par couche
│   ├── staging/        #   stg_*.sql + _src/_stg_local_bike.yml
│   ├── intermediate/   #   int_*.sql + _int_local_bike.yml
│   └── marts/          #   dim_*/fct_*.sql + _marts.yml
├── tests/              # tests singuliers (métier)
├── secrets/            # credentials locaux (ignoré par git)
├── .env.example        # variables d'environnement attendues
├── dbt_project.yml     # config dbt
├── pyproject.toml      # point d'entrée Dagster ([tool.dagster])
└── requirements.txt
```

> Détails métier, axes d'analyse et schéma en étoile : voir `CLAUDE.md`.
