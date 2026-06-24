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

### Pourquoi une brique d'orchestration ?

Sans orchestrateur, faire vivre le pipeline veut dire lancer **à la main**, dans le bon
ordre, `make ingest` → `make run` → `make test` — et recommencer chaque jour. Aucune
planification, aucune nouvelle tentative en cas d'échec, aucune visibilité sur *quel*
modèle a cassé, et aucune trace de fraîcheur des données.

**Dagster** résout exactement ça. Il transforme chaque table brute et chaque modèle dbt
en *asset* (un objet de données versionné et observable) et apporte :

- **Planification** : recharger les données et reconstruire les marts automatiquement (cf. *scheduler* ci-dessous).
- **Lineage continue** : un seul graphe Supabase → `local_bike_raw` → staging → intermediate → marts. On voit d'un coup d'œil ce qui alimente chaque table du dashboard.
- **Observabilité** : statut (vert/rouge), durée, logs et **résultats des tests dbt** par asset, dans une UI web.
- **Ré-exécution ciblée** : rejouer seulement une partie du DAG (ex. uniquement les marts après un changement de modèle) au lieu de tout reconstruire.
- **Robustesse** : retries, timeouts, et point d'accroche pour des alertes.

### Les briques en place

| Composant | Code | Rôle |
|---|---|---|
| Asset d'ingestion | [`raw_supabase_tables`](orchestration/ingestion.py) | `@multi_asset` qui réutilise `load_supabase_to_bq.py` et matérialise les 9 tables `local_bike_raw/public_*` |
| Assets dbt | [`local_bike_dbt_assets`](orchestration/dbt.py) | `@dbt_assets` : 1 modèle dbt = 1 asset ; un translator mappe les *sources* dbt sur les assets d'ingestion (DAG continu) |
| Job | [`local_bike_pipeline`](orchestration/definitions.py) | Sélectionne **tous** les assets (ingestion + dbt) |
| Schedule | [`daily_refresh`](orchestration/definitions.py) | Déclenche le job tous les jours à **05:00** (Europe/Paris) |

### Le scheduler : recharger les données et mettre à jour les marts

Le rafraîchissement quotidien est défini ici :

```python
# orchestration/definitions.py
local_bike_pipeline = define_asset_job(name="local_bike_pipeline", selection=AssetSelection.all())

daily_refresh = ScheduleDefinition(
    name="daily_refresh",
    job=local_bike_pipeline,
    cron_schedule="0 5 * * *",          # tous les jours à 05:00…
    execution_timezone="Europe/Paris",  # …heure de Paris
)
```

**Ce qu'un run planifié exécute, dans l'ordre du DAG :**

1. **Recharge les données brutes** — l'asset d'ingestion relance l'extraction Supabase et
   réécrit les 9 tables de `local_bike_raw` en **full refresh** (`WRITE_TRUNCATE`) : les
   données BigQuery reflètent l'état courant de la source.
2. **Reconstruit les transformations** — comme les assets dbt sont **en aval** de
   l'ingestion dans la lineage, Dagster enchaîne automatiquement : les vues `staging`,
   puis `intermediate`, puis les **tables `marts`** (`dim_*` / `fct_*`) sont recalculées
   à partir des données fraîches.
3. **Valide la qualité** — le job lance `dbt build` (et non `dbt run`), donc **les tests
   tournent juste après chaque modèle**. Un test rouge marque l'asset en échec et coupe la
   propagation en aval, plutôt que de livrer des marts douteuses au dashboard.

Résultat : chaque matin, Looker Studio lit des marts reconstruites et testées, sans
intervention manuelle.

> **Le daemon doit tourner.** Les schedules sont exécutés par le *daemon* Dagster, lancé
> en même temps que l'UI par `make dagster`. Si rien ne tourne, aucun déclenchement.
>
> **Un schedule démarre désactivé.** Après `make dagster`, va dans l'onglet *Automation*
> de l'UI et bascule `daily_refresh` sur **on**. Tu peux aussi lancer un run immédiat
> depuis *Assets → Materialize all*.
>
> **Changer la fréquence** = changer le `cron_schedule` (ex. `0 */6 * * *` = toutes les
> 6 h, `0 5 * * 1` = chaque lundi à 5 h).

### Lancer l'orchestrateur

```bash
make dagster          # UI + daemon en local sur http://127.0.0.1:3000
```

Dans l'UI :
- **Materialize all** → un run complet ingestion + dbt (équivaut au run planifié, à la demande).
- **Sélection + Materialize** → reconstruire un sous-ensemble. Ex. après avoir modifié un
  modèle de marts : sélectionner les assets `marts/*` et les matérialiser sans relancer
  l'ingestion ni le staging.

> L'UI Dagster écoute sur `127.0.0.1` (jamais `0.0.0.0`). L'état local (runs, historique
> des schedules) est stocké dans `.dagster_home/` (ignoré par git). Le code vit dans
> `orchestration/` et est découvert via `[tool.dagster]` de `pyproject.toml`.

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
