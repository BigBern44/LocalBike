# Local Bike — Pipeline ELT (Supabase → BigQuery → dbt → Looker Studio)

Pipeline data **ELT** pour Local Bike (chaîne de magasins de vélos, dataset *BikeStores*).
Objectif : fournir à l'équipe Opérations un premier tableau de bord data-driven pour
**optimiser les ventes** et **maximiser le revenu**.

> 📊 **Dashboard en ligne** : [Local Bike — Looker Studio](https://datastudio.google.com/reporting/c545b736-0247-4ae7-a23d-909e9cdeb3bd/page/p_22sfwlex4d?pli=1)
> _(voir la section [Dataviz](#-dataviz--dashboard-looker-studio) pour le détail des pages)._

---

# Partie 1 — Présentation & fonctionnement

## 1. Objectif

Local Bike dispose de ses données de ventes et de production dans une base **Supabase
(PostgreSQL)** mais d'**aucun moyen de les exploiter** pour le pilotage. Le rôle du Data
Engineer est de **modéliser** ces données brutes en indicateurs fiables et requêtables,
afin que l'équipe Opérations puisse répondre à des questions métier concrètes :

- Quel **revenu** par magasin / état / période ?
- Quels sont les **top produits / catégories / marques** ?
- Où sont les **ruptures de stock** et comment tourne l'inventaire ?
- Qui sont les **meilleurs clients** (panier moyen, fréquence, géographie) ?
- Quels sont les **délais de livraison** et le taux de retard ?
- Quelles **ventes** par vendeur / magasin ?

La livraison finale est un **tableau de bord Looker Studio** branché sur des tables prêtes
à l'emploi, plus un **pipeline reproductible et versionné** (peer-review GitHub).

## 2. Fonctionnement du pipeline

Le pipeline suit une logique **ELT** (Extract-Load-Transform) : on charge d'abord les
données brutes dans l'entrepôt (BigQuery), **puis** on les transforme avec dbt, couche
par couche, jusqu'à des tables directement lisibles par le dashboard.

### Schéma d'ensemble

```
   SOURCE                INGESTION (EL)            ENTREPÔT + TRANSFORMATION (dbt)                         RESTITUTION
 ┌──────────┐          ┌──────────────┐    ┌──────────────────────────────────────────────────┐       ┌──────────────┐
 │ Supabase │          │     dlt      │    │                   BigQuery                         │       │    Looker     │
 │ Postgres │  ──────► │ sql_database │──► │                                                    │  ───► │    Studio     │
 │          │  extract │  → BigQuery  │load│  raw ─► staging ─► intermediate ─► marts ─► reporting│ read  │  (dashboard)  │
 │ 9 tables │          │ replace/merge│    │  brut    nettoyé    enrichi      étoile    tables   │       │  1 page/axe   │
 │ (public) │          └──────────────┘    │  1:1     typé       revenu       dim/fct   plates   │       └──────────────┘
 └──────────┘                              └──────────────────────────────────────────────────┘
                                                            │
                                                  tests dbt (qualité) + docs
                                                            │
                                              Orchestration Dagster (planif. + lineage)
                                                            │
                                                   GitHub (PR / peer-review)
```

### Les étapes, en détail

| # | Étape | Outil | Ce qu'il se passe |
|---|-------|-------|-------------------|
| 1 | **Ingestion (EL)** | Python (`dlt`) | Extraction des 9 tables du schéma `public` de Supabase (SSL requis) → chargement **brut** dans BigQuery via dlt (source `sql_database`, destination `bigquery`). Idempotent : `merge` (upsert sur PK) pour customers/orders/order_items, `replace` (full refresh) pour les référentiels. |
| 2 | **`raw`** (`local_bike_raw`) | BigQuery | Copie **1:1** des tables source, nommées `public_<table>`. Aucune transformation : c'est le point de vérité brut. |
| 3 | **`staging`** (`local_bike_staging`) | dbt (`view`) | 1 modèle par table source : nettoyage, **renommage**, **typage explicite** (dates, numériques). |
| 4 | **`intermediate`** | dbt | Enrichissement des `order_items` (jointure produits) et calcul du **revenu** : `revenue = quantity * list_price * (1 - discount)`. |
| 5 | **`marts`** (`local_bike_marts`) | dbt (`table`) | **Modèle en étoile** : dimensions (`dim_*`) + faits (`fct_*`). Grain principal = la ligne de commande (`fct_order_items`). |
| 6 | **`reporting`** (`local_bike_reporting`) | dbt (`table`) | Tables **plates** (fait + dimensions aplatis), 1 par axe d'analyse, lues **directement** par Looker Studio (sans « blend »). |

### Pourquoi une couche `reporting` séparée des `marts` ?

Looker Studio ne lit pas confortablement le **modèle en étoile** : croiser un fait avec ses
dimensions y impose des « blends » (jointures côté BI) lents et fragiles. On expose donc une
couche `reporting` de tables **dénormalisées** (une par axe), que le dashboard requête
directement — **un graphique = une source, sans blend**.

### Orchestration (Dagster) — pourquoi ?

Sans orchestrateur, faire vivre le pipeline veut dire lancer **à la main**, dans le bon
ordre, l'ingestion → les transformations → les tests, et recommencer chaque jour. **Dagster**
transforme chaque table brute et chaque modèle dbt en *asset* (objet de données versionné et
observable) et apporte :

- **Planification** : un schedule quotidien (05:00 Europe/Paris) recharge les données et
  reconstruit les marts automatiquement.
- **Lineage continue** : un seul graphe Supabase → `raw` → staging → intermediate → marts.
- **Observabilité** : statut, durée, logs et **résultats des tests dbt** par asset, dans une UI web.
- **Ré-exécution ciblée** : rejouer seulement une partie du DAG (ex. les marts après un changement de modèle).

> Détails de mise en route de l'orchestrateur : voir [Installation technique → Orchestration](#orchestration-dagster).

## 3. Qualité des données (tests dbt)

La fiabilité du dashboard repose sur des tests qui tournent à **chaque couche** :

- **Tests génériques** : `unique`, `not_null`, `relationships` (intégrité référentielle),
  `accepted_values` (statuts de commande), `accepted_range` (quantités/prix/remises/revenus),
  `unique_combination_of_columns` (clés composites).
- **Tests singuliers (métier)** dans `tests/` :
  - `assert_shipped_after_order_date.sql` — une commande ne peut être expédiée avant d'être passée ;
  - `assert_order_revenue_reconciliation.sql` — `fct_orders.total_revenue` = somme des lignes de `fct_order_items` ;
  - `assert_revenue_formula.sql` — `revenue = quantity * list_price * (1 - discount)` (garde-fou de calcul).

Le run Dagster quotidien lance `dbt build` (et non `dbt run`) : **les tests tournent juste
après chaque modèle**, et un test rouge coupe la propagation en aval plutôt que de livrer
des marts douteuses au dashboard.

## 4. 📊 Dataviz — Dashboard Looker Studio

🔗 **Lien du tableau de bord** : <https://datastudio.google.com/reporting/c545b736-0247-4ae7-a23d-909e9cdeb3bd/page/p_22sfwlex4d?pli=1>

Le dashboard est branché sur le dataset `local_bike_reporting` (une source de données par
table `rpt_*`), avec **une page par axe d'analyse** :

| Page / axe | Table source | Indicateurs clés |
|------------|--------------|------------------|
| **Revenu & ventes** | [`rpt_sales`](models/marts/reporting/rpt_sales.sql) | Revenu par magasin / état / période, top produits / catégories / marques, clients, staff |
| **Commandes & livraison** | [`rpt_orders`](models/marts/reporting/rpt_orders.sql) | Délais (`shipped_date − order_date`), taux de retard vs `required_date`, statuts de commande, revenu/commande |
| **Stocks** | [`rpt_stocks`](models/marts/reporting/rpt_stocks.sql) | Niveau de stock par magasin, produits en rupture, valorisation |

Documentation + tests de ces tables : [`_reporting.yml`](models/marts/reporting/_reporting.yml).
Elles sont reconstruites et testées par `make build` (et par le run Dagster quotidien).

> Les étapes pour (re)connecter Looker Studio à BigQuery sont décrites dans
> [Installation technique → Connecter Looker Studio](#connecter-looker-studio).

---

# Partie 2 — Installation technique

## Stack

| Brique          | Outil                          |
|-----------------|--------------------------------|
| Source          | Supabase (PostgreSQL)          |
| Ingestion EL    | Python (`dlt` : Supabase → BigQuery) |
| Entrepôt        | Google BigQuery                |
| Transformation  | dbt (`dbt-bigquery`)           |
| Tests / docs    | dbt (`dbt test`, `dbt docs`)   |
| Orchestration   | Dagster (`dagster-dbt`)        |
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
make run             #    Construire les modèles (staging -> intermediate -> marts -> reporting)
make test            #    Lancer les tests
make build           #    run + test (construction + validation)
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

## Lancer les tests

```bash
make test                      # lance toute la suite de tests
make build                     # run + test (construction + validation)
# sélectif :
dbt test --profiles-dir . --select staging             # tests d'une couche
dbt test --profiles-dir . --select test_type:singular  # uniquement les tests métier
```

## Orchestration (Dagster)

### Les briques en place

| Composant | Code | Rôle |
|---|---|---|
| Asset d'ingestion | [`raw_supabase_tables`](orchestration/ingestion.py) | `@multi_asset` qui réutilise `load_supabase_to_bq.py` et matérialise les 9 tables `local_bike_raw/public_*` |
| Assets dbt | [`local_bike_dbt_assets`](orchestration/dbt.py) | `@dbt_assets` : 1 modèle dbt = 1 asset ; un translator mappe les *sources* dbt sur les assets d'ingestion (DAG continu) |
| Job | [`local_bike_pipeline`](orchestration/definitions.py) | Sélectionne **tous** les assets (ingestion + dbt) |
| Schedule | [`daily_refresh`](orchestration/definitions.py) | Déclenche le job tous les jours à **05:00** (Europe/Paris) |

### Lancer l'orchestrateur

```bash
make dagster          # UI + daemon en local sur http://127.0.0.1:3000
```

Dans l'UI :
- **Materialize all** → un run complet ingestion + dbt (équivaut au run planifié, à la demande).
- **Sélection + Materialize** → reconstruire un sous-ensemble. Ex. après avoir modifié un
  modèle de marts : sélectionner les assets `marts/*` et les matérialiser sans relancer
  l'ingestion ni le staging.

> **Le daemon doit tourner.** Les schedules sont exécutés par le *daemon* Dagster, lancé
> en même temps que l'UI par `make dagster`. Si rien ne tourne, aucun déclenchement.
>
> **Un schedule démarre désactivé.** Après `make dagster`, va dans l'onglet *Automation*
> de l'UI et bascule `daily_refresh` sur **on**. Tu peux aussi lancer un run immédiat
> depuis *Assets → Materialize all*.
>
> **Changer la fréquence** = changer le `cron_schedule` (ex. `0 */6 * * *` = toutes les
> 6 h, `0 5 * * 1` = chaque lundi à 5 h).
>
> L'UI Dagster écoute sur `127.0.0.1` (jamais `0.0.0.0`). L'état local (runs, historique
> des schedules) est stocké dans `.dagster_home/` (ignoré par git). Le code vit dans
> `orchestration/` et est découvert via `[tool.dagster]` de `pyproject.toml`.

## Connecter Looker Studio

1. [Looker Studio](https://lookerstudio.google.com) → **Créer → Source de données → BigQuery**
   (avec un compte ayant accès au projet GCP).
2. **Mon projet** → `GCP_PROJECT_ID` → dataset **`local_bike_reporting`** → choisir
   `rpt_sales`, `rpt_orders` ou `rpt_stocks` (une source de données par table).
3. **Connecter**, puis vérifier les types (date sur `order_date` / `required_date` /
   `shipped_date`, métrique sur `revenue` / `total_revenue` / `stock_value`).
4. Construire une **page par axe** : ex. *Revenu* = série temporelle `order_year_month` × `revenue`,
   filtres `store_state` / `brand_name` ; *Livraison* = taux `is_late` par magasin ; *Stocks* =
   table `rpt_stocks` filtrée sur `is_out_of_stock`.

> Le compte de service dbt écrit `local_bike_reporting` ; pour le dashboard, donner un accès
> **lecture** BigQuery au(x) compte(s) Looker Studio (rôle *BigQuery Data Viewer* + *Job User*).
> Penser à **partager le rapport** Looker Studio à l'équipe Opérations.

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
│   └── marts/          #   dim_*/fct_*.sql + _marts.yml (star schema)
│       └── reporting/  #     rpt_*.sql + _reporting.yml (tables plates -> Looker Studio)
├── tests/              # tests singuliers (métier)
├── secrets/            # credentials locaux (ignoré par git)
├── .env.example        # variables d'environnement attendues
├── dbt_project.yml     # config dbt
├── pyproject.toml      # point d'entrée Dagster ([tool.dagster])
└── requirements.txt
```

> Détails métier, axes d'analyse et schéma en étoile : voir `CLAUDE.md`.
