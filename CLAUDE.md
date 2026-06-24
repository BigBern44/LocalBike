# CLAUDE.md — Projet Local Bike (Data Engineering)

> Fichier d'instructions pour Claude Code. Lis-le entièrement avant toute action.
> Le but : construire un pipeline ELT **Supabase → BigQuery → dbt → Dashboard**, versionné sur GitHub pour peer-review.

---

## 1. Contexte & objectif

Local Bike (chaîne de magasins de vélos, données = dataset *BikeStores*) veut son **premier tableau de bord data-driven**. Notre rôle (Data Engineer) : modéliser les données pour aider l'**équipe Opérations** à **optimiser les ventes** et **maximiser le revenu**.

Le dataset source vit dans une base **Neon (PostgreSQL serverless)** avec deux schémas : `sales` et `production`.

**Le pipeline doit reprendre l'ensemble des éléments vus en TD** (ingestion, sources dbt, staging, marts, tests, docs, GitHub).

---

## 2. Architecture cible

```
Neon (PostgreSQL)             BigQuery                         dbt                          Dashboard
 sales + production    ──►   dataset RAW (brut 1:1)   ──►   staging ─► intermediate ─► marts   ──►  Looker Studio
                                                                 │
                                                       tests (génériques + singuliers)
                                                       docs (descriptions + dbt docs)
                                                                 │
                                                         GitHub (PR / peer-review)
```

Couches BigQuery :
- `local_bike_raw` : copie brute des tables source (aucune transfo)
- `local_bike_staging` : nettoyage / renommage / typage (1 modèle = 1 source)
- `local_bike_marts` : modèle en étoile (dimensions + faits) exposé aux dashboards

---

## 3. Stack technique

| Brique        | Outil                                  |
|---------------|----------------------------------------|
| Source        | Neon / PostgreSQL serverless           |
| Ingestion EL  | Python (`dlt` recommandé, ou script `psycopg2`/`pandas` → `pandas-gbq`) |
| Entrepôt      | Google BigQuery                        |
| Transformation| dbt (adapter `dbt-bigquery`)           |
| Tests / docs  | dbt (`dbt test`, `dbt docs`)           |
| Dashboard     | Looker Studio (connecté aux marts)     |
| Versioning    | Git / GitHub                           |

---

## 4. ⚠️ Sécurité — À RESPECTER ABSOLUMENT

- **Aucun secret en clair dans le code ni dans Git.** Le mot de passe Neon et la clé GCP passent par un fichier `.env` **listé dans `.gitignore`**.
- La clé service account BigQuery (`.json`) ne doit JAMAIS être commitée.
- Fournir un `.env.example` avec les noms de variables mais sans valeurs.
- Avant chaque `git add`, vérifier qu'aucun credential ne part dans le commit.
- **Neon impose le SSL** : la connexion doit utiliser `sslmode=require`.

Variables d'environnement attendues :

```
NEON_HOST=ep-broad-star-asg2hlgh.c-4.eu-central-1.aws.neon.tech
NEON_PORT=5432
NEON_USER=neondb_owner
NEON_PASSWORD=
NEON_DB=neondb
NEON_SSLMODE=require
GCP_PROJECT_ID=
GCP_DATASET_RAW=local_bike_raw
GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp-sa.json
```

---

## 5. Modèle de données source

### Schéma `sales`
- **customers** : `customer_id` (PK), first_name, last_name, phone, email, street, city, state, zip_code
- **orders** : `order_id` (PK), customer_id (FK), order_status, order_date, required_date, shipped_date, store_id (FK), staff_id (FK)
- **order_items** : (`order_id`, `item_id`) (PK), product_id (FK), quantity, list_price, discount
- **staffs** : `staff_id` (PK), first_name, last_name, email, phone, active, store_id (FK), manager_id (FK auto-référence)
- **stores** : `store_id` (PK), store_name, phone, email, street, city, state, zip_code

### Schéma `production`
- **categories** : `category_id` (PK), category_name
- **products** : `product_id` (PK), product_name, brand_id (FK), category_id (FK), model_year, list_price
- **stocks** : (`store_id`, `product_id`) (PK), quantity
- **brands** : `brand_id` (PK), brand_name

### Règle de calcul clé
Revenu d'une ligne de commande :
```
revenue = quantity * list_price * (1 - discount)
```
Le grain de la table de faits principale est **`order_items`**.

---

## 6. Structure du projet dbt attendue

```
models/
├── staging/
│   ├── sales/
│   │   ├── _sales__sources.yml
│   │   ├── stg_sales__customers.sql
│   │   ├── stg_sales__orders.sql
│   │   ├── stg_sales__order_items.sql
│   │   ├── stg_sales__staffs.sql
│   │   └── stg_sales__stores.sql
│   └── production/
│       ├── _production__sources.yml
│       ├── stg_production__categories.sql
│       ├── stg_production__products.sql
│       ├── stg_production__stocks.sql
│       └── stg_production__brands.sql
├── intermediate/
│   └── int_order_items_enriched.sql        # order_items + produit + revenu calculé
└── marts/
    ├── dim_customers.sql
    ├── dim_products.sql                     # products + categories + brands
    ├── dim_stores.sql
    ├── dim_staffs.sql
    ├── dim_date.sql
    ├── fct_order_items.sql                  # grain ligne — revenu
    ├── fct_orders.sql                       # grain commande (entête)
    ├── fct_stocks.sql                       # snapshot inventaire
    └── _marts.yml                           # tests + descriptions
```

---

## 7. Conventions

**Nommage**
- Staging : `stg_<schema>__<table>` (matérialisé en `view`)
- Intermediate : `int_<sujet>_<verbe>` (`ephemeral` ou `view`)
- Marts : `dim_<entité>` et `fct_<process>` (matérialisé en `table`)
- Clés : suffixe `_id`. Garder les noms source en staging, harmoniser en marts.

**SQL**
- CTE en `snake_case`, une CTE finale `select` propre.
- Pas de `SELECT *` dans les marts.
- Typage et casting explicites dans le staging (dates, numériques).

**dbt_project.yml** : staging en `view`, marts en `table`, dataset cible par couche.

---

## 8. Workflow / étapes à exécuter

1. **Setup** : projet GCP + datasets BigQuery, service account, repo Git, `.gitignore`, `.env`, venv Python.
2. **Ingestion** : script/`dlt` qui extrait les 9 tables Neon (SSL requis) et les charge brutes dans `local_bike_raw`. Idempotent (full refresh ou merge).
3. **Init dbt** : `profiles.yml` BigQuery, `sources.yml` pointant vers `local_bike_raw`.
4. **Staging** : 1 modèle par table source (nettoyage, renommage, typage).
5. **Intermediate** : enrichir `order_items` (jointure produits, calcul `revenue`).
6. **Marts** : dimensions + faits (modèle en étoile).
7. **Tests** : génériques (`unique`, `not_null`, `relationships`, `accepted_values`) + singuliers métier (`revenue >= 0`, `discount between 0 and 1`, `shipped_date >= order_date`).
8. **Docs** : descriptions des modèles/colonnes des marts, puis `dbt docs generate`.
9. **Dashboard** : connecter Looker Studio aux marts, construire les vues par axe d'analyse.
10. **GitHub** : README clair, push, Pull Request pour peer-review.

---

## 9. Axes d'analyse cibles (livrable métier)

Modéliser pour permettre à l'équipe Opérations de répondre à :
1. **Revenu** par magasin / état / période (jour, mois, année).
2. **Top produits / catégories / marques** par revenu et volume.
3. **Stocks** : niveau par magasin, produits en rupture, rotation.
4. **Clients** : panier moyen, fréquence, RFM, rétention, géographie.
5. **Livraison** : délais (`shipped_date` − `order_date`), taux de retard vs `required_date`, répartition des `order_status`.
6. **Staff** : ventes générées par vendeur / magasin.

Chaque axe doit être servi par une table de marts directement requêtable par le dashboard.

---

## 10. Commandes utiles

```bash
# Ingestion
python ingestion/load_neon_to_bq.py

# dbt
dbt debug                 # vérifier la connexion BigQuery
dbt deps                  # packages éventuels (dbt_utils)
dbt run                   # construire tous les modèles
dbt run --select staging  # une couche
dbt test                  # lancer tous les tests
dbt docs generate && dbt docs serve

# Git
git add . && git commit -m "feat: ..." && git push
```

---

## 11. Règles pour Claude Code

- Toujours travailler par petites étapes vérifiables ; lancer `dbt run`/`dbt test` après chaque couche.
- Ne jamais hardcoder de credential : lire depuis l'environnement.
- Ne pas inventer de colonnes : se référer au modèle source de la section 5.
- Documenter chaque modèle de marts (description + tests) car ils alimentent le dashboard.
- Proposer un commit Git atomique à la fin de chaque étape majeure.
