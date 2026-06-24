# CLAUDE.md — Projet Local Bike (Data Engineering)

> Fichier d'instructions pour Claude Code. Lis-le entièrement avant toute action.
> Le but : construire un pipeline ELT **Supabase → BigQuery → dbt → Dashboard**, versionné sur GitHub pour peer-review.

---

## 1. Contexte & objectif

Local Bike (chaîne de magasins de vélos, données = dataset *BikeStores*) veut son **premier tableau de bord data-driven**. Notre rôle (Data Engineer) : modéliser les données pour aider l'**équipe Opérations** à **optimiser les ventes** et **maximiser le revenu**.

Le dataset source vit dans une base **Supabase (PostgreSQL)**. Les 9 tables BikeStores sont toutes dans le schéma **`public`** (regroupables logiquement en un domaine *ventes* et un domaine *production*).

**Le pipeline doit reprendre l'ensemble des éléments vus en TD** (ingestion, sources dbt, staging, marts, tests, docs, GitHub).

---

## 2. Architecture cible

```
Supabase (PostgreSQL)         BigQuery                         dbt                          Dashboard
 schéma public (9 tbl) ──►   dataset RAW (brut 1:1)   ──►   staging ─► intermediate ─► marts   ──►  Looker Studio
                                                                 │
                                                       tests (génériques + singuliers)
                                                       docs (descriptions + dbt docs)
                                                                 │
                                                         GitHub (PR / peer-review)
```

Couches BigQuery :
- `local_bike_raw` : copie brute des tables source (aucune transfo), nommées `public_<table>`
- `local_bike_staging` : nettoyage / renommage / typage (1 modèle = 1 source)
- `local_bike_marts` : modèle en étoile (dimensions + faits) exposé aux dashboards

---

## 3. Stack technique

| Brique        | Outil                                  |
|---------------|----------------------------------------|
| Source        | Supabase (PostgreSQL)                  |
| Ingestion EL  | Python (`polars` + ADBC → Parquet → BigQuery) |
| Entrepôt      | Google BigQuery                        |
| Transformation| dbt (adapter `dbt-bigquery`)           |
| Tests / docs  | dbt (`dbt test`, `dbt docs`)           |
| Dashboard     | Looker Studio (connecté aux marts)     |
| Versioning    | Git / GitHub                           |

---

## 4. ⚠️ Sécurité — À RESPECTER ABSOLUMENT

- **Aucun secret en clair dans le code ni dans Git.** Le mot de passe Supabase et la clé GCP passent par un fichier `.env` **listé dans `.gitignore`**.
- La clé service account BigQuery (`.json`) ne doit JAMAIS être commitée.
- Fournir un `.env.example` avec les noms de variables mais sans valeurs.
- Avant chaque `git add`, vérifier qu'aucun credential ne part dans le commit.
- **Supabase impose le SSL** : la connexion doit utiliser `sslmode=require`.
- Connexion via le **Connection Pooler** (Session mode, port 5432) : host `aws-<n>-<region>.pooler.supabase.com` (attention au cluster `aws-0` vs `aws-1`), user `postgres.<project_ref>`.

Variables d'environnement attendues :

```
SUPABASE_HOST=aws-1-eu-west-1.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_USER=postgres.<project_ref>
SUPABASE_PASSWORD=
SUPABASE_DB=postgres
SUPABASE_SSLMODE=require
GCP_PROJECT_ID=
GCP_DATASET_RAW=local_bike_raw
GCP_LOCATION=EU
GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp-sa.json
```

---

## 5. Modèle de données source

> Les 9 tables sont physiquement dans le schéma `public` de Supabase. On les regroupe ci-dessous en deux domaines **logiques** (ventes / production).

### Domaine ventes
- **customers** : `customer_id` (PK), first_name, last_name, phone, email, street, city, state, zip_code
- **orders** : `order_id` (PK), customer_id (FK), order_status, order_date, required_date, shipped_date, store_id (FK), staff_id (FK)
- **order_items** : (`order_id`, `item_id`) (PK), product_id (FK), quantity, list_price, discount
- **staffs** : `staff_id` (PK), first_name, last_name, email, phone, active, store_id (FK), manager_id (FK auto-référence)
- **stores** : `store_id` (PK), store_name, phone, email, street, city, state, zip_code

### Domaine production
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

> Source unique = dataset BigQuery `local_bike_raw` (tables `public_<table>`).

```
models/
├── staging/
│   ├── _src_local_bike.yml                 # source -> local_bike_raw (tables public_*)
│   ├── stg_customers.sql
│   ├── stg_orders.sql
│   ├── stg_order_items.sql
│   ├── stg_staffs.sql
│   ├── stg_stores.sql
│   ├── stg_categories.sql
│   ├── stg_products.sql
│   ├── stg_stocks.sql
│   └── stg_brands.sql
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
- Staging : `stg_<table>` (matérialisé en `view`), 1 modèle par table source.
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
2. **Ingestion** : script Python (`polars` + ADBC) qui extrait les 9 tables Supabase (schéma `public`, SSL requis) et les charge brutes dans `local_bike_raw`. Idempotent (full refresh).
3. **Init dbt** : `profiles.yml` BigQuery, source `_src_local_bike.yml` pointant vers `local_bike_raw`.
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
python ingestion/load_supabase_to_bq.py

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
