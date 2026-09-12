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
- **Unit tests** sur `int_order_items_enriched` : la formule de revenu est vérifiée sur des
  entrées choisies (remise standard, bornes 0 et 1, arrondi, produit non rattaché). Ils
  valident la *logique SQL* là où les tests ci-dessus valident la *donnée produite*, et
  `dbt build` les joue **avant** de construire le modèle.
- **Tests singuliers (métier)** dans `tests/` :
  - `assert_shipped_after_order_date.sql` — une commande ne peut être expédiée avant d'être passée ;
  - `assert_order_revenue_reconciliation.sql` — `fct_orders.total_revenue` = somme des lignes de `fct_order_items` ;
  - `assert_revenue_formula.sql` — `revenue = quantity * list_price * (1 - discount)` (garde-fou de calcul).

Le run Dagster quotidien lance `dbt build` (et non `dbt run`) : **les tests tournent juste
après chaque modèle**, et un test rouge coupe la propagation en aval plutôt que de livrer
des marts douteuses au dashboard.

> Ces tests valident la **donnée produite**. La validation du pipeline dans son ensemble
> — ingestion, flux temps réel, orchestration, alertes, dashboard, sécurité — est décrite
> dans le **[cahier de recettes](recette/README.md)** (§ 8).

## 4. 📊 Dataviz — Dashboard Looker Studio

🔗 **Lien du tableau de bord** : <https://datastudio.google.com/u/0/reporting/c545b736-0247-4ae7-a23d-909e9cdeb3bd/page/25E2F>

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

## 5. 🩺 Supervision & alertes

Le pipeline est surveillé par une stack **Prometheus + Grafana**, déployée par
`make monitoring`. La présentation complète du système (indicateurs, configuration,
visualisation, alertes, limites) est dans **[`monitoring/README.md`](monitoring/README.md)**.

Le principe tient en une ligne de partage : **Prometheus surveille l'infrastructure,
BigQuery surveille les données, Grafana sert de façade unique aux deux.**

| | Surveillé par | Indicateurs |
|---|---|---|
| **Flux temps réel** (process long-vivant) | Prometheus (scrape de `/metrics`) | liveness, latence p95, débit, rejets BigQuery, profondeur de file |
| **Source Supabase** | Prometheus (sonde TCP blackbox) | joignabilité du port PostgreSQL |
| **Données brutes** (batch quotidien) | BigQuery — vues [`mon_*`](models/monitoring/) | fraîcheur vs SLA 26 h, volumétrie, tables vides, historique des chargements dlt |

Un job batch n'existe que trois minutes par jour : il n'a rien à offrir au scrape de
Prometheus le reste du temps. Son état se lit en revanche très bien dans la donnée
elle-même — d'où les trois vues dbt de la couche `monitoring`, qui détectent notamment
un run manqué *même quand l'orchestrateur ne s'est jamais lancé* (machine éteinte à 05:00 :
aucun run en échec n'existe, seule la fraîcheur de la donnée le révèle).

**6 règles d'alerte** sont provisionnées en tant que code (flux arrêté, Supabase
injoignable, rejets BigQuery, latence dégradée, données périmées, table vide), routées
vers Slack ou e-mail selon la configuration du `.env`.

```bash
make monitoring      # Grafana http://127.0.0.1:3001 · Prometheus http://127.0.0.1:9090
```

---

## 6. 🛠️ Exploitation & maintien en condition opérationnelle

La supervision dit *ce qui va mal* ; la **feuille de route d'exploitation** dit *ce qu'on
fait pour que ça aille bien* : procédures d'administration (démarrage, déploiement,
rollback, accès), calendrier de maintenance du quotidien à l'annuel, échéancier daté
(certificats, rotation des secrets, montées de version), plan de sauvegarde et de reprise
(RTO 4 h / RPO 24 h) et registre des points de vigilance.

Le document complet est dans **[`exploitation/README.md`](exploitation/README.md)**.

---

## 7. 🔁 CI/CD — intégration & déploiement continus

Chaque modification du code est **validée automatiquement avant d'atteindre `main`** par
deux workflows complémentaires :

| Workflow | Volet | Ce qu'il rejoue |
|---|---|---|
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | Fonctionnel & structurel | Reconstruit tout le projet dbt et lance les 149 contrôles (145 tests de données + 4 unit tests) sur des datasets BigQuery **isolés de la prod** |
| [`tests-python.yml`](.github/workflows/tests-python.yml) | Structurel (code) | 53 tests `pytest` sur l'ingestion, les métriques et le flux temps réel — sans secret ni connexion |
| [`security.yml`](.github/workflows/security.yml) | Sécurité | Secrets dans l'historique (gitleaks), efficacité du `.gitignore`, vulnérabilités des dépendances (pip-audit) |

Le déploiement (CD), lui, est porté par le schedule Dagster qui rafraîchit la prod à partir
du code de `main`.

### Schéma de la pipeline

```
 DÉVELOPPEMENT            DÉCLENCHEURS              CI — GitHub Actions                 BIGQUERY
┌───────────────┐        ┌─────────────────────┐   ┌───────────────────────────────┐   ┌─────────────────────────┐
│ branche       │ push   │ pull_request → main │   │ job  build-and-test           │   │ datasets de CI (écrits) │
│ feature/*     │───────►│ push (merge) → main │──►│ runner ubuntu-latest          │──►│ local_bike_ci_*         │
└───────┴───────┘        │ workflow_dispatch   │   │ 1  checkout                   │   │ staging → … → reporting │
        │                └─────────────────────┘   │ 2  Python 3.12 + uv           │   └─────────────────────────┘
        │  ouvre une PR                            │ 3  dbt-bigquery~=1.9          │   ┌─────────────────────────┐
        │                                          │ 4  clé SA ← secret GCP_SA_KEY │◄──│ local_bike_raw  (lu)    │
        ▼                                          │ 5  dbt deps                   │   │ non écrite par la CI    │
┌───────────────┐                                  │ 6  dbt debug  --target ci     │   └─────────────────────────┘
│ Pull Request  │◄── statut du job : OK / KO ──────│ 7  dbt build  --target ci     │
│ peer-review   │                                  │      = run modèles + tests    │
└───────┬───────┘                                  │ 8  rm clé SA  (if: always())  │
        │                                          └───────────────────────────────┘
        │  merge si CI verte + review
        ▼
┌───────────────┐        ┌────────────────────────┐   ┌────────────────────────────────────────────┐
│ main          │  CD    │ Dagster                │   │ PROD : ingestion dlt + dbt build           │
│ (code validé) │───────►│ daily_refresh 05:00    │──►│ local_bike_{staging … reporting}           │
└───────────────┘        └────────────────────────┘   └────────────────────────────────────────────┘
```

### Détail des étapes

| # | Étape (workflow) | Rôle |
|---|---|---|
| — | **Déclencheurs** | `pull_request → main` (valider **avant** le merge), `push → main` (valider l'état final de `main`), `workflow_dispatch` (lancement manuel depuis l'onglet *Actions*) |
| — | **`concurrency`** | 1 run par ref : un nouveau commit annule le run en cours de la PR ; les runs sur `main` sont mis en file (pas deux builds concurrents sur BigQuery) |
| 1 | `actions/checkout@v4` | Récupère le code de la branche testée |
| 2 | `setup-python` 3.12 + `setup-uv` | Runtime + installeur rapide |
| 3 | `uv pip install dbt-bigquery~=1.9` | Seul dbt est installé (pas dlt/dagster) → CI rapide |
| 4 | Écriture de la clé de service | Le JSON vient du secret GitHub `GCP_SA_KEY` ; il est écrit hors du repo et **jamais commité** |
| 5 | `dbt deps` | Installe `dbt_utils` |
| 6 | `dbt debug --target ci` | Échoue tôt et clairement si la connexion BigQuery est cassée |
| 7 | `dbt build --target ci` | **run des modèles + exécution des tests** (génériques et singuliers). Les tests dépendent des modèles : on les reconstruit donc avant de tester |
| 8 | `rm` de la clé (`if: always()`) | Nettoyage du credential même si le build a échoué |

### Isolation de la production

La cible `ci` de [`profiles.yml`](profiles.yml) écrit dans les datasets `local_bike_ci_*`
(`staging`, `intermediate`, `marts`, `reporting`, `monitoring`), créés automatiquement par
`dbt-bigquery`. Elle **lit** `local_bike_raw` mais n'y écrit rien : une PR ne peut donc ni
casser les tables du dashboard, ni altérer les données brutes ingérées.

### Secrets et variables attendus côté GitHub

| Nom | Type | Contenu |
|---|---|---|
| `GCP_SA_KEY` | *secret* | Contenu **complet** du JSON du service account BigQuery |
| `GCP_PROJECT_ID` | *secret* | ID du projet GCP |
| `GCP_LOCATION` | *variable* (optionnelle) | Région des datasets — défaut `EU` |

> **Le « CD » est volontairement découplé de GitHub Actions.** Actions valide le *code* ;
> c'est Dagster (`daily_refresh`, 05:00 Europe/Paris — voir § Orchestration) qui rejoue
> l'ingestion puis `dbt build` sur les datasets de production. Un merge sur `main` ne
> déclenche donc pas de rebuild immédiat de la prod : le prochain run planifié prend le
> nouveau code.

---

## 8. 🧪 Recette & cahier de tests

Les tests dbt prouvent que la *donnée* est juste ; ils ne disent rien de l'idempotence de
l'ingestion, de la latence du flux temps réel, du déclenchement effectif d'une alerte ou de
l'absence de secret dans le dépôt. Le **[cahier de recettes](recette/README.md)** couvre
l'ensemble : **54 scénarios** rattachés à chaque fonctionnalité livrée, avec résultat attendu,
criticité et mode d'exécution.

| Famille | Scénarios | Ce qu'elle valide |
|---|---|---|
| **[Fonctionnels](recette/scenarios/fonctionnels.md)** | 31 (F-01 → F-31) | Comportement de bout en bout : ingestion idempotente, flux temps réel, marts, orchestration, les 6 alertes, les 6 axes du dashboard, CI |
| **[Structurels](recette/scenarios/structurels.md)** | 14 (S-01 → S-14) | Propriétés internes : `pytest` sur le code Python, dbt unit tests sur le calcul du revenu, compilation, lineage, socle des 145 tests dbt |
| **[Sécurité](recette/scenarios/securite.md)** | 9 (SEC-01 → SEC-09) | Règles de la section 4 du `CLAUDE.md` : secrets hors dépôt, SSL imposé, moindre privilège, exposition des UI, dépendances |

Le plan (`recette/scenarios/`) est **stable**, les campagnes (`recette/resultats/`) sont
**datées** : c'est leur comparaison d'une version à l'autre qui rend une régression lisible.
La non-régression automatique est assurée à chaque PR par la CI (§ 7) ; la campagne complète,
scénarios manuels compris, est rejouée à chaque release et consignée dans un procès-verbal
signé ([gabarit](recette/resultats/_gabarit.md)).

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
| Supervision     | Prometheus + Grafana (Docker)  |
| Dashboard       | Looker Studio                  |
| Versioning      | Git / GitHub                   |

## Prérequis

- Python 3.12 (voir `.python-version`)
- Un projet GCP avec BigQuery activé + un service account (rôles *BigQuery Data Editor* + *Job User*)
- Une base Supabase accessible via le **Connection Pooler** (SSL requis)
- Docker + Docker Compose, pour la stack de supervision uniquement (`make monitoring` ; le pipeline fonctionne sans)

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

On se connecte en **direct** (Supabase Studio → Database → Connection string → URI),
port `5432`, SSL obligatoire.

```ini
SUPABASE_HOST=db.<project_ref>.supabase.co          # connexion directe
SUPABASE_PORT=5432
SUPABASE_USER=postgres                               # format direct : postgres (sans suffixe)
SUPABASE_PASSWORD=<mot de passe>                     # uniquement dans .env (ignoré par git)
SUPABASE_DB=postgres
SUPABASE_SSLMODE=require
```

> ⚠️ L'hôte direct `db.<ref>.supabase.co` ne résout **qu'en IPv6**. Vérifier que le réseau
> le supporte (`getent hosts db.<ref>.supabase.co`). Sinon, basculer sur le **Connection
> Pooler** (Session mode, port `5432`, requis par `LISTEN`/`NOTIFY` du flux temps réel) :
> host `aws-<n>-<region>.pooler.supabase.com`, user `postgres.<project_ref>`.
> Un `tenant or user not found` signale un mauvais cluster pooler — vérifier `aws-0` vs
> `aws-1` et la région (Settings → Database → Connection pooling).

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
make monitoring      #    Supervision : Grafana sur http://127.0.0.1:3001
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

make test-unit                 # unit tests : la logique de calcul du revenu (S-06 à S-09)
make test-python               # suite pytest : ingestion, métriques, flux (S-01 à S-05)
make fonctionnels              # campagne fonctionnelle : idempotence, grain des marts
make recette                   # campagne complète : pytest + sécurité + dbt build + fonctionnels
make security                  # volet sécurité du cahier de recettes (SEC-01/02/03)
```

> Le plan de test complet — 54 scénarios fonctionnels, structurels et de sécurité — est le
> **[cahier de recettes](recette/README.md)**.

## Orchestration (Dagster)

### Les briques en place

| Composant | Code | Rôle |
|---|---|---|
| Asset d'ingestion | [`raw_supabase_tables`](orchestration/ingestion.py) | `@multi_asset` (`can_subset=True`) qui réutilise `load_supabase_to_bq.py` et matérialise les 9 tables `local_bike_raw/public_*` — ou seulement celles sélectionnées |
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

### Extension (hors v1) — déclenchement **événementiel** par sensor

Un orchestrateur sait se déclencher de deux façons : sur le **temps** (schedule) et sur un
**événement** (sensor). La v1 n'utilise que le premier : le flux temps réel
([`stream_stock_alerts.py`](ingestion/stream_stock_alerts.py)) **court-circuite volontairement
le DAG** : il n'écrit que dans `local_bike_raw.stock_events_stream`, et la vue
`rpt_stock_alerts_live` l'expose au dashboard en temps réel, sans rejouer les 32 modèles dbt.

[`stock_events_sensor`](orchestration/sensors.py) illustre le second mode et **est livré
désactivé** (`DefaultSensorStatus.STOPPED`) : il ne modifie pas le fonctionnement nominal.

| | Schedule `daily_refresh` (v1) | Sensor `stock_events_sensor` (extension) |
|---|---|---|
| Déclencheur | cron 05:00 Europe/Paris | nouveaux événements dans `local_bike_raw.stock_events_stream` |
| Job | `local_bike_pipeline` — **tout** le DAG | `stock_refresh` — ingestion de `stocks` seule + `stg_stocks → fct_stocks → rpt_stocks` |
| Statut par défaut | à activer dans *Automation* | **off** (hors périmètre v1) |

Détails d'implémentation :

- **Curseur Dagster** : le sensor mémorise le `ingested_at` du dernier événement vu ; seuls
  les événements postérieurs déclenchent un run (idempotent d'un tick à l'autre, et à travers
  un redémarrage du daemon). Au premier tick, il s'aligne sur l'existant sans rejouer l'historique.
- **`minimum_interval_seconds`** : plancher anti-rafale — une salve de mouvements de stock ne
  peut pas produire plus d'un run par intervalle.
- **`run_key`** = borne de fraîcheur : Dagster déduplique, un même lot d'événements ne peut pas
  produire deux runs.
- **Pourquoi ré-ingérer ?** Le flux temps réel n'écrit que dans `stock_events_stream` ;
  `local_bike_raw.public_stocks` reste figé jusqu'au batch suivant. Rebâtir `stg_stocks` sans
  ré-ingérer relirait des quantités périmées.
- **Ingestion subsettable** : l'asset déclare `can_subset=True` et traduit la sélection Dagster
  en argument `only` de dlt — le run n'extrait que `stocks`, pas les 9 tables.
- Le run reste **minimal** : 1 table extraite + 3 modèles dbt, au lieu de 9 tables et 32 modèles.

Voir le sensor en action :

```bash
make stream           # terminal 1 : le listener temps réel
make dagster          # terminal 2 : UI + daemon -> Automation -> stock_events_sensor sur "on"
```

Puis, dans Supabase Studio (SQL Editor) :

```sql
update public.stocks set quantity = quantity - 3 where store_id = 1 and product_id = 1;
```

L'événement apparaît dans les logs du terminal 1 en moins d'une seconde ; au tick suivant,
l'onglet *Automation → stock_events_sensor* montre le tick vert et le run `stock_refresh`
déclenché, taggé `trigger=stock_events_stream`.

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
├── ingestion/          # extraction/chargement Supabase -> BigQuery + métriques Prometheus
├── orchestration/      # code Dagster (assets ingestion + dbt, jobs, schedule, sensor)
├── monitoring/         # stack de supervision (Prometheus + Grafana, docker-compose)
├── exploitation/       # feuille de route d'exploitation (MCO, sauvegarde/reprise)
├── recette/            # cahier de recettes : scénarios de test + PV de campagne
│   ├── scenarios/      #   fonctionnels.md, structurels.md, securite.md
│   ├── checks/         #   contrôles scriptés rejoués par la CI et `make security`
│   ├── run_fonctionnels.py  # campagne fonctionnelle automatisée (F-01, F-03→F-06, F-11)
│   └── resultats/      #   procès-verbaux datés (gabarit fourni)
├── models/             # modèles dbt + YAML doc/tests par couche
│   ├── staging/        #   stg_*.sql + _src/_stg_local_bike.yml
│   ├── intermediate/   #   int_*.sql + _int_local_bike.yml
│   ├── monitoring/     #   mon_*.sql + _monitoring.yml (indicateurs -> Grafana)
│   └── marts/          #   dim_*/fct_*.sql + _marts.yml (star schema)
│       └── reporting/  #     rpt_*.sql + _reporting.yml (tables plates -> Looker Studio)
├── tests/              # tests singuliers (métier)
├── tests_python/       # suite pytest (ingestion, métriques, flux temps réel)
├── secrets/            # credentials locaux (ignoré par git)
├── .env.example        # variables d'environnement attendues
├── dbt_project.yml     # config dbt
├── pyproject.toml      # point d'entrée Dagster ([tool.dagster])
└── requirements.txt
```

> Détails métier, axes d'analyse et schéma en étoile : voir `CLAUDE.md`.
