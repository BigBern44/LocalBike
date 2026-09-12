# Scénarios structurels — S-01 à S-14

Validation des propriétés **internes** du code et des modèles, indépendamment du contenu des
données de production. Un scénario structurel doit pouvoir échouer sur un dépôt dont la base
source est éteinte : c'est ce qui le distingue d'un scénario fonctionnel.

> Conventions et criticités : [`../README.md`](../README.md).

---

## 1. Code Python — `pytest`

Cible : [`tests_python/`](../../tests_python/) — distinct de [`tests/`](../../tests/),
réservé aux tests singuliers dbt. Aucune connexion réseau : les scénarios portent sur des
fonctions pures ou moquées, et passent donc sur un poste sans `.env` comme dans une CI sans
secret.
**Exécution** : `make test-python` en local, job `pytest` de
[`tests-python.yml`](../../.github/workflows/tests-python.yml) en CI. **53 tests.**

### S-01 — Stratégie de chargement par table 🔴 · auto (CI) ✅ *outillé*
**Cible** : `_load_strategy` / `MERGE_PRIMARY_KEYS` de
[`ingestion/load_supabase_to_bq.py`](../../ingestion/load_supabase_to_bq.py).
**Résultat attendu** :
- `customers` → `merge` sur `customer_id` ;
- `orders` → `merge` sur `order_id` ;
- `order_items` → `merge` sur la clé composite `(order_id, item_id)` ;
- les **6 autres** tables (`staffs`, `stores`, `categories`, `products`, `stocks`, `brands`)
  → `replace` ;
- `SOURCE_TABLES` contient exactement 9 entrées.

*Pourquoi ce test* : c'est la contrepartie structurelle de F-03/F-04. Si quelqu'un retire
`orders` du mapping, l'idempotence se perd silencieusement — la table se rechargerait en
`replace`, ce qui reste correct pour la volumétrie mais détruit l'historique d'`_dlt_load_id`
et fait diverger la stratégie documentée du comportement réel.

### S-02 — Construction de l'URI source 🔴 · auto (CI) ✅ *outillé*
**Cible** : `build_source_uri`.
**Résultat attendu** : l'URI produite porte le paramètre `sslmode` issu de l'environnement
(valeur attendue `require`, cf. section 4 du `CLAUDE.md`) ; un mot de passe contenant des
caractères spéciaux (`@`, `/`, `:`, `#`) est **URL-encodé** et l'URI reste parsable ; le mot
de passe n'apparaît dans aucune sortie de log.

> **Anomalie détectée et corrigée le 11/09/2026 (défaut latent, G1 si déclenché).**
> `quote()` laisse par défaut
> le caractère `/` non encodé (`safe="/"`). Un mot de passe Supabase contenant un slash
> produisait donc une URI dont le `netloc` était tronqué au premier `/` : l'hôte était lu
> comme `postgres` et la connexion échouait sans message pointant vers la cause.
> Correction : `quote(..., safe="")` sur l'utilisateur et le mot de passe. Le test couvre
> désormais les cinq caractères réservés.

### S-03 — Échec explicite sur variable manquante 🟠 · auto (CI) ✅ *outillé*
**Cible** : `env(..., required=True)`.
**Résultat attendu** : lève `SystemExit` avec un code non nul ; le message nomme la variable
absente ; aucune valeur de secret n'est incluse. Contrepartie structurelle de F-06.

### S-04 — Métriques Prometheus du flux 🟠 · auto (CI) ✅ *outillé*
**Cible** : [`ingestion/metrics.py`](../../ingestion/metrics.py).
**Résultat attendu** : les 5 compteurs (`events_received`, `alerts_raised`, `rows_flushed`,
`insert_errors`, `parse_errors`), l'histogramme de latence et les 5 jauges sont déclarés sous
le préfixe `local_bike_stream` ; le port d'exposition par défaut est **9101** et correspond à
la cible déclarée dans [`monitoring/prometheus/prometheus.yml`](../../monitoring/prometheus/prometheus.yml).
Un incrément de compteur est reflété dans la valeur exposée.

*Pourquoi ce test* : une métrique renommée casse silencieusement une règle d'alerte — le code
tourne, le scrape réussit, et l'alerte cesse simplement de se déclencher. La suite croise donc
trois fichiers qui ne se relisent pas entre eux : les séries déclarées dans le code, la cible
scrapée par `prometheus.yml` (port 9101) et les séries référencées par `rules.yml.tmpl` —
toute règle d'alerte orpheline fait échouer le test.

### S-05 — Robustesse du parsing d'événement 🟠 · auto (CI) ✅ *outillé*
**Cible** : traitement du payload `pg_notify` dans
[`ingestion/stream_stock_alerts.py`](../../ingestion/stream_stock_alerts.py).
**Résultat attendu** : un payload JSON valide produit un événement complet (magasin, produit,
quantité, seuil) ; un payload **malformé ou tronqué** incrémente `parse_errors` et est
ignoré **sans interrompre la boucle d'écoute**. Le seuil d'alerte est **inclusif**
(`quantity_after <= threshold`), règle que reprend la vue `rpt_stock_alerts_live`.

> **Anomalie détectée et corrigée le 11/09/2026 (gravité G2).** Ce contrat n'était pas
> implémenté : `build_event_row` propageait `JSONDecodeError` ou `KeyError` jusqu'à la boucle
> d'écoute, et la série `local_bike_stream_parse_errors_total` — pourtant déclarée et
> documentée pour ce cas précis — n'était incrémentée que par un horodatage illisible. La
> supervision sous-comptait donc les rejets. Correction : la fonction retourne `None` sur
> charge utile illisible, après incrément du compteur et journalisation ; l'appelant ignore
> l'événement et poursuit l'écoute.

---

## 2. Logique de transformation — dbt unit tests

*Différence de nature avec les tests singuliers de [`tests/`](../../tests/) : ceux-ci
vérifient la donnée réelle produite, les unit tests vérifient la **logique SQL** sur des
entrées choisies. Ils s'exécutent sans données en base et attrapent une régression au moment
où le modèle est modifié, avant même la prochaine ingestion.*

Cible : `unit_tests:` dans
[`models/intermediate/_int_local_bike.yml`](../../models/intermediate/_int_local_bike.yml),
modèle `int_order_items_enriched`.
**Exécution** : `make test-unit` en local, ou `dbt build` — qui les joue **avant** de
construire le modèle : une logique fausse empêche la vue d'exister, au lieu de la publier
puis de la tester. Ils sont donc déjà couverts par la CI `dbt-ci.yml`, sans job dédié.

> **Vérification par mutation (11/09/2026).** Un test vert ne prouve rien tant qu'on n'a pas
> vu ce qu'il attrape. Deux régressions ont été introduites volontairement dans le modèle :
> inverser la remise (`1 - discount` → `1 + discount`) fait tomber S-06, S-07 et S-08 ;
> transformer le `left join products` en `inner join` fait tomber S-09 — et seulement S-09.
> Chaque scénario détecte bien la régression qu'il est censé couvrir, et le modèle a été
> restauré à l'identique après contrôle.

### S-06 — Revenu avec remise standard 🔴 · auto (CI) ✅ *outillé*
**Unit test** : `revenue_applique_la_remise`
**Entrée** : `quantity = 2`, `list_price = 100.00`, `discount = 0.10`.
**Attendu** : `revenue = 180.00`. Verrouille la formule contre une inversion de la remise
(qui donnerait 20,00 €) ou son oubli (200,00 €).

### S-07 — Bornes de remise 🔴 · auto (CI) ✅ *outillé*
**Unit test** : `revenue_bornes_de_remise`
**Entrées** : `discount = 0` (aucune remise) et `discount = 1` (gratuité totale).
**Attendu** : respectivement `quantity × list_price` et `0.00`. Ces deux bornes encadrent la
plage admise par le test générique `accepted_range` de la couche staging.

### S-08 — Arrondi monétaire 🟠 · auto (CI) ✅ *outillé*
**Unit test** : `revenue_arrondi_deux_decimales`
**Entrée** : `quantity = 3`, `list_price = 33.33`, `discount = 0.15` → 84,9915 € avant arrondi.
**Attendu** : résultat arrondi à **2 décimales**, conformément au `round(..., 2)` du modèle —
et donc réconciliable avec `fct_orders.total_revenue` (cf. F-12). Un arrondi divergent entre
la ligne et l'entête ferait échouer la réconciliation sur de gros volumes.

### S-09 — Ligne de commande sans produit rattaché 🟠 · auto (CI) ✅ *outillé*
**Unit test** : `revenue_ligne_sans_produit_rattache`
**Entrée** : une ligne dont le `product_id` est absent de `stg_products`.
**Attendu** : la ligne est **conservée** (jointure `left join`), le revenu est calculé à
partir du `list_price` porté par la ligne de commande, et les attributs produit / marque /
catégorie sont `null`. Le revenu total ne doit jamais dépendre de la complétude du
référentiel produits.

---

## 3. Intégrité du projet

### S-10 — Compilation du projet dbt 🔴 · auto (CI)
**Étapes** : `dbt deps && dbt compile`
**Résultat attendu** : **25 modèles** compilés sans erreur ; toutes les références `ref()` et
`source()` résolues ; aucun modèle orphelin. Le découpage attendu est 9 staging,
1 intermediate, 8 marts, 4 reporting, 3 monitoring.

### S-11 — Chargement des définitions Dagster 🟠 · auto (CI)
**Étapes** : `dagster definitions validate`
**Résultat attendu** : le code se charge sans exécution ; 2 jobs (`local_bike_pipeline`,
`stock_refresh`), 1 schedule (`daily_refresh`), 1 sensor (`stock_events_sensor`), la ressource
`dbt` et les assets d'ingestion + dbt sont déclarés. **Aucune connexion à Supabase ou
BigQuery ne doit être ouverte à l'import** — c'est la propriété qui rend ce test exécutable
en CI sans secret de base.

### S-12 — Continuité de la lineage ingestion → dbt 🟠 · auto
**Cible** : `raw_asset_key` / `LocalBikeDbtTranslator` de
[`orchestration/dbt.py`](../../orchestration/dbt.py).
**Résultat attendu** : chacune des 9 sources dbt `local_bike_raw.public_*` porte la **même
asset key** que l'asset d'ingestion correspondant, de sorte que la lineage Dagster soit
continue de Supabase jusqu'aux `rpt_*`. Une divergence de nommage produirait deux graphes
déconnectés — visuellement discret, fonctionnellement invalidant pour l'orchestration.

### S-13 — Socle de tests génériques par couche 🔴 · auto (CI) ✅ *outillé*
**Étapes** : `dbt build`
**Résultat attendu** : les **145 tests** passent, répartis comme suit :

| Test générique | Occurrences | Rôle |
|---|---|---|
| `not_null` | 66 | Complétude des clés et mesures |
| `accepted_range` | 23 | Bornes métier (quantités, prix, remises, revenus) |
| `unique` | 21 | Unicité des clés primaires |
| `relationships` | 22 | Intégrité référentielle entre faits et dimensions |
| `unique_combination_of_columns` | 8 | Clés composites (`order_items`, `stocks`) |
| `accepted_values` | 2 | Domaine des `order_status` |
| Tests singuliers | 3 | Règles métier (cf. F-12) |
| **Total tests de données** | **145** | |
| *Unit tests* (S-06 → S-09) | *4* | *Logique de calcul, hors données réelles* |

Chacune des 5 couches (staging, intermediate, marts, reporting, monitoring) est couverte.
S'y ajoutent **2 configurations `freshness`** de source — non comptées comme tests par dbt,
évaluées par `dbt source freshness` et reprises par la vue `mon_raw_freshness` (cf. F-20).

### S-14 — Complétude documentaire 🟡 · auto
**Étapes** : après `dbt docs generate`, inspecter le `manifest.json`.
**Résultat attendu** : 100 % des modèles des couches **marts** et **reporting** portent une
`description`, ainsi que chacune de leurs colonnes — ce sont elles qui alimentent le
dashboard et qui seront lues par l'équipe Opérations. Les couches staging, intermediate et
monitoring sont documentées au niveau modèle au minimum.
