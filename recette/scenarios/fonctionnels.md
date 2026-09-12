# Scénarios fonctionnels — F-01 à F-31

Validation du comportement **observable** du pipeline, du point de vue de l'équipe
Opérations. Chaque scénario indique sa criticité (🔴 bloquant · 🟠 majeur · 🟡 mineur) et son
mode d'exécution (auto / manuel).

> Conventions, criticités et critères d'acceptation : [`../README.md`](../README.md).
> Rappel : aucun scénario n'écrit en production ; ceux qui modifient Supabase précisent
> leur **remise en état**.

---

> **Scénarios outillés.** F-01, F-03 → F-06 et F-11 sont exécutés par
> [`recette/run_fonctionnels.py`](../run_fonctionnels.py), qui produit un rapport Markdown
> horodaté dans [`resultats/`](../resultats/) : `make fonctionnels` pour la campagne
> fonctionnelle seule, `make recette` pour l'enchaînement complet (pytest → sécurité →
> `dbt build` → fonctionnels). Sélection possible : `--scenarios F-03,F-06`.
>
> Ces scénarios **écrivent réellement** : ils relancent l'ingestion dlt vers
> `local_bike_raw` (opération nominale et idempotente — c'est la propriété testée), et F-04
> modifie une ligne de `public.brands` côté Supabase avant de rétablir l'état initial dans un
> bloc `finally`.

## 1. Ingestion batch — Supabase → `local_bike_raw`

### F-01 — Extraction complète des 9 tables source 🔴 · auto ✅ *outillé*
**Pré-requis** : `.env` renseigné, Supabase joignable, dataset `local_bike_raw` existant.
**Étapes** :
1. `make ingest`
2. Lister les tables de `local_bike_raw` et compter leurs lignes.
**Résultat attendu** : les 9 tables `public_customers`, `public_orders`, `public_order_items`,
`public_staffs`, `public_stores`, `public_categories`, `public_products`, `public_stocks`,
`public_brands` existent ; le `COUNT(*)` de chacune est **égal** à celui de la table Supabase
correspondante ; le script se termine avec le code de sortie 0.

### F-02 — Réconciliation de volumétrie bout-à-bout 🔴 · auto
**Pré-requis** : Dagster lancé (`make dagster`).
**Étapes** : matérialiser l'asset `raw_supabase_tables` et consulter les *Asset Checks*.
**Résultat attendu** : les 18 checks (2 par table) sont au vert —
`postgres_row_count_matches_dlt_load` (pas de perte à l'extraction) et
`bigquery_row_count_matches_dlt_load` (pas de perte au chargement). Tout écart est reporté
avec son delta et la stratégie de chargement appliquée.
**Note** : ces checks sont en sévérité `WARN` — ils remontent dans l'UI sans bloquer le DAG
dbt aval. Un échec ici est donc à traiter comme une **anomalie G1** même si le run est vert.

### F-03 — Idempotence du chargement `merge` 🔴 · auto ✅ *outillé*
*La table transactionnelle est rechargée intégralement à chaque run (pas de colonne
`updated_at` en source) : c'est le `merge` sur clé primaire qui garantit l'absence de doublon.
Cette propriété est le pivot de l'architecture d'ingestion — elle doit être testée.*

**Pré-requis** : une première ingestion réussie (F-01).
**Étapes** :
1. Relever `COUNT(*)` sur `public_customers`, `public_orders`, `public_order_items`.
2. Relancer `make ingest`.
3. Relever à nouveau les trois `COUNT(*)`.
4. Vérifier l'unicité des clés primaires : `customer_id`, `order_id`, `(order_id, item_id)`.
**Résultat attendu** : volumétries **strictement identiques** avant/après ; zéro doublon de
clé primaire ; les colonnes `_dlt_load_id` montrent bien deux passes de chargement distinctes.

Ce dernier point n'est pas décoratif : sans lui, le scénario passerait pour une mauvaise
raison le jour où l'ingestion ne s'exécuterait pas du tout — une volumétrie inchangée est
aussi ce que produit un run qui n'a rien fait.

### F-04 — Idempotence et rafraîchissement du chargement `replace` 🔴 · auto ✅ *outillé*
**Pré-requis** : F-01 réussi.
**Étapes** :
1. Modifier un libellé en source : `UPDATE public.brands SET brand_name = 'RECETTE-F04' WHERE brand_id = 1;`
2. `make ingest`
3. Lire `public_brands` dans BigQuery.
**Résultat attendu** : la valeur modifiée est présente ; le nombre de lignes est inchangé
(remplacement complet, pas d'accumulation) ; les 6 référentiels concernés se comportent de
même (`staffs`, `stores`, `categories`, `products`, `stocks`, `brands`).
**Remise en état** : restaurer le libellé d'origine puis relancer `make ingest`.

### F-05 — Ingestion ciblée sur une table 🟠 · auto ✅ *outillé*
**Étapes** : `python ingestion/load_supabase_to_bq.py customers`
**Résultat attendu** : seule `public_customers` est rechargée (nouveau `_dlt_load_id`) ;
les 8 autres tables conservent leur `_dlt_load_id` précédent. C'est la propriété exploitée
par le sensor `stock_events_sensor` pour ne ré-ingérer que `stocks`.

### F-06 — Échec explicite sur credential manquant 🟠 · auto ✅ *outillé*
**Étapes** : lancer l'ingestion avec `SUPABASE_PASSWORD` vide.
**Résultat attendu** : arrêt immédiat, code de sortie **non nul**, message nommant la
variable manquante et renvoyant vers `.env.example` — jamais de trace contenant une valeur
de secret (cf. SEC-08).

---

## 2. Flux temps réel — mouvements de stock

### F-07 — Capture d'un mouvement de stock 🟠 · manuel
**Pré-requis** : trigger `stock_events` installé côté Supabase
([`ingestion/sql/stock_events_trigger.sql`](../../ingestion/sql/stock_events_trigger.sql)),
`make stream` en cours d'exécution.
**Étapes** :
1. Noter l'heure, puis en source : `UPDATE public.stocks SET quantity = quantity - 1 WHERE store_id = 1 AND product_id = 1;`
2. Interroger `local_bike_raw.stock_events_stream` sur les dernières lignes.
**Résultat attendu** : un événement correspondant apparaît en **moins de 5 secondes**
(SLO p95 de [`exploitation/README.md`](../../exploitation/README.md)), portant le magasin, le
produit, la nouvelle quantité et le seuil d'alerte en vigueur au moment de l'événement.
**Remise en état** : rétablir la quantité initiale.

### F-08 — Alerte de stock en vue temps réel 🟠 · manuel
**Pré-requis** : F-07 réussi, `STOCK_ALERT_THRESHOLD` connu.
**Étapes** :
1. Passer la quantité d'un couple magasin/produit **sous** le seuil ; lire `rpt_stock_alerts_live`.
2. Repasser la quantité **au-dessus** du seuil ; relire la vue.
**Résultat attendu** : le couple apparaît dans la vue après (1) et en disparaît après (2),
sans aucune action de rafraîchissement — la vue étant recalculée à chaque lecture. C'est la
propriété qui permet au widget Looker Studio d'être juste en permanence.
**Remise en état** : rétablir la quantité initiale.

### F-09 — Détection de l'arrêt du flux 🟠 · manuel
**Étapes** : interrompre `make stream`, attendre 5 minutes.
**Résultat attendu** : la cible Prometheus `stock_stream` passe à `up == 0` et l'alerte
Grafana « Flux temps réel arrêté » est notifiée (cf. F-18).
**Limite connue** : le listener n'implémente pas de reconnexion automatique — une coupure
réseau met fin au process, ce qui est **détecté** mais pas **réparé**. Le redémarrage
automatique est porté par le chantier E1 (`systemd Restart=always`, échéance 25/09/2026) de
la feuille de route d'exploitation. À re-tester après ce chantier.

---

## 3. Transformation dbt

### F-10 — Construction complète et suite de tests 🔴 · auto (CI)
**Étapes** : `make build` (soit `dbt build`).
**Résultat attendu** : les **25 modèles** se construisent (9 staging, 1 intermediate,
8 marts, 4 reporting, 3 monitoring) ; les **149 contrôles** passent — 145 tests de données
(142 génériques + 3 singuliers) et 4 unit tests ; code de sortie 0. Les datasets `local_bike_staging`,
`local_bike_intermediate`, `local_bike_marts`, `local_bike_reporting` et
`local_bike_monitoring` sont peuplés.

### F-11 — Conservation du grain et exhaustivité 🔴 · auto ✅ *outillé*
**Étapes** : comparer `COUNT(*)` de `fct_order_items` à celui de `public_order_items`, et
`COUNT(*)` de `fct_orders` au `COUNT(DISTINCT order_id)` de la source.
**Résultat attendu** : égalité stricte dans les deux cas. Une jointure d'enrichissement mal
écrite (produit dupliqué) se manifesterait ici par une inflation de lignes — donc par un
revenu total surévalué sur tous les axes.
**Prérequis** : les marts doivent être construits. Si `fct_order_items` n'existe pas, le
scénario est marqué **non joué** avec sa justification plutôt que non conforme — un prérequis
absent n'est pas un défaut du produit.

### F-12 — Cohérence du revenu entête / lignes 🔴 · auto
**Étapes** : `dbt test --select fct_orders fct_order_items int_order_items_enriched`
**Résultat attendu** : les trois tests singuliers passent —
`assert_revenue_formula` (revenu = `quantity × list_price × (1 − discount)`),
`assert_order_revenue_reconciliation` (`fct_orders.total_revenue` = somme des lignes),
`assert_shipped_after_order_date` (aucune expédition antérieure à la commande).

### F-13 — Lisibilité du reporting sans blend 🟠 · manuel
*Raison d'être de la couche `reporting` : Looker Studio devrait sinon joindre fait et
dimensions côté BI, opération coûteuse et source d'erreurs.*

**Étapes** : pour chacune des tables `rpt_sales`, `rpt_orders`, `rpt_stocks`, vérifier qu'un
graphique du dashboard peut être construit **à partir de cette seule table**.
**Résultat attendu** : chaque `rpt_*` porte à la fois les mesures et les attributs
descriptifs (magasin, état, produit, catégorie, marque, client, vendeur, dates) ; aucune
page du dashboard n'utilise de source mixte (« blend »).

---

## 4. Orchestration Dagster

### F-14 — Exécution du pipeline complet 🟠 · manuel
**Étapes** : `make dagster`, puis lancer le job `local_bike_pipeline` depuis l'UI.
**Résultat attendu** : le run matérialise dans l'ordre l'asset d'ingestion (9 tables) puis
les 25 assets dbt ; la lineage affichée est continue de `public_*` jusqu'aux `rpt_*` ; le
run se termine en `SUCCESS`.

### F-15 — Déclenchement quotidien programmé 🟠 · auto
**Étapes** : `dagster definitions validate`, puis consulter l'onglet *Automation*.
**Résultat attendu** : le schedule `daily_refresh` est enregistré, actif, associé au job
`local_bike_pipeline`, cron `0 5 * * *`, fuseau `Europe/Paris`.

### F-16 — Chemin d'erreur : un test rouge coupe la propagation 🔴 · manuel
*Le scénario le plus important de cette section : il ne valide pas le chemin nominal mais
la **garantie** que rien de douteux n'atteint le dashboard.*

**Étapes** :
1. Introduire temporairement un test voué à l'échec (ex. `accepted_range` impossible sur
   `fct_order_items.revenue`).
2. Lancer `dbt build`.
**Résultat attendu** : le test échoue, `dbt build` se termine en erreur, et **les modèles
situés en aval du modèle testé ne sont pas construits** — les tables `reporting` conservent
leur contenu précédent plutôt que d'être rafraîchies à partir de données invalides.
**Remise en état** : retirer le test temporaire, relancer `make build`.

### F-17 — Conformité du périmètre v1 : sensor livré désactivé 🟡 · auto
**Étapes** : consulter l'état de `stock_events_sensor`.
**Résultat attendu** : statut **STOPPED** par défaut (`DefaultSensorStatus.STOPPED`) — le
déclenchement événementiel est livré comme démonstration, le fonctionnement nominal restant
le schedule quotidien.

---

## 5. Supervision et alertes

*Une règle d'alerte jamais déclenchée n'est pas une règle prouvée. Les six règles
provisionnées sont donc déclenchées volontairement, une par une.*
Procédures détaillées : [`monitoring/README.md`](../../monitoring/README.md).

### F-18 — Alerte « Flux temps réel arrêté » 🟠 · manuel
**Étapes** : arrêter `make stream`, attendre 5 min.
**Résultat attendu** : notification reçue sur le canal configuré (Slack ou e-mail),
mentionnant que le listener ne répond plus. **Preuve** : capture de la notification.

### F-19 — Alerte « Supabase injoignable » 🟠 · manuel
**Étapes** : bloquer le port PostgreSQL vers Supabase (règle pare-feu locale temporaire).
**Résultat attendu** : la sonde blackbox passe à `probe_success == 0`, alerte notifiée.
**Remise en état** : retirer la règle de pare-feu.

### F-20 — Alerte « Données brutes périmées » 🟠 · manuel
**Étapes** : simuler la péremption sans attendre 26 h —
`dbt run --select mon_raw_freshness --vars '{raw_freshness_sla_hours: 0}'`
**Résultat attendu** : `is_stale` passe à vrai sur les 9 tables, alerte notifiée au tick
suivant. **Remise en état** : `dbt run --select mon_raw_freshness` (SLA nominal de 26 h).

### F-21 — Alerte « Table brute vide » 🟠 · manuel
**Étapes** : provoquer une table brute à zéro ligne dans un dataset de test et pointer la vue
`mon_raw_freshness` dessus (`--vars '{raw_dataset: ...}'`).
**Résultat attendu** : la table vide est détectée et notifiée. **Important** : ce scénario ne
s'exécute **jamais** sur `local_bike_raw` en production.

### F-22 — Alerte « Rejets BigQuery sur le flux » 🟠 · manuel
**Étapes** : faire échouer volontairement l'écriture (table cible temporairement renommée ou
droits retirés), puis émettre un mouvement de stock.
**Résultat attendu** : `local_bike_stream_insert_errors_total` s'incrémente, alerte notifiée
sous 15 min. **Remise en état** : rétablir la table et les droits.

### F-23 — Alerte « Latence dégradée » 🟠 · manuel
**Étapes** : vérifier l'histogramme `local_bike_stream_event_latency_seconds` directement sur
`http://localhost:9101/metrics` après une rafale de mouvements.
**Résultat attendu** : le p95 mesuré est **inférieur à 5 s** en fonctionnement nominal (SLO
respecté) ; la règle se déclenche si le seuil est temporairement abaissé pour la démonstration.

---

## 6. Dashboard Looker Studio — les 6 axes d'analyse

*Un axe est validé quand un membre de l'équipe Opérations obtient la réponse à sa question
**sans écrire de SQL**. Chaque scénario croise la lecture du dashboard avec une requête de
contrôle sur BigQuery.*

### F-24 — Axe Revenu 🔴 · manuel
**Étapes** : sur la page *Revenu & ventes*, lire le revenu total, puis le revenu du magasin
le plus performant sur un mois donné ; comparer à `SELECT SUM(revenue) FROM rpt_sales` avec
les mêmes filtres.
**Résultat attendu** : concordance exacte ; les filtres magasin / état / période (jour, mois,
année) sont opérationnels.

### F-25 — Axe Top produits / catégories / marques 🟠 · manuel
**Résultat attendu** : classement par revenu **et** par volume ; le top 10 produits concorde
avec la requête de contrôle sur `rpt_sales` ; les trois niveaux (produit, catégorie, marque)
sont disponibles.

### F-26 — Axe Stocks 🟠 · manuel
**Résultat attendu** : niveau de stock par magasin, liste des produits en rupture
(`quantity = 0`) et valorisation concordent avec `rpt_stocks` ; le widget temps réel alimenté
par `rpt_stock_alerts_live` reflète l'état courant (cf. F-08).

### F-27 — Axe Clients 🟠 · manuel
**Résultat attendu** : panier moyen, fréquence d'achat, répartition géographique et lecture
RFM sont disponibles et concordent avec `rpt_sales`.

### F-28 — Axe Livraison 🟠 · manuel
**Résultat attendu** : délai moyen (`shipped_date − order_date`), taux de retard vs
`required_date` et répartition des `order_status` concordent avec `rpt_orders` ; aucune
commande n'affiche de délai négatif (cohérent avec `assert_shipped_after_order_date`).

### F-29 — Axe Staff 🟠 · manuel
**Résultat attendu** : revenu généré par vendeur et par magasin, concordant avec `rpt_sales` ;
chaque vendeur est bien rattaché à son magasin.

---

## 7. CI / CD

### F-30 — Validation automatique avant merge 🟠 · auto
**Étapes** : ouvrir une PR vers `main`.
**Résultat attendu** : le workflow `dbt CI` se déclenche, exécute `dbt deps`,
`dbt debug --target ci` puis `dbt build --target ci`, et conditionne le merge à son succès.
Un nouveau commit sur la PR annule le run en cours (concurrence `cancel-in-progress`).

### F-31 — Isolation des datasets de CI 🔴 · auto
**Étapes** : après un run de CI, lister les datasets du projet BigQuery.
**Résultat attendu** : la CI a écrit **exclusivement** dans `local_bike_ci_*` ; les datasets
de production (`local_bike_staging`, `_marts`, `_reporting`, `_monitoring`) n'ont pas été
modifiés — vérifiable par leur horodatage de dernière modification.
