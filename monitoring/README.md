# Supervision & alertes — Local Bike

Présentation du système de supervision du pipeline ELT (compétence **C4.3.1**).

---

## 1. Principe : deux natures de signal, une seule façade

L'architecture comporte deux moitiés qui ne se surveillent pas de la même manière.

| | Flux temps réel | Batch quotidien |
|---|---|---|
| Nature | process **long-vivant** (`make stream`) | jobs **éphémères** (3 min à 05:00) |
| Question posée | « tourne-t-il, à quelle latence ? » | « a-t-il tourné, et qu'a-t-il produit ? » |
| Outil adapté | **Prometheus** (scrape d'un endpoint) | **BigQuery** (état persistant de la donnée) |

Prometheus est un système *pull* : il interroge périodiquement un endpoint HTTP. C'est
le modèle exact d'un listener qui tourne en permanence — mais l'inverse d'un job batch,
qui n'existe que trois minutes par jour et n'a rien à offrir au scrape le reste du temps.
Inversement, l'état d'un chargement se lit très bien dans l'entrepôt lui-même, et
BigQuery le conserve sans limite de rétention.

D'où la ligne de partage retenue :

- **Prometheus** → santé de l'**infrastructure** : liveness, latence, débit, rejets.
  Haute fréquence (10–15 s), rétention 15 jours.
- **BigQuery** (`local_bike_monitoring`) → santé des **données** : fraîcheur, volumétrie,
  historique des chargements. Basse fréquence, rétention illimitée.
- **Grafana** → **façade unique** : les deux sources dans un même dashboard, et un seul
  moteur d'alertes qui sait interroger les deux.

```
stream_stock_alerts.py ──/metrics:9101──┐
blackbox_exporter ──TCP──► Supabase     ├──► Prometheus ──┐
                   └──HTTP─► Dagster    ┘                 ├──► Grafana ──► Slack / e-mail
BigQuery · local_bike_monitoring ────────────────────────┘     :3001
  (vues dbt mon_raw_freshness, mon_ingestion_loads, mon_stream_latency)
```

**Pas d'Alertmanager** : l'alerting unifié de Grafana couvre Prometheus *et* BigQuery
dans un seul moteur, avec un seul canal. Deux moteurs d'alerte pour un pipeline de cette
taille produiraient de la confusion, pas de la robustesse.

---

## 2. Les éléments et indicateurs surveillés

### Infrastructure — collectée par Prometheus

| Composant | Indicateur | Métrique | SLO |
|---|---|---|---|
| Flux temps réel | Process vivant | `up{job="stock_stream"}` | = 1 |
| | Latence bout en bout (p95) | `local_bike_stream_event_latency_seconds` | < 5 s |
| | Débit d'événements | `local_bike_stream_events_total` | — (contexte) |
| | Mouvements sous seuil | `local_bike_stream_alerts_total` | — (métier) |
| | Rejets à l'écriture | `local_bike_stream_insert_errors_total` | = 0 |
| | Retard d'écriture | `local_bike_stream_queue_depth` | ≈ 0 |
| | Charges utiles illisibles | `local_bike_stream_parse_errors_total` | = 0 |
| Source Supabase | Joignabilité TCP | `probe_success{job="blackbox_tcp"}` | = 1 |
| Orchestrateur | UI Dagster joignable | `probe_success{job="blackbox_http"}` | — (indicatif) |
| Prometheus | Auto-supervision | `up{job="prometheus"}` | = 1 |

La latence est mesurée entre `event_ts` (COMMIT PostgreSQL, posé par le trigger) et
`ingested_at` (réception par le listener) — exactement les deux horodatages écrits sur
chaque ligne BigQuery, pour que Prometheus et l'entrepôt mesurent la même chose.

### Données — calculée par les vues dbt de `local_bike_monitoring`

| Vue | Indicateur | Seuil |
|---|---|---|
| `mon_raw_freshness` | Âge de chaque table brute (`hours_since_last_load`) | < 26 h |
| | Dépassement de SLA (`is_stale`) | faux |
| | Table vide après chargement (`is_empty`) | faux |
| | Volumétrie (`row_count`, `size_mb`) | — (détection d'anomalie) |
| `mon_ingestion_loads` | Dernier chargement dlt réussi (`is_success`) | vrai |
| | Écart entre passes (`minutes_since_previous_load`) | ≈ 1440 min |
| `mon_stream_latency` | p95 horaire reconstitué depuis les données écrites | < 5 s |
| | Débit horaire (`events`, `alerts`) | — (contexte) |

Le SLA de 26 h = 24 h de cycle + 2 h de marge (durée du run, retard de schedule).
Il est paramétré dans `dbt_project.yml` (`vars: raw_freshness_sla_hours`).

**Les trois vues sont matérialisées en `view`, pas en `table`.** Une table figerait les
indicateurs à la dernière exécution de dbt — c'est-à-dire précisément au moment où le
pipeline allait bien. Une vue est recalculée à chaque lecture de Grafana.

### Deux indicateurs qui se recoupent volontairement

`mon_stream_latency` recalcule une latence que Prometheus mesure déjà. Ce doublon est
délibéré : Prometheus mesure ce que **le process croit avoir traité**, la vue mesure ce
qui **est réellement arrivé dans l'entrepôt**. Un écart entre les deux dénonce un échec
d'écriture — panne invisible pour une simple sonde de liveness, puisque le process reste
vivant tout en perdant des données.

---

## 3. Choix et configuration des outils

### Ce qui est déployé

| Outil | Image | Rôle | Port |
|---|---|---|---|
| Prometheus | `prom/prometheus:v3.6.0` | collecte + stockage des séries | 9090 |
| Blackbox exporter | `prom/blackbox-exporter:v0.27.0` | sondes réseau externes | 9115 |
| Grafana | `grafana/grafana:12.2.0` | dashboards + alerting | **3001** |

Grafana est publié sur **3001** et non 3000 : ce port est déjà pris par l'UI Dagster.

Le blackbox exporter tourne sur le **réseau de l'hôte** (`network_mode: host`), à la
différence des deux autres. Raison : l'hôte Supabase direct `db.<ref>.supabase.co` ne
publie qu'un enregistrement **AAAA (IPv6)**, or les réseaux bridge Docker sont IPv4-only
par défaut. Depuis un bridge, la sonde échouerait en permanence — c'est-à-dire
signalerait une panne Supabase inexistante, le pire défaut possible pour une supervision.

### Pourquoi pas seulement Looker Studio ?

Le dashboard métier est déjà sur Looker Studio. Mais Looker Studio ne sait pas lire
Prometheus, ne gère pas d'alertes sur seuil, et n'a pas de notion d'état d'incident.
Grafana couvre les trois, et lit BigQuery aussi bien que Looker Studio via le plugin
officiel `grafana-bigquery-datasource`. La séparation est nette : **Looker Studio pour
le métier** (revenu, stocks, clients), **Grafana pour l'exploitation**.

### Configuration versionnée, secrets générés

La source de données BigQuery a besoin de la **clé privée** du service account.
L'écrire dans un fichier de provisioning versionné violerait la règle de sécurité du
projet. D'où le principe :

> **Ce qu'un humain édite est versionné ; ce que Grafana et Prometheus lisent est généré.**

`monitoring/bootstrap.py` lit le `.env` et rend les gabarits dans `monitoring/generated/`
(ignoré par git) : source BigQuery avec la clé injectée, dashboard avec l'identifiant de
projet GCP, règles d'alerte, cibles des sondes. Aucune valeur d'environnement n'est
committée, et `make monitoring` reste une seule commande.

```
monitoring/
├── docker-compose.yml              # la stack
├── bootstrap.py                    # rend les gabarits depuis .env
├── prometheus/prometheus.yml       # scrape (cibles env-dépendantes en file_sd)
├── blackbox/blackbox.yml           # modules de sonde TCP / HTTP
├── grafana/                        # ← VERSIONNÉ (gabarits)
│   ├── datasources/                #   Prometheus (statique) + BigQuery (.tmpl)
│   ├── dashboards/                 #   provider + dashboard (.tmpl)
│   └── alerting/rules.yml.tmpl     #   les 6 règles d'alerte
└── generated/                      # ← IGNORÉ PAR GIT (rendu, contient la clé)
```

---

## 4. Visualisation

Un dashboard, **« Local Bike — Santé du pipeline »**, en deux bandeaux qui reprennent la
ligne de partage de l'architecture.

**Infrastructure (Prometheus)** — rafraîchi toutes les 30 s
- 4 scorecards : flux temps réel `ACTIF/ARRÊTÉ`, source Supabase `JOIGNABLE/INJOIGNABLE`,
  latence p95 (seuils vert < 2 s < orange < 5 s < rouge), rejets BigQuery sur 1 h ;
- débit d'événements par minute, ventilé par opération PostgreSQL, avec la part sous seuil ;
- profondeur de file et lignes écrites par minute, superposées : si la file monte pendant
  que l'écriture stagne, le goulot est côté BigQuery.

**Données (BigQuery)**
- scorecards : nombre de tables périmées, âge de la donnée la plus ancienne ;
- tableau de fraîcheur des 9 tables brutes (âge en dégradé de couleur, volumétrie, drapeaux) ;
- historique des 15 derniers chargements dlt, avec l'écart entre passes ;
- latence horaire reconstituée depuis BigQuery sur 7 jours, à comparer à la courbe Prometheus.

Répartition des usages : **Grafana** pour la vue de synthèse et l'état des alertes,
**l'UI Dagster** (:3000) pour le diagnostic technique — lineage, logs, traces, asset checks.

---

## 5. Le système d'alertes

6 règles provisionnées, en deux groupes évalués à des cadences différentes.

| Règle | Déclencheur | Pour | Sévérité |
|---|---|---|---|
| **Flux temps réel arrêté** | `up{job="stock_stream"} < 1` | 5 min | 🔴 critique |
| **Supabase injoignable** | `probe_success < 1` | 5 min | 🔴 critique |
| **Rejets BigQuery** | `increase(insert_errors[15m]) > 0` | immédiat | 🔴 critique |
| **Latence dégradée** | `p95 > 5 s` | 10 min | 🟡 warning |
| **Données brutes périmées** | `countif(is_stale) > 0` | immédiat | 🔴 critique |
| **Table brute vide** | `countif(is_empty) > 0` | immédiat | 🔴 critique |

Le groupe `infrastructure` est évalué **toutes les minutes** (un composant tombé doit se
voir tout de suite) ; le groupe `donnees` **toutes les 10 minutes** — une donnée périmée
l'est depuis des heures, interroger BigQuery plus souvent ne détecterait rien plus tôt
et coûterait des octets scannés.

### Trois choix qui méritent d'être explicités

**`noDataState: Alerting` sur la liveness.** Pour les autres règles, l'absence de donnée
est neutre. Ici elle *est* la panne : pas de métrique = pas de process.

**La fraîcheur est mesurée sur la donnée, pas sur l'orchestrateur.** Si la machine est
éteinte à 05:00, Dagster ne tourne pas, donc aucun run en échec n'existe, donc une
supervision fondée sur les runs ne voit rien. Seule la donnée elle-même révèle le trou.

**Anti-bruit, réglé dans l'arbre de routage plutôt que dans les règles** : regroupement
par `alertname` + `component` (une panne touchant plusieurs tables produit *une*
notification, pas neuf), `group_wait` de 30 s pour laisser les alertes corrélées se
rassembler, `repeat_interval` de 4 h ramené à 1 h pour les critiques — un incident non
résolu se rappelle au bon souvenir sans saturer le canal.

### Canal de notification

Slack si `SLACK_WEBHOOK_URL` est renseigné dans le `.env`, e-mail (`ALERT_EMAIL`) sinon.
Dans les deux cas les alertes restent consultables dans Grafana (*Alerting → Alert rules*).
Le basculement est automatique : un récepteur Slack sans URL est refusé au provisioning
et empêcherait Grafana de démarrer, `bootstrap.py` choisit donc le type de récepteur
selon ce qui est configuré.

---

## 6. Utilisation

```bash
make monitoring        # génère la conf depuis .env puis lance la stack
make stream            # alimente les métriques du flux temps réel
make monitoring-logs   # suit les logs des 3 conteneurs
make monitoring-down   # arrête (les séries Prometheus sont conservées)
```

- Grafana : <http://127.0.0.1:3001> — identifiants `GRAFANA_ADMIN_*` du `.env`
- Prometheus : <http://127.0.0.1:9090> — état des cibles dans *Status → Targets*

Si le compte n'appartient pas au groupe `docker` :
`make monitoring DOCKER="sudo docker"`.

### Vérifier que la chaîne d'alerte fonctionne

```bash
# 1. Liveness : arrêter `make stream` -> « Flux temps réel arrêté » après 5 min.
# 2. Latence  : vérifier l'histogramme directement sur l'endpoint
curl -s localhost:9101/metrics | grep local_bike_stream_event_latency
# 3. Fraîcheur : simuler une donnée périmée sans attendre 26 h
#    dbt run --select mon_raw_freshness --vars '{raw_freshness_sla_hours: 0}'
#    -> `is_stale` passe à vrai sur les 9 tables, l'alerte se déclenche au tick suivant.
```

---

## 7. Limites connues

**La supervision ne veille que quand la machine est allumée.** Les trois conteneurs
tournent en local ; une panne survenant machine éteinte ne sera notifiée qu'au
redémarrage. C'est la même contrainte que le daemon Dagster, mais elle doit être dite.
Passage en production : héberger la stack sur une petite VM, ou pousser les métriques
vers Grafana Cloud (offre gratuite : 10 000 séries, 14 jours, alerting inclus).

**Le listener est lancé à la main.** `make stream` n'est pas un service : entre deux
sessions de travail, l'alerte « Flux temps réel arrêté » est donc un vrai positif pour
une situation normale. En production, il tournerait en conteneur ou sous systemd.

**Le coût BigQuery n'est pas supervisé.** Il demanderait
`INFORMATION_SCHEMA.JOBS_BY_PROJECT`, qui exige la permission `bigquery.jobs.listAll`
(rôle *BigQuery Resource Viewer*) — refusée au service account actuel, volontairement
limité à la lecture/écriture de données. Deux voies : élargir le rôle et ajouter une vue
`mon_bigquery_cost`, ou poser une **alerte de budget GCP**, native et suffisante ici.

**Les résultats des tests dbt ne sont pas dans le dashboard.** Ils vivent aujourd'hui
dans les logs de run et dans l'UI Dagster. Les y amener demanderait `--store-failures`
ou le package `elementary-data` — extension naturelle, non traitée dans cette version.
