# Cahier de recettes et de tests — Local Bike

Plan de validation du pipeline ELT **Supabase → BigQuery → dbt → Looker Studio** :
scénarios de test, résultats attendus, procédure de non-régression (compétence **C4.4.1**).

> Complément de **[`monitoring/README.md`](../monitoring/README.md)** (*comment on voit* l'état
> du système) et de **[`exploitation/README.md`](../exploitation/README.md)** (*ce qu'on fait*
> pour qu'il tienne). Ce document répond à une troisième question : **comment on prouve que
> ça marche**, avant la mise en production et à chaque modification.
>
> Révision : **v1.0 — 11/09/2026** · Prochaine relecture : à chaque release · Responsable : Data Engineer

---

## 1. Objet et périmètre

### 1.1 Ce que ce cahier valide

Le pipeline livre de la donnée à une équipe Opérations qui prend des décisions dessus. Une
anomalie n'y est pas visible : un revenu faux s'affiche aussi joliment qu'un revenu juste.
Ce cahier existe pour que **chaque fonctionnalité livrée soit rattachée à au moins un
scénario de test daté, rejouable et tracé**.

| Composant | Couvert | Scénarios |
|---|---|---|
| Ingestion batch (dlt, 9 tables) | ✅ | F-01 → F-06 |
| Flux temps réel (LISTEN/NOTIFY → BigQuery) | ✅ | F-07 → F-09 |
| Transformation dbt (25 modèles, 5 couches) | ✅ | F-10 → F-13, S-06 → S-10 |
| Orchestration Dagster (job, schedule, sensor) | ✅ | F-14 → F-17, S-11 |
| Supervision & alertes (6 règles Grafana) | ✅ | F-18 → F-23 |
| Dashboard Looker Studio (6 axes d'analyse) | ✅ | F-24 → F-29 |
| CI/CD (GitHub Actions) | ✅ | F-30, F-31 |
| Code Python (ingestion, métriques, flux) | ✅ | S-01 → S-05 |
| Sécurité (secrets, SSL, privilèges, exposition) | ✅ | SEC-01 → SEC-09 |

### 1.2 Ce qui est hors périmètre

- **Tests de charge / montée en volume** : la volumétrie source est de ~9 000 lignes et
  n'évolue pas (dataset figé). Un test de charge mesurerait la performance de BigQuery,
  pas celle du pipeline. Réévaluer si la source devient vivante.
- **Tests de compatibilité navigateur** du dashboard : Looker Studio est un produit tiers.
- **Pénétration applicative** : aucun applicatif exposé n'est développé ici ; la surface
  d'attaque se limite aux UI d'exploitation (Grafana, Dagster), traitées en SEC-06/SEC-07.

---

## 2. Stratégie de test

### 2.1 Les trois familles

| Famille | Question posée | Moyens | Volume |
|---|---|---|---|
| **Fonctionnel** | Le pipeline fait-il ce qu'on attend de lui, de bout en bout ? | Exécution réelle (`make ingest`, `make build`, UI Dagster, Grafana, Looker Studio) | 31 scénarios |
| **Structurel** | Le code et les modèles sont-ils corrects *par construction* ? | pytest, dbt unit tests, `dbt compile`, `dagster definitions validate` | 14 scénarios |
| **Sécurité** | Les règles de la section 4 du `CLAUDE.md` tiennent-elles vraiment ? | gitleaks, pip-audit, tests négatifs (connexion refusée, privilège refusé) | 9 scénarios |

La distinction fonctionnel / structurel se joue sur le **point d'observation** : le
fonctionnel observe un résultat métier (« le revenu du magasin X sur septembre est de Y »),
le structurel observe une propriété du code (« la fonction `_load_strategy` renvoie `merge`
pour `orders` »). Les deux sont nécessaires : un modèle SQL peut être structurellement
correct et fonctionnellement faux s'il est branché sur la mauvaise source.

### 2.2 Tests automatisés et tests manuels

Un scénario est **automatisé** quand son résultat attendu est vérifiable par une machine
sans jugement humain. Les autres restent manuels et sont tracés par capture dans le
procès-verbal de campagne.

| Mode | Nombre | Rejoué | Preuve |
|---|---|---|---|
| **Automatisé** | 31 | À chaque PR (CI) ou à chaque campagne (`make recette`) | Run GitHub Actions / rapport `recette/resultats/` |
| **Manuel** | 23 | À chaque release | Capture ou sortie de commande jointe au PV |

Parmi les 31 scénarios automatisables, **28 sont outillés à ce jour** :

| Outillage | Scénarios |
|---|---|
| `dbt build` + CI `dbt-ci.yml` | F-10, F-12, F-30, F-31, S-06 → S-09, S-10, S-13 |
| *Asset checks* et définitions Dagster | F-02, F-15, F-17, S-11 |
| CI `security.yml` (`make security`) | SEC-01, SEC-02, SEC-03 |
| CI `tests-python.yml` (`make test-python`) | S-01 → S-05 |
| Campagne locale `make fonctionnels` | F-01, F-03 → F-06, F-11 |

Les **3 autres restent à outiller** : continuité de la lineage (S-12), complétude
documentaire (S-14) et IAM des datasets BigQuery (SEC-09). Tant qu'un scénario
n'est pas outillé, il se déroule manuellement : le plan est complet, son automatisation est
progressive.

Les scénarios manuels ne sont pas un pis-aller : déclencher réellement une alerte Slack ou
lire un chiffre sur le dashboard sont des vérifications qu'aucune assertion automatique ne
remplace. Ils sont simplement plus coûteux, donc rejoués moins souvent.

### 2.3 Les tests de données ne suffisent pas

Le projet portait déjà, avant ce cahier, **145 tests dbt** (142 génériques et 3 singuliers
métier) ; s'y ajoutent désormais **4 unit tests** sur la logique de calcul du revenu.
Ensemble, ils valident la *donnée produite* et la *logique qui la produit*. Ils valident la *donnée produite* : unicité des clés, intégrité
référentielle, bornes de valeurs, formule de revenu. C'est nécessaire et ça constitue
l'ossature du volet structurel — mais ça ne dit rien de l'idempotence de l'ingestion, de la
latence du flux temps réel, du déclenchement d'une alerte ou de l'absence de secret dans le
dépôt. D'où les deux autres familles.

---

## 3. Environnements de recette

| Environnement | Cible BigQuery | Usage | Données |
|---|---|---|---|
| **CI** | `local_bike_ci_*` (target `ci`) | Non-régression automatique à chaque PR | Réelles, en lecture depuis `local_bike_raw` |
| **CI — sans entrepôt** | aucune | `pytest` et volet sécurité : ni Supabase, ni BigQuery, ni secret | Aucune |
| **Local** | `local_bike_*` (target `dev`) | Campagne de recette complète, scénarios manuels | Réelles |
| **Production** | `local_bike_*` (target `prod`) | Aucun test destructif | Réelles |

**Règle** : aucun scénario de ce cahier ne modifie la donnée de production. Les scénarios qui
écrivent dans la source (F-03, F-04, F-07, F-08 — mouvements de stock, modification de
référentiel) opèrent sur Supabase en **rétablissant l'état initial** en fin de scénario ;
chaque fiche précise sa procédure de remise en état. Les Parquet de `data_raw/` permettent
un rechargement complet par `make seed` en cas de doute.

---

## 4. Nomenclature

```
F-nn    scénario fonctionnel      -> recette/scenarios/fonctionnels.md
S-nn    scénario structurel       -> recette/scenarios/structurels.md
SEC-nn  scénario de sécurité      -> recette/scenarios/securite.md
```

Chaque test automatisé référence son identifiant **dans son nom ou son docstring**
(`def test_load_strategy_merge_sur_transactionnelles():  # S-01`), de sorte que la trace
entre le plan et l'exécution soit lisible dans les deux sens.

**Criticité d'un scénario** — détermine si un échec est bloquant :

| Niveau | Signification | Conséquence d'un échec |
|---|---|---|
| 🔴 **Bloquant** | Fausse la donnée livrée ou expose un secret | Livraison refusée |
| 🟠 **Majeur** | Dégrade le service sans fausser la donnée | Livraison possible avec anomalie ouverte et échéance |
| 🟡 **Mineur** | Confort, documentation, ergonomie | Consigné, traité au fil de l'eau |

---

## 5. Matrice de traçabilité

Chaque fonctionnalité attendue (étapes de la section 8 et axes d'analyse de la section 9
du [`CLAUDE.md`](../CLAUDE.md)) et sa couverture.

| # | Fonctionnalité attendue | Scénarios | Criticité max |
|---|---|---|---|
| 1 | Setup : datasets BigQuery, service account, secrets hors dépôt | SEC-01, SEC-02, SEC-05 | 🔴 |
| 2 | Ingestion des 9 tables Supabase → `local_bike_raw` | F-01, F-02, F-05, F-06, SEC-04 | 🔴 |
| 2b | Chargement hybride idempotent (`merge` / `replace`) | F-03, F-04, S-01 | 🔴 |
| 3 | Sources dbt pointant vers `local_bike_raw` | S-10, S-12 | 🟠 |
| 4 | Staging — 1 modèle par table, typage explicite | F-10, S-10 | 🔴 |
| 5 | Intermediate — enrichissement + calcul du revenu | S-06 → S-09, F-12 | 🔴 |
| 6 | Marts — modèle en étoile (5 dimensions, 3 faits) | F-10, F-11, F-12 | 🔴 |
| 7 | Reporting — tables plates lisibles sans blend | F-13, F-24 → F-29 | 🟠 |
| 8 | Tests sur chaque couche (génériques + singuliers) | F-10, F-16, S-13 | 🔴 |
| 9 | Documentation des modèles et colonnes | S-14 | 🟡 |
| 10 | Dashboard Looker Studio — 1 page par axe | F-24 → F-29 | 🟠 |
| 11 | GitHub — CI, PR, peer-review | F-30, F-31, SEC-08 | 🟠 |
| 12 | Orchestration — job, schedule quotidien, sensor | F-14 → F-17, S-11 | 🟠 |
| 13 | Flux temps réel — alertes de stock | F-07 → F-09, S-04, S-05 | 🟠 |
| 14 | Supervision — 6 règles d'alerte | F-18 → F-23 | 🟠 |
| **Axe 1** | Revenu par magasin / état / période | F-24 | 🔴 |
| **Axe 2** | Top produits / catégories / marques | F-25 | 🟠 |
| **Axe 3** | Stocks, ruptures, rotation | F-26, F-08 | 🟠 |
| **Axe 4** | Clients — panier moyen, fréquence, géographie | F-27 | 🟠 |
| **Axe 5** | Livraison — délais, retards, statuts | F-28 | 🟠 |
| **Axe 6** | Staff — ventes par vendeur / magasin | F-29 | 🟠 |

Aucune ligne sans scénario : c'est le critère « le cahier reprend l'ensemble des
fonctionnalités attendues ».

---

## 6. Critères d'acceptation

La recette est **prononcée** (go) si et seulement si :

1. **100 % des scénarios 🔴 bloquants** sont au statut ✅ ;
2. **aucune anomalie de gravité 1** (bloquante) n'est ouverte ;
3. les scénarios 🟠 majeurs en échec font l'objet d'une **anomalie datée avec échéance** ;
4. la CI est verte sur le commit recetté ;
5. le procès-verbal de campagne est signé et versionné dans `recette/resultats/`.

Une recette prononcée **avec réserves** est possible si 1 et 2 sont satisfaits mais que des
scénarios 🟠 restent en échec : les réserves sont listées nominativement dans le PV.

---

## 7. Procédure de non-régression

C'est le cœur du dispositif : détecter qu'une modification casse quelque chose **qui
marchait avant**.

### 7.1 À chaque Pull Request — automatique

Le workflow [`dbt-ci.yml`](../.github/workflows/dbt-ci.yml) rejoue, sur des datasets
`local_bike_ci_*` isolés de la production :

```
dbt deps → dbt debug --target ci → dbt build --target ci
```

`dbt build` reconstruit les 25 modèles **et** exécute les 149 contrôles (145 tests de
données + 4 unit tests) dans l'ordre du DAG :
un test rouge coupe la propagation aval au lieu de livrer des marts douteuses. Le merge sur
`main` est conditionné à ce run vert.

En parallèle, le workflow [`security.yml`](../.github/workflows/security.yml) rejoue le volet
sécurité — un job par scénario (SEC-01 secrets, SEC-02 `.gitignore`, SEC-03 dépendances) —
à chaque PR, à chaque push sur `main` et **chaque lundi matin**, ce dernier rythme couvrant
les vulnérabilités publiées entre deux contributions.

S'y ajoute [`tests-python.yml`](../.github/workflows/tests-python.yml) : 53 tests `pytest`
sur le code d'ingestion, de métriques et de flux (S-01 → S-05), sans secret ni connexion.

### 7.2 À chaque release — campagne complète

```bash
make recette          # exécute tous les scénarios automatisables, produit le rapport
```

`make recette` enchaîne les quatre volets dans l'ordre du moins au plus coûteux : suite
`pytest`, contrôles de sécurité, `dbt build` (modèles, tests de données et unit tests), puis
la campagne fonctionnelle — qui a besoin des marts construits par l'étape précédente.

La campagne fonctionnelle écrit son propre rapport horodaté dans
[`resultats/`](resultats/). Puis déroulé manuel des 23 scénarios non automatisables, et
consolidation dans le PV daté, à partir du
`recette/resultats/AAAA-MM-JJ_vX.Y.md` à partir du [gabarit](resultats/_gabarit.md).

### 7.3 Comparaison entre campagnes

Deux campagnes successives sur le même plan rendent la régression **lisible** : tout
scénario passé de ✅ à ❌ entre deux PV est une régression, et le diff Git entre les deux
commits recettés en donne le périmètre. C'est la raison pour laquelle le plan
(`scenarios/`) et les résultats (`resultats/`) sont séparés : le plan est stable, les
résultats sont datés.

---

## 8. Gestion des anomalies

Toute anomalie détectée ouvre une **issue GitHub** portant le label `anomalie-recette` et
référençant l'identifiant du scénario.

| Gravité | Définition | Traitement |
|---|---|---|
| **G1 — Bloquante** | Donnée fausse livrée, secret exposé, pipeline à l'arrêt | Correction avant livraison. Recette suspendue. |
| **G2 — Majeure** | Fonctionnalité dégradée, contournement existant | Correction sous 5 jours ouvrés, réserve au PV |
| **G3 — Mineure** | Confort, documentation, libellé | Backlog |

**Fiche d'anomalie** — contenu minimal :

```
ID scénario     : F-03
Gravité         : G1
Constaté le     : 11/09/2026, commit 9b61620, environnement local
Attendu         : volumétrie identique après second `make ingest`
Obtenu          : +1 247 lignes sur public_orders
Reproductible   : oui, systématiquement
Impact métier   : revenu surévalué sur tous les axes du dashboard
Correction      : <lien PR>   Re-testé le : <date>  Statut : <ouvert|corrigé|fermé>
```

Le champ **impact métier** n'est pas décoratif : c'est lui qui arbitre entre G1 et G2.

---

## 9. Journal des campagnes

| Date | Version | Commit | Environnement | ✅ | ❌ | Verdict | PV |
|---|---|---|---|---|---|---|---|
| — | v1.0 | — | — | — | — | *à exécuter* | — |

*Première campagne à dérouler après la mise en place des tests automatisés (chantiers 1 à 3).*

---

## 10. Limites connues de ce plan

Trois choix assumés, à rediscuter si le contexte change :

- **Pas de jeu de données de test dédié.** Les scénarios s'exécutent sur la donnée réelle
  BikeStores, figée et non confidentielle. Un jeu synthétique apporterait de la
  reproductibilité mais doublerait le coût de maintenance pour un dataset qui ne bouge pas.
  Les dbt unit tests (S-06 → S-09) couvrent le besoin là où il est réel : la logique de
  calcul, testée sur des entrées choisies.
- **Pas de test de restauration automatisé.** Le RTO de 4 h annoncé dans
  [`exploitation/README.md`](../exploitation/README.md#L53) repose sur un test de
  restauration **trimestriel manuel**. Automatiser une reprise complète supposerait un
  second environnement, hors budget d'une PME de 3 magasins.
- **Couverture de code non mesurée.** Le volume de code Python est faible (~4 modules) ;
  un taux de couverture y serait plus décoratif qu'informatif. Les scénarios S-01 → S-05
  ciblent les fonctions à risque, pas l'exhaustivité.
