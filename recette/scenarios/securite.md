# Scénarios de sécurité — SEC-01 à SEC-09

Vérification des règles posées en **section 4 du [`CLAUDE.md`](../../CLAUDE.md)** (« À
RESPECTER ABSOLUMENT ») et de l'exposition de l'infrastructure d'exploitation.

Une règle déclarée n'est pas une règle tenue. Ces scénarios existent pour transformer des
intentions (« aucun secret en clair ») en constats datés. Plusieurs sont des **tests
négatifs** : ils valident qu'une opération interdite **échoue bien**.

> Conventions et criticités : [`../README.md`](../README.md).

---

## 1. Secrets et credentials

### SEC-01 — Aucun secret dans l'historique Git 🔴 · auto (CI) ✅ *outillé*
**Outil** : `gitleaks` 8.30.1 (binaire épinglé), configuration [`.gitleaks.toml`](../../.gitleaks.toml)
**Exécution** : job `SEC-01` de [`security.yml`](../../.github/workflows/security.yml), ou
`make security` en local.
**Commande** : `gitleaks git --redact --no-banner --exit-code 1 --config .gitleaks.toml .`
**Périmètre** : **l'intégralité de l'historique**, pas seulement le dernier commit — un
secret retiré par un commit ultérieur reste lisible dans l'historique et doit être révoqué,
pas simplement supprimé.
**Résultat attendu** : **0 finding**. Sont particulièrement recherchés : clé privée de
service account GCP, mot de passe Supabase, webhook Slack, mot de passe admin Grafana.
**En cas d'échec** : anomalie **G1** — la correction consiste à *révoquer et faire tourner le
secret*, puis seulement ensuite à nettoyer l'historique.

> **Exception en vigueur — 1 occurrence historique.** Le commit initial `ed7aaf07`
> (24/06/2026) porte un `NEON_PASSWORD` valué dans `.env.example` et `CLAUDE.md`, retiré au
> commit `635bde9` lors de la migration de Neon vers Supabase. Le projet Neon étant
> abandonné, le credential n'ouvre plus aucun accès : l'historique n'est pas réécrit (un
> `filter-repo` réécrirait tous les SHA depuis l'origine pour un secret mort) et l'exception
> est portée par une allowlist **scopée à ce seul commit** dans `.gitleaks.toml`. Vérifié le
> 11/09/2026 : la configuration du projet détecte exactement autant qu'une configuration par
> défaut — toute fuite dans un autre commit reste bloquante.

### SEC-02 — Efficacité du `.gitignore` 🔴 · auto (CI) ✅ *outillé*
**Outil** : [`recette/checks/sec_02_gitignore.sh`](../checks/sec_02_gitignore.sh) — git seul,
aucune dépendance externe.
**Exécution** : job `SEC-02` de [`security.yml`](../../.github/workflows/security.yml), ou
`make security` en local.

Le script contrôle trois propriétés et sort en erreur si l'une échoue :
1. **aucun fichier sensible suivi** par git (`git ls-files`) — un fichier ignoré n'est jamais
   indexé, donc tout résultat signifie qu'un secret a été commité ;
2. **chaque chemin sensible est effectivement ignoré** (`git check-ignore`), et `.env.example`
   reste versionné (règle de négation `!.env.example`) ;
3. **`.env.example` ne porte aucune valeur de secret** : `SUPABASE_PASSWORD`,
   `GCP_PROJECT_ID`, `SLACK_WEBHOOK_URL` et `GRAFANA_ADMIN_PASSWORD` doivent y être
   **présents** (documentation) mais **vides**. Les variables de configuration (hôte, port,
   région) conservent une valeur d'exemple.

**Exceptions déclarées** : `secrets/.gitkeep` et `secrets/README.md` sont volontairement
versionnés — ils ne contiennent aucun credential et documentent l'emplacement attendu de la
clé de service account.

**Rappel du périmètre protégé** : `.env` et `.env.*`, `secrets/`, `*-sa.json`,
`service-account*.json`, `*credentials*.json`, `monitoring/generated/` — ce dernier contenant
la clé privée du service account BigQuery rendue depuis `.env` par `monitoring/bootstrap.py`.

### SEC-08 — Hygiène des secrets en CI 🔴 · manuel
**Étapes** : inspecter les logs d'un run GitHub Actions terminé.
**Résultat attendu** :
- aucune valeur de secret n'apparaît en clair (masquage GitHub effectif sur `GCP_SA_KEY`,
  `GCP_PROJECT_ID`) ;
- la clé de service account écrite sur le runner est **supprimée en fin de job**, y compris
  lorsque le job échoue — l'étape `Cleanup credentials` porte bien `if: always()` ;
- le heredoc d'écriture de la clé est quoté (`<<'EOF'`), donc non interprété par le shell.

---

## 2. Chiffrement et transport

### SEC-04 — SSL imposé sur la connexion Supabase 🔴 · manuel *(test négatif)*
**Étapes** : tenter une connexion à Supabase avec `sslmode=disable`.
**Résultat attendu** : la connexion est **refusée par le serveur**. Un succès signifierait
que le transport peut se faire en clair et constituerait une anomalie G1.
**Contrôle complémentaire** : la connexion nominale (`sslmode=require`) réussit et la session
est bien chiffrée (`SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();` → `true`).

### SEC-07 — TLS de l'accès distant 🟠 · manuel
**Périmètre** : nginx en frontal des UI d'exploitation (Dagster, Grafana).
**Étapes** : `openssl s_client -connect <hôte>:443` et test des versions de protocole.
**Résultat attendu** : certificat Let's Encrypt valide et non expiré (échéance suivie dans
[`exploitation/README.md`](../../exploitation/README.md) § 4) ; **TLS 1.2 minimum**, TLS 1.0
et 1.1 refusés ; redirection HTTP → HTTPS active.

---

## 3. Privilèges et exposition

### SEC-05 — Moindre privilège du service account BigQuery 🔴 · manuel *(test négatif)*
**Étapes** : avec les credentials de `secrets/gcp-sa.json`, tenter une opération **hors
périmètre** — par exemple supprimer un dataset non géré par le pipeline, ou lire un dataset
d'un autre projet.
**Résultat attendu** : `Permission denied`. Le service account ne doit porter que les rôles
nécessaires à la lecture/écriture des datasets `local_bike_*` et à l'exécution de jobs de
requête. Un compte disposant de `Owner` ou d'`Editor` au niveau projet est une anomalie G1 :
la compromission d'une clé y donnerait un accès total.

### SEC-09 — Datasets BigQuery non publics 🔴 · auto
**Étapes** : inspecter les IAM de chaque dataset `local_bike_*`.
**Résultat attendu** : **aucune** occurrence de `allUsers` ni `allAuthenticatedUsers` ; les
accès se limitent au service account du pipeline, au compte propriétaire et à celui utilisé
par Looker Studio pour la lecture des tables `rpt_*`.

### SEC-06 — Durcissement des interfaces d'exploitation 🟠 · manuel
**Périmètre** : Grafana (`:3001`), Prometheus (`:9090`), Dagster (`:3000`).
**Résultat attendu** :
- les identifiants Grafana par défaut (`admin`/`admin`) sont **changés** —
  `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` renseignés dans `.env` ;
- les trois services écoutent sur **127.0.0.1** et ne sont joignables de l'extérieur que via
  nginx + TLS (SEC-07), sur LAN ou VPN ;
- aucune anonymisation d'accès (`auth.anonymous`) n'est activée sur Grafana ;
- le mot de passe Grafana figure au calendrier de rotation des secrets d'exploitation.

> **Point ouvert détecté le 11/09/2026 (anomalie G2).**
> [`monitoring/docker-compose.yml`](../../monitoring/docker-compose.yml) déclare
> `GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}` : en l'absence de la
> variable, Grafana démarre avec **admin/admin**, sans avertissement. `.env.example` a été
> corrigé (la variable y est désormais vide plutôt que valuée à `admin`), mais le repli du
> compose subsiste. Correction proposée : remplacer le repli par une substitution
> obligatoire (`${GRAFANA_ADMIN_PASSWORD:?renseigner GRAFANA_ADMIN_PASSWORD dans .env}`),
> qui fait échouer `make monitoring` avec un message explicite au lieu de démarrer une UI
> ouverte. Arbitrage à confirmer : ce durcissement rend la variable obligatoire pour tout
> nouveau déploiement.

---

## 4. Chaîne d'approvisionnement

### SEC-03 — Vulnérabilités des dépendances 🟠 · auto (CI) ✅ *outillé*
**Outil** : `pip-audit` 2.10.1
**Exécution** : job `SEC-03` de [`security.yml`](../../.github/workflows/security.yml) — à
chaque PR **et chaque lundi 06:00 UTC** : une dépendance saine au dernier merge ne le reste
pas indéfiniment, une CVE publiée entre deux PR doit être vue sans attendre.
**Commande** : `uv tool run --from pip-audit==2.10.1 pip-audit -r requirements.txt`
**Résultat attendu** : aucune vulnérabilité de sévérité **haute ou critique** ; les
vulnérabilités moyennes ou basses sont consignées avec une échéance de montée de version.
**Périmètre surveillé** : `dlt[bigquery]`, `sqlalchemy`, `psycopg2-binary`, `dbt-bigquery`,
`dagster` et ses deux compagnons, `google-cloud-bigquery`, `prometheus-client`,
`python-dotenv`.
**Articulation avec l'exploitation** : les montées de version correspondantes sont planifiées
dans l'échéancier de [`exploitation/README.md`](../../exploitation/README.md) § 3.2 ; ce
scénario est le déclencheur, la feuille de route est le calendrier.

---

## Synthèse de couverture

| Règle de sécurité du projet | Scénario(s) |
|---|---|
| Aucun secret en clair dans le code ni dans Git | SEC-01, SEC-02, SEC-08 |
| La clé service account n'est jamais commitée | SEC-01, SEC-02 |
| `.env.example` sans valeurs | SEC-02 |
| Supabase impose le SSL (`sslmode=require`) | SEC-04 |
| Accès entrepôt restreint au strict nécessaire | SEC-05, SEC-09 |
| Interfaces d'exploitation non exposées | SEC-06, SEC-07 |
| Dépendances à jour | SEC-03 |

**Outillage** : SEC-01, SEC-02 et SEC-03 sont automatisés par le workflow
[`security.yml`](../../.github/workflows/security.yml) (un job par scénario, pour qu'un job
rouge désigne directement le scénario en échec) et rejouables en local par `make security`.
Ces trois contrôles ne consomment aucun secret et ne touchent ni Supabase ni BigQuery : ils
tournent donc aussi sur les forks. SEC-04 à SEC-09 restent manuels.
