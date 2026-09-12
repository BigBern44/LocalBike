"""Génère la configuration de supervision dérivée du `.env`.

Principe : **ce qu'un humain édite est versionné, ce que Grafana et Prometheus
lisent est généré.** Les gabarits vivent dans `monitoring/grafana/` et
`monitoring/prometheus/` ; leur rendu atterrit dans `monitoring/generated/`, ignoré
par git.

Motif : la source de données BigQuery de Grafana a besoin de la **clé privée** du
service account. L'écrire dans un fichier de provisioning versionné violerait la
règle de sécurité du projet (aucun secret dans Git). Le rendu à la volée règle le
problème sans dégrader l'expérience : `make monitoring` reste une seule commande.

Sont générés :
  - `generated/provisioning/datasources/`   Prometheus (copie) + BigQuery (clé injectée)
  - `generated/provisioning/dashboards/`    déclaration du fournisseur de dashboards
  - `generated/provisioning/alerting/`      règles d'alerte + canal de notification
  - `generated/dashboards/`                 dashboard rendu (identifiant de projet GCP)
  - `generated/blackbox-*.json`             cibles des sondes réseau (hôte Supabase)

Usage : `make monitoring` (recommandé), ou `python monitoring/bootstrap.py`.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
GENERATED = ROOT / "generated"


def env(name: str, default: str = "", *, required: bool = False) -> str:
    """Lit une variable d'environnement, avec un message actionnable si elle manque."""
    value = os.getenv(name, default)
    if required and not value:
        sys.exit(f"✗ Variable d'environnement manquante : {name} (voir .env.example)")
    return value


def render(template: Path, destination: Path, substitutions: dict[str, str]) -> None:
    """Écrit `template` dans `destination` après substitution des marqueurs `__NOM__`."""
    content = template.read_text()
    for marker, value in substitutions.items():
        content = content.replace(marker, value)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content)


def service_account() -> dict[str, str]:
    """Charge la clé service account désignée par GOOGLE_APPLICATION_CREDENTIALS."""
    keyfile = env("GOOGLE_APPLICATION_CREDENTIALS", required=True)
    path = Path(keyfile)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.is_file():
        sys.exit(f"✗ Clé service account introuvable : {path}")
    return json.loads(path.read_text())


def build_datasources(project: str, location: str) -> None:
    """Source Prometheus (copie telle quelle) + source BigQuery (clé privée injectée)."""
    target_dir = GENERATED / "provisioning" / "datasources"
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "grafana/datasources/prometheus.yml", target_dir / "prometheus.yml")

    key = service_account()
    render(
        ROOT / "grafana/datasources/bigquery.yml.tmpl",
        target_dir / "bigquery.yml",
        {
            "__CLIENT_EMAIL__": key["client_email"],
            "__GCP_PROJECT__": project,
            "__GCP_LOCATION__": location,
            "__TOKEN_URI__": key.get("token_uri", "https://oauth2.googleapis.com/token"),
            # La clé PEM est multi-ligne : `json.dumps` produit un scalaire YAML
            # double-quoté dont les `\n` sont correctement échappés (YAML étant un
            # sur-ensemble de JSON, la valeur est relue à l'identique).
            "__PRIVATE_KEY__": json.dumps(key["private_key"]),
        },
    )


def build_dashboards(project: str, location: str) -> None:
    """Déclaration du fournisseur + rendu du dashboard (identifiant de projet GCP)."""
    provider_dir = GENERATED / "provisioning" / "dashboards"
    provider_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(ROOT / "grafana/dashboards/provider.yml", provider_dir / "provider.yml")

    render(
        ROOT / "grafana/dashboards/local-bike-pipeline.json.tmpl",
        GENERATED / "dashboards" / "local-bike-pipeline.json",
        {"__GCP_PROJECT__": project, "__GCP_LOCATION__": location},
    )


def build_alerting(project: str, location: str) -> None:
    """Règles d'alerte + canal de notification et arbre de routage."""
    target_dir = GENERATED / "provisioning" / "alerting"
    target_dir.mkdir(parents=True, exist_ok=True)

    render(
        ROOT / "grafana/alerting/rules.yml.tmpl",
        target_dir / "rules.yml",
        {"__GCP_PROJECT__": project, "__GCP_LOCATION__": location},
    )

    # Canal de notification : Slack si un webhook est fourni, e-mail sinon. Le
    # fichier est construit ici plutôt que rendu depuis un gabarit, car le *type* de
    # récepteur change — un récepteur Slack sans URL est refusé au provisioning et
    # empêcherait Grafana de démarrer.
    webhook = env("SLACK_WEBHOOK_URL")
    address = env("ALERT_EMAIL", "data-alerts@localhost")
    if webhook:
        receiver = f"""      - uid: data-alerts-primary
        type: slack
        disableResolveMessage: false
        settings:
          url: {json.dumps(webhook)}"""
        channel = "Slack"
    else:
        receiver = f"""      - uid: data-alerts-primary
        type: email
        disableResolveMessage: false
        settings:
          addresses: {json.dumps(address)}
          singleEmail: true"""
        channel = f"e-mail ({address}) — définir SLACK_WEBHOOK_URL pour basculer sur Slack"

    (target_dir / "contact-points.yml").write_text(
        f"""# Canal de notification et arbre de routage — généré par monitoring/bootstrap.py.
#
# Anti-bruit, réglé par l'arbre de routage plutôt que par les règles elles-mêmes :
#   - regroupement par `alertname` + `component` : une panne touchant plusieurs
#     tables produit UNE notification, pas neuf ;
#   - `group_wait` 30 s : laisse le temps aux alertes corrélées de se rassembler ;
#   - `repeat_interval` 4 h par défaut, ramené à 1 h pour les alertes critiques :
#     un incident non résolu se rappelle au bon souvenir sans saturer le canal.
apiVersion: 1

contactPoints:
  - orgId: 1
    name: data-alerts
    receivers:
{receiver}

policies:
  - orgId: 1
    receiver: data-alerts
    group_by: [alertname, component]
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    routes:
      - receiver: data-alerts
        object_matchers:
          # Éléments cités : un `=` nu est interprété par YAML comme le tag
          # `tag:yaml.org,2002:value`, et le fichier devient illisible.
          - ["severity", "=", "critical"]
        repeat_interval: 1h
"""
    )
    return channel


def build_blackbox_targets() -> str:
    """Cibles des sondes réseau, découvertes par fichier (file_sd) côté Prometheus."""
    host = env("SUPABASE_HOST", required=True)
    port = env("SUPABASE_PORT", "5432")
    GENERATED.mkdir(parents=True, exist_ok=True)

    (GENERATED / "blackbox-tcp.json").write_text(
        json.dumps(
            [{"targets": [f"{host}:{port}"],
              "labels": {"component": "source", "service": "supabase-postgresql"}}],
            indent=2,
        )
        + "\n"
    )

    # Le blackbox exporter tourne sur le réseau de l'hôte (cf. docker-compose.yml) :
    # `127.0.0.1` y désigne bien la machine, où tourne l'UI Dagster.
    (GENERATED / "blackbox-http.json").write_text(
        json.dumps(
            [{"targets": ["http://127.0.0.1:3000"],
              "labels": {"component": "orchestration", "service": "dagster-webserver"}}],
            indent=2,
        )
        + "\n"
    )
    return host


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    project = env("GCP_PROJECT_ID", required=True)
    location = env("GCP_LOCATION", "EU")

    build_datasources(project, location)
    build_dashboards(project, location)
    channel = build_alerting(project, location)
    host = build_blackbox_targets()

    print("✓ Configuration de supervision générée dans monitoring/generated/")
    print(f"    projet BigQuery     : {project} ({location})")
    print(f"    sonde TCP source    : {host}")
    print(f"    canal d'alerte      : {channel}")


if __name__ == "__main__":
    main()
