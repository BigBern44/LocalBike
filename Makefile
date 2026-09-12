# Makefile — Local Bike
# Charge .env automatiquement et passe --profiles-dir . à dbt.
# Utilise les binaires du venv (.venv) : pas besoin d'activer le venv au préalable.
.DEFAULT_GOAL := help

VENV     := .venv/bin
LOADENV  := set -a && . ./.env && set +a
PROFILES := --profiles-dir .
DAGSTER_HOME := $(CURDIR)/.dagster_home
# Surchargeable : `make monitoring DOCKER="sudo docker"` si l'utilisateur
# n'appartient pas au groupe `docker`.
DOCKER   ?= docker
COMPOSE  := $(DOCKER) compose -f monitoring/docker-compose.yml

.PHONY: help install seed ingest stream deps debug run test build docs clean dagster \
        monitoring monitoring-down monitoring-logs security test-unit test-python recette fonctionnels

help: ## Liste les commandes disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Crée le venv et installe les dépendances (uv)
	uv venv .venv
	uv pip install -r requirements.txt

seed: ## Recharge les Parquet de data_raw/ dans Supabase (schéma public)
	$(LOADENV) && $(VENV)/python ingestion/load_parquet_to_supabase.py

ingest: ## Ingestion dlt Supabase -> BigQuery (dataset local_bike_raw)
	$(LOADENV) && $(VENV)/python ingestion/load_supabase_to_bq.py

stream: ## Pipeline temps réel : mouvements de stock Supabase -> BigQuery (Ctrl+C pour arrêter)
	$(LOADENV) && $(VENV)/python -m ingestion.stream_stock_alerts

deps: ## Installe les packages dbt (dbt_utils)
	$(LOADENV) && $(VENV)/dbt deps $(PROFILES)

debug: ## Vérifie la connexion BigQuery
	$(LOADENV) && $(VENV)/dbt debug $(PROFILES)

run: ## Construit tous les modèles dbt
	$(LOADENV) && $(VENV)/dbt run $(PROFILES)

test: ## Lance les tests dbt
	$(LOADENV) && $(VENV)/dbt test $(PROFILES)

test-python: ## Lance la suite pytest (code d'ingestion et de flux, scénarios S-01 a S-05)
	$(VENV)/python -m pytest

test-unit: ## Lance les seuls unit tests dbt (logique SQL, scénarios S-06 a S-09)
	$(LOADENV) && $(VENV)/dbt test $(PROFILES) --select test_type:unit

build: ## dbt build (run + test)
	$(LOADENV) && $(VENV)/dbt build $(PROFILES)

docs: ## Génère et sert la documentation dbt
	$(LOADENV) && $(VENV)/dbt docs generate $(PROFILES) && $(VENV)/dbt docs serve $(PROFILES)

dagster: ## Lance l'orchestrateur Dagster (UI + daemon) sur http://127.0.0.1:3000
	@mkdir -p $(DAGSTER_HOME) && touch $(DAGSTER_HOME)/dagster.yaml
	$(LOADENV) && DAGSTER_HOME=$(DAGSTER_HOME) $(VENV)/dagster dev

monitoring: ## Lance la stack de supervision (Grafana 127.0.0.1:3001, Prometheus :9090)
	$(LOADENV) && $(VENV)/python monitoring/bootstrap.py
	$(LOADENV) && $(COMPOSE) up -d
	@echo ""
	@echo "  Grafana    -> http://127.0.0.1:3001   (dashboard « Santé du pipeline »)"
	@echo "  Prometheus -> http://127.0.0.1:9090   (cibles : Status > Targets)"
	@echo ""
	@echo "  Pour alimenter les métriques du flux temps réel : make stream"

monitoring-down: ## Arrête la stack de supervision (les données Prometheus sont conservées)
	$(COMPOSE) down

monitoring-logs: ## Suit les logs de la stack de supervision
	$(COMPOSE) logs -f --tail=50

recette: ## Campagne de recette complète (pytest, securite, dbt build, fonctionnels)
	@echo "=== Volet structurel — code (S-01 a S-05) ==="
	@$(VENV)/python -m pytest
	@echo ""
	@echo "=== Volet securite (SEC-01 a SEC-03) ==="
	@$(MAKE) --no-print-directory security
	@echo ""
	@echo "=== Volet structurel — donnees et logique (S-06 a S-13, F-10, F-12) ==="
	@$(LOADENV) && $(VENV)/dbt build $(PROFILES)
	@echo ""
	@echo "=== Volet fonctionnel (F-01, F-03 a F-06, F-11) ==="
	@$(LOADENV) && $(VENV)/python recette/run_fonctionnels.py

fonctionnels: ## Campagne fonctionnelle seule (F-01, F-03 a F-06, F-11) — ecrit dans local_bike_raw
	$(LOADENV) && $(VENV)/python recette/run_fonctionnels.py

security: ## Volet sécurité du cahier de recettes (SEC-01/02/03) — même contrôles qu'en CI
	@recette/checks/sec_02_gitignore.sh
	@echo ""
	@if command -v gitleaks >/dev/null 2>&1; then \
		echo "SEC-01 — Secrets dans l'historique Git"; \
		gitleaks git --redact --no-banner --exit-code 1 --config .gitleaks.toml . && echo "  SEC-01 : CONFORME"; \
	else \
		echo "SEC-01 — gitleaks absent : contrôle joué en CI (.github/workflows/security.yml)"; \
		echo "         installation : https://github.com/gitleaks/gitleaks/releases"; \
	fi
	@echo ""
	@echo "SEC-03 — Vulnérabilités des dépendances"
	@uv tool run --from pip-audit==2.10.1 pip-audit -r requirements.txt --progress-spinner off

clean: ## Nettoie les artefacts dbt (target/, dbt_packages/)
	$(LOADENV) && $(VENV)/dbt clean $(PROFILES)
