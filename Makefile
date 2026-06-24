# Makefile — Local Bike
# Charge .env automatiquement et passe --profiles-dir . à dbt.
# Utilise les binaires du venv (.venv) : pas besoin d'activer le venv au préalable.
.DEFAULT_GOAL := help

VENV     := .venv/bin
LOADENV  := set -a && . ./.env && set +a
PROFILES := --profiles-dir .
DAGSTER_HOME := $(CURDIR)/.dagster_home

.PHONY: help install ingest deps debug run test build docs clean dagster

help: ## Liste les commandes disponibles
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## Crée le venv et installe les dépendances (uv)
	uv venv .venv
	uv pip install -r requirements.txt

ingest: ## Ingestion Supabase -> BigQuery (dataset local_bike_raw)
	$(LOADENV) && $(VENV)/python ingestion/load_supabase_to_bq.py

deps: ## Installe les packages dbt (dbt_utils)
	$(LOADENV) && $(VENV)/dbt deps $(PROFILES)

debug: ## Vérifie la connexion BigQuery
	$(LOADENV) && $(VENV)/dbt debug $(PROFILES)

run: ## Construit tous les modèles dbt
	$(LOADENV) && $(VENV)/dbt run $(PROFILES)

test: ## Lance les tests dbt
	$(LOADENV) && $(VENV)/dbt test $(PROFILES)

build: ## dbt build (run + test)
	$(LOADENV) && $(VENV)/dbt build $(PROFILES)

docs: ## Génère et sert la documentation dbt
	$(LOADENV) && $(VENV)/dbt docs generate $(PROFILES) && $(VENV)/dbt docs serve $(PROFILES)

dagster: ## Lance l'orchestrateur Dagster (UI + daemon) sur http://127.0.0.1:3000
	@mkdir -p $(DAGSTER_HOME) && touch $(DAGSTER_HOME)/dagster.yaml
	$(LOADENV) && DAGSTER_HOME=$(DAGSTER_HOME) $(VENV)/dagster dev

clean: ## Nettoie les artefacts dbt (target/, dbt_packages/)
	$(LOADENV) && $(VENV)/dbt clean $(PROFILES)
