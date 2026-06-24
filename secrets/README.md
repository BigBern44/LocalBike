# secrets/

Dossier pour les credentials **locaux uniquement**. Son contenu est ignoré par git
(voir `.gitignore`), seuls ce `README.md` et `.gitkeep` sont versionnés.

## Clé service account BigQuery

Placer ici le fichier JSON de la clé du service account GCP, nommé :

```
secrets/gcp-sa.json
```

C'est le chemin référencé par `GOOGLE_APPLICATION_CREDENTIALS` dans `.env`.

⚠️ Ne jamais committer ce fichier. Vérifier avec `git status` avant chaque `git add`.
