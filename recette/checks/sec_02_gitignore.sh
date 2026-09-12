#!/usr/bin/env bash
# SEC-02 — Efficacité du .gitignore (cf. recette/scenarios/securite.md).
#
# Vérifie trois propriétés, sans dépendance externe (git seul) :
#   1. aucun fichier sensible n'est SUIVI par git ;
#   2. chaque chemin sensible est bien IGNORÉ (règle .gitignore effective) ;
#   3. .env.example est versionné et ne porte AUCUNE valeur de secret.
#
# Sortie : 0 si tout est conforme, 1 sinon (détail sur stderr).
# Usage  : recette/checks/sec_02_gitignore.sh

set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

fail=0
ko() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; fail=1; }
ok() { printf '  \033[32m✓\033[0m %s\n' "$*"; }

echo "SEC-02 — Efficacité du .gitignore"

# ---- 1. Aucun fichier sensible suivi par git -------------------------------
# `git ls-files` ne liste que ce qui est INDEXÉ : un fichier ignoré n'y figure
# jamais. Un résultat non vide signifie qu'un secret a été commité (et doit être
# révoqué, pas seulement supprimé).
echo
echo "1. Fichiers sensibles suivis par git"
# Exceptions documentées : le dossier secrets/ est ignoré, mais son README et son
# .gitkeep sont volontairement versionnés (ils ne contiennent aucun credential).
tracked=$(git ls-files | grep -E '(^|/)\.env$|(^|/)\.env\.[^/]*$|^secrets/|-sa\.json$|^service-account.*\.json$|credentials.*\.json$|^monitoring/generated/' \
          | grep -vE '^\.env\.example$|^secrets/(\.gitkeep|README\.md)$' || true)
if [ -n "$tracked" ]; then
    ko "fichiers sensibles versionnés :"
    printf '      %s\n' $tracked >&2
else
    ok "aucun fichier sensible dans l'index"
fi

# ---- 2. Chemins sensibles effectivement ignorés ----------------------------
# `git check-ignore -q` répond 0 si le chemin est ignoré. On teste des chemins
# représentatifs, existants ou non (check-ignore n'exige pas l'existence).
echo
echo "2. Règles .gitignore effectives"
for path in .env .env.local secrets/gcp-sa.json service-account.json \
            my-credentials.json monitoring/generated/datasource.yml; do
    if git check-ignore -q "$path"; then
        ok "$path ignoré"
    else
        ko "$path N'EST PAS ignoré"
    fi
done
# Contrôle inverse : le gabarit doit rester versionné.
if git check-ignore -q .env.example; then
    ko ".env.example est ignoré (il doit être versionné)"
else
    ok ".env.example versionné (règle de négation active)"
fi

# ---- 3. .env.example sans valeur de secret ---------------------------------
# Les variables porteuses de secret doivent être présentes (documentation) mais
# VIDES. Les variables de configuration (host, port, région) gardent un exemple.
echo
echo "3. .env.example — variables porteuses de secret"
if [ ! -f .env.example ]; then
    ko ".env.example absent"
else
    for var in SUPABASE_PASSWORD GCP_PROJECT_ID SLACK_WEBHOOK_URL GRAFANA_ADMIN_PASSWORD; do
        line=$(grep -E "^${var}=" .env.example || true)
        if [ -z "$line" ]; then
            ko "$var absent de .env.example (variable non documentée)"
        elif [ "$line" != "${var}=" ]; then
            ko "$var porte une valeur : ${line}"
        else
            ok "$var présent et vide"
        fi
    done
fi

echo
if [ "$fail" -eq 0 ]; then
    echo "SEC-02 : CONFORME"
else
    echo "SEC-02 : NON CONFORME — voir les lignes ✗ ci-dessus" >&2
fi
exit "$fail"
