# Procès-verbal de recette — Local Bike

> Gabarit. Copier en `AAAA-MM-JJ_vX.Y.md` au lancement d'une campagne, puis remplir au fil
> du déroulé. Un PV est **immuable une fois signé** : une nouvelle campagne = un nouveau
> fichier. C'est cette accumulation qui rend les régressions lisibles d'une version à l'autre.

| | |
|---|---|
| **Version recettée** | vX.Y |
| **Commit** | `<sha>` — branche `<branche>` |
| **Environnement** | local / CI |
| **Date de campagne** | JJ/MM/AAAA |
| **Testeur** | <nom> |
| **Run CI de référence** | <lien GitHub Actions> |

---

## 1. Synthèse

| Famille | Total | ✅ Passés | ❌ Échoués | ⏭️ Non joués |
|---|---|---|---|---|
| Fonctionnels (F) | 31 | | | |
| Structurels (S) | 14 | | | |
| Sécurité (SEC) | 9 | | | |
| **Total** | **54** | | | |

**Verdict** : ☐ Recette prononcée ☐ Prononcée avec réserves ☐ Refusée

**Motivation du verdict** (critères de la section 6 du cahier) :

> …

---

## 2. Résultats détaillés

Un scénario **non joué** doit être justifié (dépendance indisponible, chantier non livré) —
l'absence de justification vaut échec.

| ID | Scénario | Crit. | Attendu | Obtenu | Statut | Anomalie |
|---|---|---|---|---|---|---|
| F-01 | Extraction des 9 tables | 🔴 | 9 tables, volumétrie = source | | ☐ | |
| F-02 | Réconciliation bout-à-bout | 🔴 | 18 checks au vert | | ☐ | |
| F-03 | Idempotence `merge` | 🔴 | Volumétrie inchangée, 0 doublon | | ☐ | |
| … | | | | | | |

*(reprendre l'intégralité des scénarios de [`../scenarios/`](../scenarios/))*

---

## 3. Anomalies ouvertes à l'issue de la campagne

| ID | Scénario | Gravité | Description | Impact métier | Issue | Échéance |
|---|---|---|---|---|---|---|
| A-01 | | | | | | |

---

## 4. Écarts par rapport à la campagne précédente

Section vide pour la première campagne. Ensuite, **c'est la section la plus importante du
document** : elle isole les régressions.

| ID | Statut précédent | Statut actuel | Nature | Commentaire |
|---|---|---|---|---|
| | ✅ | ❌ | **Régression** | |
| | ❌ | ✅ | Correction | |
| | — | ✅ | Nouvelle couverture | |

---

## 5. Preuves jointes

Les scénarios manuels n'ont de valeur que par leur preuve. À joindre ou référencer :

- [ ] Sortie complète de `make recette` (rapport automatisé)
- [ ] Lien du run GitHub Actions vert sur le commit recetté
- [ ] Captures des notifications d'alerte déclenchées (F-18 → F-23)
- [ ] Captures des pages du dashboard avec la requête de contrôle associée (F-24 → F-29)
- [ ] Sorties des tests négatifs de sécurité (SEC-04, SEC-05)

---

## 6. Signature

| Rôle | Nom | Date | Avis |
|---|---|---|---|
| Data Engineer (testeur) | | | |
| Relecteur (peer-review) | | | |
