# CLAUDE.md — Skypeo Scrapper

Mémoire du projet pour Claude Code. Mis à jour au fil des sessions.

## Objectif du projet

Scraper d'emails professionnels pour des entreprises françaises, 100% gratuit.
Piloté via une interface Streamlit.

**Cible boss :**
- Code NAF 5610A + 5610C (restauration) → ~71 000 emails
- Code NAF 9602B (soins/beauté) → ~16 000 emails

---

## Architecture

```
skypeo-scrapper-main/
├── app.py           # Interface Streamlit (1 onglet)
├── scraper.py       # Toute la logique métier
├── requirements.txt
├── .env.example     # Modèle de configuration des clés API
└── .env             # Clé API réelle (ne pas commiter)
```

### Lancement
```bash
pip install -r requirements.txt
streamlit run app.py
```

---

## Coûts — 100% gratuit

| Composant | Coût |
|---|---|
| Google Places API | $0 (couvert par $200 crédit/mois Google) |
| Scraping site web | $0 |
| SMTP email guessing | $0 |
| Claude Code | $0 (tourne en local, aucun token) |

Google Places : ~4 920 requêtes min × $0.017 = ~$84 → dans le crédit gratuit mensuel.

---

## Interface — 1 onglet : 🚀 Collecte Massive

- Boucle automatique : 492 villes françaises × types d'activité
- Sélecteur de secteurs (Restauration / Beauté & Spa)
- Filtre par département ou France entière
- Scraping email depuis le site web (15 workers parallèles)
- Fallback SMTP guessing si pas d'email trouvé sur le site
- Sauvegarde CSV en temps réel avec reprise possible
- Téléchargement CSV depuis l'UI

---

## Clé API nécessaire

| Variable | Usage | Où l'obtenir |
|---|---|---|
| `GOOGLE_PLACES_API_KEY` | Google Places API (New) | console.cloud.google.com |

Fichier `.env` à la racine :
```
GOOGLE_PLACES_API_KEY=AIzaSy...
```
Ou dans Streamlit secrets (déploiement cloud).

---

## Classes principales — `scraper.py`

### `MassiveCollector(output_path, sectors, dept_filter)`
Collecte massive via Google Places API.
- `sectors` : liste de clés parmi `SECTORS` (`"restauration"`, `"beaute"`)
- `dept_filter` : liste de codes département ou `None` (France entière)
- `start()` / `stop()` / `get_state()` / `load_existing()`
- Scraping emails en parallèle (`_scrape_emails_batch`, 15 workers)
- CSV : `place_id, nom, adresse, telephone, email, site_web, note_google, nombre_avis, statut, types, ville, dept, secteur, type_business`

---

## Secteurs configurés (`SECTORS`)

```python
"restauration": types = ["restaurant", "cafe", "bar", "fast_food", "meal_takeaway"]
"beaute":        types = ["beauty_salon", "spa", "hair_care", "hair_salon", "nail_salon"]
```
→ Ces types Google Places correspondent aux NAF 5610A/5610C et 9602B.

---

## Pipeline email (100% gratuit)

### Étape 1 — Scraping site web (`scrape_email_from_website`)
1. Visite la page principale
2. Sonde 16 pages de contact (`_CONTACT_PATHS`) dont `/mentions-legales`
3. Priorité aux balises `<a href="mailto:...">`
4. Fallback regex sur HTML brut
5. Filtre emails gratuits + domaines techniques

### Étape 2 — SMTP guessing (`find_email_by_smtp`) — si étape 1 échoue
1. Résout le MX record du domaine via DNS (`dnspython`)
2. Teste les patterns courants : `contact@`, `info@`, `accueil@`, `direction@`...
3. Vérifie chaque adresse via SMTP RCPT TO (sans envoyer de message)
4. Retourne le premier email confirmé

### Patterns SMTP testés (`_SMTP_PATTERNS`)
```
contact, info, bonjour, hello, accueil,
direction, gerant, manager, admin,
reservation, reservations, booking,
restaurant, resto, salon, spa, pro
```

---

## Décisions d'architecture

- **Dropcontact supprimé** — payant (~0.10€/contact), remplacé par SMTP guessing gratuit
- **Pappers abandonné** — nécessitait une clé API, abandonné avant même d'être mis en prod
- **Google Places** suffit pour couvrir restauration/beauté (types = codes NAF cibles)
- **15 workers** pour le scraping email en parallèle (était 10)
- **dnspython** ajouté dans requirements.txt pour la résolution MX
- Compatibilité Python 3.7+ (`from __future__ import annotations`)
- Si `dnspython` non installé, le SMTP guessing est silencieusement désactivé

---

## Points d'amélioration futures

- Ajouter des villes moyennes manquantes pour améliorer la couverture
- Tester un scraping Pages Jaunes comme source complémentaire (pas de clé API)
- Améliorer la détection d'emails obfusqués (JS, encodage caractères)
- Certains serveurs SMTP bloquent le port 25 — ajouter détection catch-all
