"""
scraper.py — Logique de scraping via Google Places API (New)

Endpoints utilisés :
  - Text Search  : POST https://places.googleapis.com/v1/places:searchText
  - Place Details : GET  https://places.googleapis.com/v1/places/{place_id}

Documentation officielle :
  https://developers.google.com/maps/documentation/places/web-service/text-search
"""

import csv
import io
import os
import time
from typing import Optional

import requests
from dotenv import load_dotenv

# Chargement de la clé API depuis le fichier .env
load_dotenv()

# ── Constantes ────────────────────────────────────────────────────────────────

PLACES_API_BASE = "https://places.googleapis.com/v1"

# Champs demandés lors du Text Search (impact sur la facturation Google)
TEXT_SEARCH_FIELDS = [
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.rating",
    "places.userRatingCount",
    "places.types",
    "places.businessStatus",
    "nextPageToken",  # nécessaire pour la pagination
]

# Champs demandés pour les détails d'un lieu (appel séparé, plus coûteux)
DETAIL_FIELDS = [
    "id",
    "displayName",
    "formattedAddress",
    "nationalPhoneNumber",
    "internationalPhoneNumber",
    "websiteUri",
    "rating",
    "userRatingCount",
    "types",
    "businessStatus",
]

# Colonnes du fichier CSV exporté
CSV_COLUMNS = [
    "nom",
    "adresse",
    "telephone",
    "site_web",
    "note_google",
    "nombre_avis",
    "statut",
    "types",
]


# ── Classe principale ─────────────────────────────────────────────────────────

class GooglePlacesScraper:
    """
    Interface vers Google Places API (New).

    Usage minimal :
        scraper = GooglePlacesScraper()
        places  = scraper.search_places("restaurant", "Lyon")
        csv     = scraper.to_csv_bytes(places)
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("GOOGLE_PLACES_API_KEY", "").strip()
        if not self.api_key:
            raise ValueError(
                "Clé API introuvable. "
                "Créez un fichier .env contenant : GOOGLE_PLACES_API_KEY=votre_clé"
            )

    # ── Helpers internes ──────────────────────────────────────────────────────

    def _headers(self, field_mask: list[str]) -> dict:
        """Construit les en-têtes HTTP requis par l'API (New)."""
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": ",".join(field_mask),
        }

    def _is_excluded(self, name: str, excluded: list[str]) -> bool:
        """Retourne True si le nom contient un mot-clé à exclure."""
        name_lower = name.lower()
        return any(kw in name_lower for kw in excluded)

    # ── Méthodes publiques ────────────────────────────────────────────────────

    def search_places(
        self,
        business_type: str,
        location: str,
        excluded_keywords: Optional[list[str]] = None,
        max_results: int = 60,
        min_reviews: int = 0,
        language: str = "fr",
    ) -> list[dict]:
        """
        Recherche des entreprises locales via Text Search (New).

        Gère automatiquement la pagination (jusqu'à 3 pages × 20 = 60 résultats max
        par requête — limite de l'API gratuite).

        Args:
            business_type     : Type d'activité (ex: "restaurant", "coiffeur")
            location          : Ville ou code postal (ex: "Paris", "69001")
            excluded_keywords : Noms à exclure (ex: ["McDonald's", "KFC"])
            max_results       : Nombre maximum de résultats à retourner
            min_reviews       : Filtre proxy sur la notoriété (nb d'avis minimum)
            language          : Code langue des résultats (ex: "fr", "en")

        Returns:
            Liste de dicts bruts tels que retournés par l'API.

        Raises:
            RuntimeError : Si l'API retourne une erreur HTTP.
        """
        url = f"{PLACES_API_BASE}/places:searchText"
        query = f"{business_type} {location}"

        # Normalisation des mots-clés exclus (insensible à la casse)
        excluded = [kw.lower().strip() for kw in (excluded_keywords or []) if kw.strip()]

        results: list[dict] = []
        seen_ids: set[str] = set()
        page_token: Optional[str] = None

        while len(results) < max_results:
            payload: dict = {
                "textQuery": query,
                "maxResultCount": 20,    # Limite fixe de l'API par page
                "languageCode": language,
            }
            if page_token:
                payload["pageToken"] = page_token

            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=self._headers(TEXT_SEARCH_FIELDS),
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
            except requests.HTTPError as exc:
                # Remonter le message d'erreur de l'API pour faciliter le debug
                raise RuntimeError(
                    f"Erreur Google Places API ({exc.response.status_code}) : "
                    f"{exc.response.text}"
                ) from exc
            except requests.RequestException as exc:
                raise RuntimeError(f"Erreur réseau : {exc}") from exc

            places = data.get("places", [])
            page_token = data.get("nextPageToken")

            for place in places:
                place_id = place.get("id")
                if not place_id or place_id in seen_ids:
                    continue  # Dédoublonnage

                name = place.get("displayName", {}).get("text", "")

                # ── Filtres ──────────────────────────────────────────────────
                if self._is_excluded(name, excluded):
                    continue

                # Le nombre d'avis sert de proxy pour la « taille » de l'entreprise
                # (l'API Google Places ne fournit pas le nombre d'employés)
                if min_reviews > 0 and place.get("userRatingCount", 0) < min_reviews:
                    continue

                seen_ids.add(place_id)
                results.append(place)

            # Arrêt si plus de pages disponibles ou page vide
            if not page_token or not places:
                break

            # Pause courtoise entre les pages pour éviter le rate limiting
            time.sleep(0.3)

        return results[:max_results]

    def get_place_details(self, place_id: str, language: str = "fr") -> dict:
        """
        Récupère les détails complets d'un lieu (Place Details New).

        Utile pour enrichir les données du Text Search (ex: téléphone manquant).

        Args:
            place_id : Identifiant Google du lieu
            language : Code langue

        Returns:
            Dict des détails ou dict vide en cas d'erreur.
        """
        url = f"{PLACES_API_BASE}/places/{place_id}"

        try:
            response = requests.get(
                url,
                headers=self._headers(DETAIL_FIELDS),
                params={"languageCode": language},
                timeout=15,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            # On retourne un dict vide plutôt que de planter l'export entier
            return {}

    def enrich_with_details(
        self,
        places: list[dict],
        language: str = "fr",
        progress_callback=None,
    ) -> list[dict]:
        """
        Complète chaque lieu avec un appel Place Details individuel.

        À utiliser si le Text Search ne remonte pas le téléphone ou le site web.
        Attention : chaque appel est facturé séparément par Google.

        Args:
            places            : Liste de lieux issus de search_places()
            language          : Code langue
            progress_callback : Fonction appelée avec (index, total) à chaque étape

        Returns:
            Liste enrichie (modification en place + retour).
        """
        total = len(places)
        for i, place in enumerate(places):
            place_id = place.get("id")
            if place_id:
                details = self.get_place_details(place_id, language)
                # On fusionne les détails dans le dict existant
                place.update(details)
            if progress_callback:
                progress_callback(i + 1, total)
            time.sleep(0.2)  # Pause entre les appels details
        return places

    # ── Formatage et export ───────────────────────────────────────────────────

    def format_for_csv(self, places: list[dict]) -> list[dict]:
        """
        Convertit les dicts bruts de l'API en lignes prêtes pour le CSV.

        Returns:
            Liste de dicts avec les clés définies dans CSV_COLUMNS.
        """
        rows = []
        for place in places:
            # Préférer le numéro national, sinon international
            phone = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber", "")

            rows.append({
                "nom":          place.get("displayName", {}).get("text", ""),
                "adresse":      place.get("formattedAddress", ""),
                "telephone":    phone,
                "site_web":     place.get("websiteUri", ""),
                "note_google":  place.get("rating", ""),
                "nombre_avis":  place.get("userRatingCount", ""),
                "statut":       place.get("businessStatus", ""),
                "types":        " | ".join(place.get("types", [])),
            })
        return rows

    def to_csv_bytes(self, places: list[dict]) -> bytes:
        """
        Exporte les lieux en CSV encodé UTF-8 avec BOM.

        Le BOM (utf-8-sig) garantit l'ouverture correcte dans Excel sur Windows.

        Returns:
            Bytes du fichier CSV, ou b"" si la liste est vide.
        """
        rows = self.format_for_csv(places)
        if not rows:
            return b""

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=CSV_COLUMNS,
            extrasaction="ignore",   # Ignore les clés supplémentaires éventuelles
            lineterminator="\r\n",   # CRLF standard Windows/Excel
        )
        writer.writeheader()
        writer.writerows(rows)

        return output.getvalue().encode("utf-8-sig")
