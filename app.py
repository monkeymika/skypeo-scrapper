"""
app.py — Interface web Streamlit pour le scraper Google Places

Lancement :
    streamlit run app.py

Paramètres disponibles dans la barre latérale :
  - Type d'entreprise
  - Zone géographique (ville / code postal)
  - Mots-clés à exclure
  - Nombre minimum d'avis (proxy taille entreprise)
  - Nombre maximum de résultats
  - Option d'enrichissement via Place Details
"""

import os

import pandas as pd
import streamlit as st

# Streamlit Community Cloud expose la clé via st.secrets.
# En local, elle est chargée depuis .env par scraper.py (python-dotenv).
# On injecte ici st.secrets dans os.environ pour que scraper.py la trouve
# dans les deux environnements sans modification.
if "GOOGLE_PLACES_API_KEY" in st.secrets:
    os.environ["GOOGLE_PLACES_API_KEY"] = st.secrets["GOOGLE_PLACES_API_KEY"]

from scraper import GooglePlacesScraper

# ── Configuration de la page ───────────────────────────────────────────────────

st.set_page_config(
    page_title="Scrapper Entreprises Locales",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── En-tête ────────────────────────────────────────────────────────────────────

st.title("🔍 Recherche d'Entreprises Locales")
st.caption("Propulsé par **Google Places API (New)** · Text Search + Place Details")
st.divider()

# ── Barre latérale : paramètres de recherche ──────────────────────────────────

with st.sidebar:
    st.header("⚙️ Paramètres")

    # --- Type d'entreprise ---
    st.subheader("🏪 Activité")
    business_type = st.text_input(
        "Type d'entreprise *",
        placeholder="restaurant, coiffeur, boulangerie…",
        help=(
            "Entrez le type d'activité. "
            "Exemples : restaurant, salon de coiffure, plombier, agence immobilière"
        ),
    )

    # --- Zone géographique ---
    st.subheader("📍 Localisation")
    location = st.text_input(
        "Ville ou code postal *",
        placeholder="Paris, Lyon, 69001…",
        help="Nom de ville, arrondissement ou code postal français",
    )

    # --- Exclusions ---
    st.subheader("🚫 Exclusions")
    excluded_raw = st.text_area(
        "Mots-clés à exclure",
        placeholder="McDonald's\nKFC\nBuffalo Grill\nPizza Hut",
        help="Un mot-clé par ligne. Les résultats dont le nom contient ce terme seront ignorés.",
        height=110,
    )

    st.divider()

    # --- Filtres avancés ---
    st.subheader("🎛️ Filtres avancés")

    min_reviews = st.slider(
        "Avis minimum",
        min_value=0,
        max_value=500,
        value=0,
        step=10,
        help=(
            "Filtre sur le nombre d'avis Google.\n\n"
            "⚠️ Google Places n'expose pas le nombre d'employés. "
            "Le nombre d'avis sert de proxy pour estimer la notoriété / taille."
        ),
    )

    max_results = st.slider(
        "Résultats maximum",
        min_value=5,
        max_value=60,
        value=20,
        step=5,
        help="L'API renvoie 20 résultats par page (3 pages max = 60 résultats).",
    )

    language = st.selectbox(
        "Langue des résultats",
        options=["fr", "en", "de", "es", "it"],
        index=0,
        help="Code de langue pour les noms et adresses retournés par l'API.",
    )

    st.divider()

    # --- Options avancées ---
    with st.expander("🔬 Options avancées"):
        enrich_details = st.toggle(
            "Enrichir via Place Details",
            value=False,
            help=(
                "Effectue un appel Place Details par résultat pour compléter "
                "téléphone / site web manquants. "
                "⚠️ Ralentit la recherche et augmente la consommation de quota API."
            ),
        )

    st.divider()
    st.caption(
        "**Légende statuts :**\n"
        "- OPERATIONAL : ouvert\n"
        "- CLOSED_TEMPORARILY : temporairement fermé\n"
        "- CLOSED_PERMANENTLY : définitivement fermé"
    )

# ── Bouton de lancement ────────────────────────────────────────────────────────

col_btn, col_clear = st.columns([3, 1])
with col_btn:
    search_clicked = st.button(
        "🔍 Lancer la recherche",
        type="primary",
        use_container_width=True,
        disabled=not (business_type and location),
    )
with col_clear:
    clear_clicked = st.button(
        "🗑️ Effacer",
        use_container_width=True,
        help="Efface les résultats affichés",
    )

if clear_clicked:
    st.session_state.pop("results", None)
    st.session_state.pop("csv_bytes", None)

# ── Logique de recherche ───────────────────────────────────────────────────────

if search_clicked:
    if not business_type.strip():
        st.error("⛔ Veuillez saisir un type d'entreprise.")
        st.stop()
    if not location.strip():
        st.error("⛔ Veuillez saisir une zone géographique.")
        st.stop()

    # Parsing des mots-clés exclus
    excluded_keywords = [
        kw.strip() for kw in excluded_raw.splitlines() if kw.strip()
    ]

    # Initialisation du scraper (vérifie la clé API au démarrage)
    try:
        scraper = GooglePlacesScraper()
    except ValueError as exc:
        st.error(f"⛔ {exc}")
        st.code("GOOGLE_PLACES_API_KEY=votre_clé_ici", language="bash")
        st.info(
            "Créez un fichier `.env` à la racine du projet et ajoutez votre clé API Google Places."
        )
        st.stop()

    # ── Recherche principale ───────────────────────────────────────────────────
    status_placeholder = st.empty()
    progress_bar = st.progress(0, text="Initialisation…")

    try:
        with st.spinner(
            f"Recherche de **{business_type}** à **{location}**…"
        ):
            places = scraper.search_places(
                business_type=business_type.strip(),
                location=location.strip(),
                excluded_keywords=excluded_keywords,
                max_results=max_results,
                min_reviews=min_reviews,
                language=language,
            )
    except RuntimeError as exc:
        progress_bar.empty()
        st.error(f"⛔ Erreur lors de la recherche :\n\n{exc}")
        st.stop()

    if not places:
        progress_bar.empty()
        st.warning(
            "🔍 Aucun résultat trouvé pour ces critères. "
            "Essayez un type d'activité différent, une autre zone, "
            "ou réduisez le filtre d'avis minimum."
        )
        st.stop()

    # ── Enrichissement optionnel via Place Details ─────────────────────────────
    if enrich_details:
        progress_bar.progress(0, text="Enrichissement des données…")

        def update_progress(current: int, total: int) -> None:
            pct = int(current / total * 100)
            progress_bar.progress(pct, text=f"Place Details {current}/{total}…")

        with st.spinner("Récupération des détails…"):
            places = scraper.enrich_with_details(
                places, language=language, progress_callback=update_progress
            )

    progress_bar.empty()

    # Mise en cache des résultats dans la session pour persistance
    st.session_state["results"] = places
    st.session_state["csv_bytes"] = scraper.to_csv_bytes(places)
    st.session_state["search_label"] = f"{business_type} — {location}"

# ── Affichage des résultats ────────────────────────────────────────────────────

if "results" in st.session_state:
    places: list[dict] = st.session_state["results"]
    scraper_display = GooglePlacesScraper() if st.session_state.get("csv_bytes") else None

    # On réinstancie un scraper juste pour le formatage (pas d'appels API)
    try:
        scraper_fmt = GooglePlacesScraper()
    except ValueError:
        # Si la clé est manquante au moment de l'affichage, on formate manuellement
        scraper_fmt = None

    rows = scraper_fmt.format_for_csv(places) if scraper_fmt else []
    df = pd.DataFrame(rows) if rows else pd.DataFrame()

    st.subheader(f"📊 Résultats : {st.session_state.get('search_label', '')}")

    # ── Métriques de synthèse ──────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("🏪 Entreprises trouvées", len(df))

    if not df.empty and "note_google" in df.columns:
        ratings = pd.to_numeric(df["note_google"], errors="coerce").dropna()
        if not ratings.empty:
            col2.metric("⭐ Note moyenne", f"{ratings.mean():.1f}")
        else:
            col2.metric("⭐ Note moyenne", "—")

    if not df.empty and "telephone" in df.columns:
        with_phone = df["telephone"].replace("", pd.NA).dropna()
        col3.metric("📞 Avec téléphone", len(with_phone))

    if not df.empty and "site_web" in df.columns:
        with_website = df["site_web"].replace("", pd.NA).dropna()
        col4.metric("🌐 Avec site web", len(with_website))

    st.divider()

    # ── Tableau des résultats ──────────────────────────────────────────────────
    if not df.empty:
        # Configuration d'affichage des colonnes
        column_config = {
            "nom": st.column_config.TextColumn("Nom", width="medium"),
            "adresse": st.column_config.TextColumn("Adresse", width="large"),
            "telephone": st.column_config.TextColumn("Téléphone", width="small"),
            "site_web": st.column_config.LinkColumn(
                "Site Web",
                width="medium",
                display_text="🔗 Visiter",
            ),
            "note_google": st.column_config.NumberColumn(
                "Note ⭐",
                format="%.1f",
                width="small",
                min_value=0,
                max_value=5,
            ),
            "nombre_avis": st.column_config.NumberColumn(
                "Avis",
                width="small",
                format="%d",
            ),
            "statut": st.column_config.TextColumn("Statut", width="small"),
            "types": st.column_config.TextColumn("Types", width="medium"),
        }

        st.dataframe(
            df,
            use_container_width=True,
            column_config=column_config,
            hide_index=True,
        )

        st.divider()

        # ── Téléchargement CSV ─────────────────────────────────────────────────
        filename = (
            f"{st.session_state.get('search_label', 'resultats')}.csv"
            .replace(" — ", "_")
            .replace(" ", "_")
            .lower()
        )

        st.download_button(
            label="📥 Télécharger les résultats en CSV",
            data=st.session_state["csv_bytes"],
            file_name=filename,
            mime="text/csv",
            use_container_width=True,
            help="Le fichier est encodé UTF-8 avec BOM pour une compatibilité Excel optimale.",
        )

        # ── Note sur les limitations ───────────────────────────────────────────
        with st.expander("ℹ️ À propos des données"):
            st.markdown(
                """
                **Données disponibles via Google Places API (New) :**
                - ✅ Nom, adresse, téléphone, site web
                - ✅ Note Google et nombre d'avis
                - ✅ Statut d'ouverture (OPERATIONAL / CLOSED…)
                - ✅ Catégories de l'établissement

                **Données non disponibles :**
                - ❌ Nombre d'employés — non exposé par l'API Google Places
                  → Le filtre **"Avis minimum"** sert de proxy pour filtrer
                    les établissements selon leur notoriété.
                - ❌ Chiffre d'affaires, date de création, SIRET

                **Quota API :**
                L'API Google Places (New) est facturée à l'utilisation.
                Text Search = ~0,017 $/requête · Place Details = ~0,017 $/requête.
                Consultez [la grille tarifaire Google](https://developers.google.com/maps/billing-and-pricing/pricing).
                """
            )
    else:
        st.info("Les résultats seront affichés ici après la recherche.")

# ── Aide initiale (première visite) ───────────────────────────────────────────

elif not search_clicked:
    st.info(
        "👈 Renseignez les paramètres dans la barre latérale puis cliquez sur "
        "**Lancer la recherche**."
    )

    with st.expander("🚀 Guide de démarrage rapide"):
        st.markdown(
            """
            ### Prérequis

            1. **Clé API Google Places**
               - Créez un projet sur [Google Cloud Console](https://console.cloud.google.com/)
               - Activez l'API **Places API (New)**
               - Générez une clé API dans *Identifiants*

            2. **Fichier `.env`**
               Créez un fichier `.env` à la racine du projet :
               ```
               GOOGLE_PLACES_API_KEY=AIza...votre_clé...
               ```

            ### Exemples de recherches

            | Type d'entreprise | Zone | Mots-clés exclus |
            |---|---|---|
            | restaurant | Paris 75001 | McDonald's, KFC |
            | salon de coiffure | Lyon | Dessange, Jean Louis David |
            | plombier | Marseille | — |
            | agence immobilière | Bordeaux | Foncia |

            ### Paramètres recommandés
            - **Avis minimum = 10** pour exclure les établissements peu actifs
            - **Max résultats = 20** pour les recherches rapides
            - Activez **"Enrichir via Place Details"** si le téléphone est manquant
            """
        )
