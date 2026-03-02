"""
app.py — Interface web Streamlit pour le scraper Google Places

Lancement :
    streamlit run app.py

Deux modes disponibles :
  - Recherche Rapide : recherche ponctuelle sur une ville
  - Collecte Massive : boucle automatique sur toute la France (ou un département)
"""

import os
import time

import pandas as pd
import streamlit as st

# Streamlit Community Cloud expose la clé via st.secrets.
# En local, elle est chargée depuis .env par scraper.py (python-dotenv).
if "GOOGLE_PLACES_API_KEY" in st.secrets:
    os.environ["GOOGLE_PLACES_API_KEY"] = st.secrets["GOOGLE_PLACES_API_KEY"]

from scraper import (
    GooglePlacesScraper,
    MassiveCollector,
    SECTORS,
    FRENCH_DEPARTMENTS,
    enrich_with_emails,
)

# ── Configuration de la page ───────────────────────────────────────────────────

st.set_page_config(
    page_title="Scrapper Entreprises Locales",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Thème clair / sombre ───────────────────────────────────────────────────────

DARK_CSS = """
<style>
/* Fond principal */
.stApp, [data-testid="stAppViewContainer"] {
    background-color: #0e1117 !important;
    color: #fafafa !important;
}
/* Sidebar */
[data-testid="stSidebar"], [data-testid="stSidebarContent"] {
    background-color: #1a1c24 !important;
}
/* Header / toolbar */
[data-testid="stHeader"], [data-testid="stToolbar"] {
    background-color: #0e1117 !important;
}
/* Inputs */
input, textarea, select,
[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea {
    background-color: #262730 !important;
    color: #fafafa !important;
    border-color: #3d3f4f !important;
}
/* Labels et textes */
label, p, h1, h2, h3, h4, span,
.stMarkdown, .stCaption {
    color: #fafafa !important;
}
/* Métriques */
[data-testid="metric-container"] {
    background-color: #1a1c24 !important;
    border: 1px solid #2d2f3d !important;
    border-radius: 10px;
    padding: 12px;
}
/* Dividers */
hr { border-color: #2d2f3d !important; }
/* Expander */
[data-testid="stExpander"] {
    background-color: #1a1c24 !important;
    border-color: #2d2f3d !important;
}
/* Dataframe */
[data-testid="stDataFrame"] {
    background-color: #1a1c24 !important;
}
/* Sliders */
[data-testid="stSlider"] [role="slider"] {
    background-color: #ff4b4b !important;
}
/* Info / Warning / Error boxes */
[data-testid="stAlert"] {
    background-color: #1a1c24 !important;
}
/* Selectbox */
[data-testid="stSelectbox"] > div > div {
    background-color: #262730 !important;
    color: #fafafa !important;
}
</style>
"""

LIGHT_CSS = """
<style>
.stApp, [data-testid="stAppViewContainer"] {
    background-color: #ffffff !important;
    color: #31333f !important;
}
[data-testid="stSidebar"], [data-testid="stSidebarContent"] {
    background-color: #f0f2f6 !important;
}
[data-testid="stHeader"] { background-color: #ffffff !important; }
input, textarea { background-color: #ffffff !important; color: #31333f !important; }
label, p, h1, h2, h3, h4, span { color: #31333f !important; }
[data-testid="metric-container"] {
    background-color: #f0f2f6 !important;
    border: 1px solid #e0e0e0 !important;
    border-radius: 10px;
    padding: 12px;
}
hr { border-color: #e0e0e0 !important; }
</style>
"""

# Initialisation de la préférence de thème (dark par défaut)
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

# Injection CSS selon le thème courant
st.markdown(DARK_CSS if st.session_state.dark_mode else LIGHT_CSS, unsafe_allow_html=True)

# ── En-tête ────────────────────────────────────────────────────────────────────

st.title("🔍 Recherche d'Entreprises Locales")
st.caption("Propulsé par **Google Places API (New)** · Text Search + Place Details")
st.divider()

# ── Sidebar : thème ────────────────────────────────────────────────────────────

with st.sidebar:
    dark = st.toggle(
        "🌙 Mode sombre",
        value=st.session_state.dark_mode,
        key="theme_toggle",
    )
    if dark != st.session_state.dark_mode:
        st.session_state.dark_mode = dark
        st.rerun()

# ── Onglets principaux ─────────────────────────────────────────────────────────

tab_quick, tab_massive = st.tabs(["🔍 Recherche Rapide", "🚀 Collecte Massive"])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — RECHERCHE RAPIDE (code existant, inchangé)
# ══════════════════════════════════════════════════════════════════════════════

with tab_quick:
    # ── Barre latérale : paramètres de recherche ───────────────────────────────
    with st.sidebar:
        st.header("⚙️ Paramètres — Recherche Rapide")

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
            scrape_emails = st.toggle(
                "🔎 Chercher les emails sur les sites web",
                value=False,
                help=(
                    "Visite le site web de chaque entreprise pour y trouver une adresse email publique.\n\n"
                    "• Analyse la page principale + pages /contact\n"
                    "• Priorise les balises mailto:\n"
                    "• ⚠️ Ajoute ~1-2s de traitement par résultat\n"
                    "• Certains sites bloquent les bots (résultat vide dans ce cas)"
                ),
            )

        st.divider()
        st.caption(
            "**Légende statuts :**\n"
            "- OPERATIONAL : ouvert\n"
            "- CLOSED_TEMPORARILY : temporairement fermé\n"
            "- CLOSED_PERMANENTLY : définitivement fermé"
        )

    # ── Bouton de lancement ────────────────────────────────────────────────────

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

    # ── Logique de recherche ───────────────────────────────────────────────────

    if search_clicked:
        if not business_type.strip():
            st.error("⛔ Veuillez saisir un type d'entreprise.")
            st.stop()
        if not location.strip():
            st.error("⛔ Veuillez saisir une zone géographique.")
            st.stop()

        excluded_keywords = [
            kw.strip() for kw in excluded_raw.splitlines() if kw.strip()
        ]

        try:
            scraper = GooglePlacesScraper()
        except ValueError as exc:
            st.error(f"⛔ {exc}")
            st.code("GOOGLE_PLACES_API_KEY=votre_clé_ici", language="bash")
            st.info(
                "Créez un fichier `.env` à la racine du projet et ajoutez votre clé API Google Places."
            )
            st.stop()

        status_placeholder = st.empty()
        progress_bar = st.progress(0, text="Initialisation…")

        try:
            with st.spinner(f"Recherche de **{business_type}** à **{location}**…"):
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

        if enrich_details:
            progress_bar.progress(0, text="Enrichissement des données…")

            def update_progress(current: int, total: int) -> None:
                pct = int(current / total * 100)
                progress_bar.progress(pct, text=f"Place Details {current}/{total}…")

            with st.spinner("Récupération des détails…"):
                places = scraper.enrich_with_details(
                    places, language=language, progress_callback=update_progress
                )

        if scrape_emails:
            sites_count = sum(1 for p in places if p.get("websiteUri"))
            progress_bar.progress(0, text=f"Recherche d'emails sur {sites_count} sites…")

            def update_email_progress(current: int, total: int) -> None:
                pct = int(current / total * 100)
                progress_bar.progress(pct, text=f"Analyse des sites {current}/{total}…")

            with st.spinner("Extraction des emails en cours…"):
                places = enrich_with_emails(places, progress_callback=update_email_progress)

        progress_bar.empty()

        st.session_state["results"] = places
        st.session_state["csv_bytes"] = scraper.to_csv_bytes(places)
        st.session_state["search_label"] = f"{business_type} — {location}"

    # ── Affichage des résultats ────────────────────────────────────────────────

    if "results" in st.session_state:
        places: list[dict] = st.session_state["results"]

        try:
            scraper_fmt = GooglePlacesScraper()
        except ValueError:
            scraper_fmt = None

        rows = scraper_fmt.format_for_csv(places) if scraper_fmt else []
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        st.subheader(f"📊 Résultats : {st.session_state.get('search_label', '')}")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("🏪 Entreprises trouvées", len(df))

        if not df.empty and "note_google" in df.columns:
            ratings = pd.to_numeric(df["note_google"], errors="coerce").dropna()
            col2.metric("⭐ Note moyenne", f"{ratings.mean():.1f}" if not ratings.empty else "—")
        else:
            col2.metric("⭐ Note moyenne", "—")

        if not df.empty and "telephone" in df.columns:
            with_phone = df["telephone"].replace("", pd.NA).dropna()
            col3.metric("📞 Avec téléphone", len(with_phone))

        if not df.empty and "site_web" in df.columns:
            with_website = df["site_web"].replace("", pd.NA).dropna()
            col4.metric("🌐 Avec site web", len(with_website))

        if not df.empty and "email" in df.columns:
            with_email = df["email"].replace("", pd.NA).dropna()
            if len(with_email) > 0:
                c1, c2 = st.columns([1, 3])
                c1.metric("📧 Avec email", len(with_email))

        st.divider()

        if not df.empty:
            column_config = {
                "nom": st.column_config.TextColumn("Nom", width="medium"),
                "adresse": st.column_config.TextColumn("Adresse", width="large"),
                "telephone": st.column_config.TextColumn("Téléphone", width="small"),
                "email": st.column_config.TextColumn("Email", width="medium"),
                "site_web": st.column_config.LinkColumn("Site Web", width="medium", display_text="🔗 Visiter"),
                "note_google": st.column_config.NumberColumn("Note ⭐", format="%.1f", width="small", min_value=0, max_value=5),
                "nombre_avis": st.column_config.NumberColumn("Avis", width="small", format="%d"),
                "statut": st.column_config.TextColumn("Statut", width="small"),
                "types": st.column_config.TextColumn("Types", width="medium"),
            }

            st.dataframe(df, use_container_width=True, column_config=column_config, hide_index=True)

            st.divider()

            filename = (
                f"{st.session_state.get('search_label', 'resultats')}.csv"
                .replace(" — ", "_").replace(" ", "_").lower()
            )

            st.download_button(
                label="📥 Télécharger les résultats en CSV",
                data=st.session_state["csv_bytes"],
                file_name=filename,
                mime="text/csv",
                use_container_width=True,
                help="Le fichier est encodé UTF-8 avec BOM pour une compatibilité Excel optimale.",
            )

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

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 2 — COLLECTE MASSIVE
# ══════════════════════════════════════════════════════════════════════════════

with tab_massive:
    st.subheader("🚀 Collecte Massive — France entière")
    st.caption(
        "Boucle automatique sur les villes × types d'activité sélectionnés. "
        "Les résultats sont sauvegardés en temps réel dans un fichier CSV."
    )
    st.divider()

    # ── Récupération du collecteur en cours (si existant) ──────────────────────
    collector: MassiveCollector | None = st.session_state.get("massive_collector")

    # ── Configuration ──────────────────────────────────────────────────────────
    is_running = collector is not None and collector.is_running
    is_done = collector is not None and collector.get_state().get("is_done", False)

    if not is_running:
        st.subheader("⚙️ Configuration")

        cfg_col1, cfg_col2 = st.columns(2)

        with cfg_col1:
            # Sélecteur de secteurs
            sector_options = {
                f"{v['icon']} {v['label']}": k
                for k, v in SECTORS.items()
            }
            selected_sector_labels = st.multiselect(
                "Secteurs à collecter *",
                options=list(sector_options.keys()),
                default=list(sector_options.keys()),
                help="Sélectionnez un ou plusieurs secteurs d'activité.",
            )
            selected_sectors = [sector_options[lbl] for lbl in selected_sector_labels]

            # Sélecteur département
            dept_options = {"🌍 France entière": None}
            dept_options.update({
                f"{code} — {name}": code
                for code, name in sorted(FRENCH_DEPARTMENTS.items(), key=lambda x: x[0])
            })
            selected_dept_label = st.selectbox(
                "Zone géographique",
                options=list(dept_options.keys()),
                index=0,
                help="Limitez la collecte à un département ou couvrez toute la France.",
            )
            dept_filter_value = dept_options[selected_dept_label]
            dept_filter = [dept_filter_value] if dept_filter_value else None

        with cfg_col2:
            # Nom du fichier de sortie
            output_filename = st.text_input(
                "Fichier de sortie",
                value="collecte_france.csv",
                help="Nom du fichier CSV où les résultats seront sauvegardés.",
            )

            # Option de reprise
            resume_mode = st.checkbox(
                "♻️ Reprendre une collecte existante",
                value=False,
                help=(
                    "Si le fichier existe déjà, charge les place_ids déjà collectés "
                    "et reprend la collecte sans re-scraper les entreprises connues."
                ),
            )

            import os as _os
            file_exists = _os.path.exists(output_filename) if output_filename else False
            if file_exists:
                file_size = _os.path.getsize(output_filename)
                st.info(f"📄 Fichier existant : `{output_filename}` ({file_size // 1024} Ko)")
            else:
                st.caption("📄 Le fichier sera créé au démarrage.")

        st.divider()

        # ── Estimation ────────────────────────────────────────────────────────
        if selected_sectors:
            total_types = sum(len(SECTORS[s]["types"]) for s in selected_sectors)
            from scraper import FRENCH_CITIES as _CITIES
            n_cities = len([
                c for c in _CITIES
                if dept_filter is None or c[1] in dept_filter
            ])
            n_combos = n_cities * total_types
            st.info(
                f"**Estimation :** {n_cities} villes × {total_types} types = "
                f"**{n_combos:,} requêtes Google Places** "
                f"(~{n_combos * 0.5 / 60:.0f} min sans scraping email)"
            )

        # ── Bouton démarrer ────────────────────────────────────────────────────
        st.divider()
        start_disabled = not selected_sectors or not output_filename.strip()

        if st.button(
            "▶ Démarrer la collecte",
            type="primary",
            use_container_width=True,
            disabled=start_disabled,
        ):
            try:
                new_collector = MassiveCollector(
                    output_path=output_filename.strip(),
                    sectors=selected_sectors,
                    dept_filter=dept_filter,
                )
                if resume_mode and file_exists:
                    loaded = new_collector.load_existing()
                    st.toast(f"♻️ Reprise : {loaded} entreprises déjà collectées ignorées.")
                elif file_exists and not resume_mode:
                    # Supprimer le fichier existant pour repartir à zéro
                    _os.remove(output_filename.strip())

                new_collector.start()
                st.session_state["massive_collector"] = new_collector
                st.session_state["massive_output_file"] = output_filename.strip()
                st.rerun()

            except ValueError as exc:
                st.error(f"⛔ {exc}")

        if is_done:
            st.divider()
            st.success("✅ Collecte terminée !")
            _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
            if _os.path.exists(_output_file):
                with open(_output_file, "rb") as f:
                    st.download_button(
                        label="📥 Télécharger le CSV complet",
                        data=f.read(),
                        file_name=_output_file,
                        mime="text/csv",
                        use_container_width=True,
                    )
            if st.button("🔄 Nouvelle collecte", use_container_width=True):
                st.session_state.pop("massive_collector", None)
                st.session_state.pop("massive_output_file", None)
                st.rerun()

    else:
        # ── COLLECTE EN COURS ──────────────────────────────────────────────────
        state = collector.get_state()

        # Bouton stop
        col_stop, col_spacer = st.columns([1, 3])
        with col_stop:
            if st.button("⏹ Arrêter la collecte", type="secondary", use_container_width=True):
                collector.stop()
                st.toast("⏹ Arrêt demandé — la collecte s'arrêtera après la tâche en cours.")

        st.divider()

        # ── Barre de progression ───────────────────────────────────────────────
        progress_val = float(state.get("progress", 0.0))
        done_combos = state.get("done_combinations", 0)
        total_combos = state.get("total_combinations", 1)
        current_task = state.get("current_task", "…")

        st.progress(
            progress_val,
            text=f"🔄 **{current_task}** — {done_combos}/{total_combos} combinaisons",
        )

        # ── Métriques ─────────────────────────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🏪 Entreprises trouvées", f"{state.get('total_found', 0):,}")
        m2.metric("🌐 Avec site web", f"{state.get('with_website', 0):,}")
        m3.metric("📧 Avec email pro", f"{state.get('with_email', 0):,}")
        m4.metric("📡 Appels API Google", f"{state.get('api_calls', 0):,}")

        # ── Erreur éventuelle ──────────────────────────────────────────────────
        if state.get("error"):
            st.warning(f"⚠️ Dernière erreur : {state['error']}")

        # ── Tableau live ───────────────────────────────────────────────────────
        st.divider()
        st.caption("**50 derniers résultats collectés :**")

        last_results = state.get("last_results", [])
        if last_results:
            df_live = pd.DataFrame(last_results[::-1])  # Plus récent en premier
            col_cfg = {
                "nom": st.column_config.TextColumn("Nom", width="medium"),
                "ville": st.column_config.TextColumn("Ville", width="small"),
                "secteur": st.column_config.TextColumn("Secteur", width="small"),
                "email": st.column_config.TextColumn("Email", width="medium"),
                "telephone": st.column_config.TextColumn("Téléphone", width="small"),
                "site_web": st.column_config.LinkColumn("Site Web", width="medium", display_text="🔗"),
                "note": st.column_config.NumberColumn("Note", format="%.1f", width="small"),
            }
            st.dataframe(df_live, use_container_width=True, column_config=col_cfg, hide_index=True)
        else:
            st.info("Les premiers résultats apparaîtront dans quelques secondes…")

        # ── Téléchargement en cours de collecte ────────────────────────────────
        _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
        import os as _os2
        if _os2.path.exists(_output_file):
            st.divider()
            with open(_output_file, "rb") as f:
                st.download_button(
                    label="📥 Télécharger CSV (snapshot actuel)",
                    data=f.read(),
                    file_name=_output_file,
                    mime="text/csv",
                    help="Télécharge les données collectées jusqu'à maintenant.",
                )

        # ── Polling : rerun automatique toutes les secondes ────────────────────
        time.sleep(1)
        st.rerun()
