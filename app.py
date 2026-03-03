"""
app.py — Interface web Streamlit pour le scraper Google Places

Lancement :
    streamlit run app.py

Onglet :
  - Collecte Massive : boucle automatique sur toute la France
    → scraping email depuis le site web + fallback SMTP guessing (100% gratuit)
"""

from __future__ import annotations

import os
import time

import pandas as pd
import streamlit as st

if "GOOGLE_PLACES_API_KEY" in st.secrets:
    os.environ["GOOGLE_PLACES_API_KEY"] = st.secrets["GOOGLE_PLACES_API_KEY"]
if "DROPCONTACT_API_KEY" in st.secrets:
    os.environ["DROPCONTACT_API_KEY"] = st.secrets["DROPCONTACT_API_KEY"]

from scraper import (
    MassiveCollector,
    SECTORS,
    FRENCH_DEPARTMENTS,
    FRENCH_CITIES,
)

# ── Configuration de la page ───────────────────────────────────────────────────

st.set_page_config(
    page_title="Skypeo Scrapper",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── En-tête ────────────────────────────────────────────────────────────────────

st.title("🔍 Skypeo Scrapper")
st.caption("Collecte massive · Google Places API (New) + SMTP email guessing — 100% gratuit")
st.divider()

# ── Onglets ────────────────────────────────────────────────────────────────────

tab_massive, = st.tabs(["🚀 Collecte Massive"])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — COLLECTE MASSIVE
# ══════════════════════════════════════════════════════════════════════════════

with tab_massive:
    st.subheader("🚀 Collecte Massive — France entière")
    st.caption(
        "Boucle automatique sur les villes × types d'activité sélectionnés. "
        "Les résultats sont sauvegardés en temps réel dans un fichier CSV."
    )
    st.divider()

    collector = st.session_state.get("massive_collector")
    is_running = collector is not None and collector.is_running
    is_done = collector is not None and collector.get_state().get("is_done", False)

    if not is_running:
        st.subheader("⚙️ Configuration")

        cfg_col1, cfg_col2 = st.columns(2)

        with cfg_col1:
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
            output_filename = st.text_input(
                "Fichier de sortie",
                value="collecte_france.csv",
                help="Nom du fichier CSV où les résultats seront sauvegardés.",
            )

            resume_mode = st.checkbox(
                "♻️ Reprendre une collecte existante",
                value=False,
                help=(
                    "Si le fichier existe déjà, charge les place_ids déjà collectés "
                    "et reprend la collecte sans re-scraper les entreprises connues."
                ),
            )

            file_exists = os.path.exists(output_filename) if output_filename else False
            if file_exists:
                file_size = os.path.getsize(output_filename)
                st.info(f"📄 Fichier existant : `{output_filename}` ({file_size // 1024} Ko)")
            else:
                st.caption("📄 Le fichier sera créé au démarrage.")

        st.divider()

        if selected_sectors:
            total_types = sum(len(SECTORS[s]["types"]) for s in selected_sectors)
            n_cities = len([
                c for c in FRENCH_CITIES
                if dept_filter is None or c[1] in dept_filter
            ])
            n_combos = n_cities * total_types
            st.info(
                f"**Estimation :** {n_cities} villes × {total_types} types = "
                f"**{n_combos:,} requêtes Google Places** "
                f"(~{n_combos * 0.5 / 60:.0f} min sans scraping email)"
            )

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
                    os.remove(output_filename.strip())

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
            if os.path.exists(_output_file):
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

        col_stop, col_spacer = st.columns([1, 3])
        with col_stop:
            if st.button("⏹ Arrêter la collecte", type="secondary", use_container_width=True):
                collector.stop()
                st.toast("⏹ Arrêt demandé — la collecte s'arrêtera après la tâche en cours.")

        st.divider()

        progress_val = float(state.get("progress", 0.0))
        done_combos = state.get("done_combinations", 0)
        total_combos = state.get("total_combinations", 1)
        current_task = state.get("current_task", "…")

        st.progress(
            progress_val,
            text=f"🔄 **{current_task}** — {done_combos}/{total_combos} combinaisons",
        )

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🏪 Entreprises trouvées", f"{state.get('total_found', 0):,}")
        m2.metric("🌐 Avec site web", f"{state.get('with_website', 0):,}")
        m3.metric("📧 Avec email pro", f"{state.get('with_email', 0):,}")
        m4.metric("📡 Appels API Google", f"{state.get('api_calls', 0):,}")

        if state.get("error"):
            st.warning(f"⚠️ Dernière erreur : {state['error']}")

        st.divider()
        st.caption("**50 derniers résultats collectés :**")

        last_results = state.get("last_results", [])
        if last_results:
            df_live = pd.DataFrame(last_results[::-1])
            col_cfg = {
                "nom":      st.column_config.TextColumn("Nom", width="medium"),
                "ville":    st.column_config.TextColumn("Ville", width="small"),
                "secteur":  st.column_config.TextColumn("Secteur", width="small"),
                "email":    st.column_config.TextColumn("Email", width="medium"),
                "telephone": st.column_config.TextColumn("Téléphone", width="small"),
                "site_web": st.column_config.LinkColumn("Site Web", width="medium", display_text="🔗"),
                "note":     st.column_config.NumberColumn("Note", format="%.1f", width="small"),
            }
            st.dataframe(df_live, use_container_width=True, column_config=col_cfg, hide_index=True)
        else:
            st.info("Les premiers résultats apparaîtront dans quelques secondes…")

        _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
        if os.path.exists(_output_file):
            st.divider()
            with open(_output_file, "rb") as f:
                st.download_button(
                    label="📥 Télécharger CSV (snapshot actuel)",
                    data=f.read(),
                    file_name=_output_file,
                    mime="text/csv",
                    help="Télécharge les données collectées jusqu'à maintenant.",
                )

        time.sleep(1)
        st.rerun()

