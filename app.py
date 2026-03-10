"""
app.py — Interface web Streamlit pour le scraper Google Places

Lancement :
    streamlit run app.py
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

# ── CSS custom ─────────────────────────────────────────────────────────────────

st.markdown("""
<style>
.metric-card {
    background: #f8f9fa;
    border: 1px solid #e9ecef;
    border-radius: 8px;
    padding: 16px 20px;
    text-align: center;
}
.metric-label { font-size: 0.78rem; color: #6c757d; font-weight: 500; margin-bottom: 4px; }
.metric-value { font-size: 1.8rem; font-weight: 700; color: #212529; line-height: 1; }
.status-running { color: #198754; font-weight: 600; }
.status-stopped { color: #dc3545; font-weight: 600; }
.status-done    { color: #0d6efd; font-weight: 600; }
.task-pill {
    display: inline-block;
    background: #e7f1ff;
    color: #0d6efd;
    border-radius: 20px;
    padding: 3px 14px;
    font-size: 0.82rem;
    font-weight: 500;
    margin-top: 4px;
}
.section-header {
    font-size: 1.05rem;
    font-weight: 600;
    color: #343a40;
    margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)

# ── En-tête ────────────────────────────────────────────────────────────────────

st.title("🔍 Skypeo Scrapper")
st.caption("Collecte massive · Google Places API (New) + SMTP email guessing — 100% gratuit")
st.divider()

# ── Onglets ────────────────────────────────────────────────────────────────────

tab_massive, = st.tabs(["🚀 Collecte Massive"])

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_duration(seconds: float) -> str:
    """Formate une durée en 'Xh Ym Zs' lisible."""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _read_csv_safe(path: str) -> bytes | None:
    """Lit le CSV de façon sécurisée ; retourne None si le fichier est verrouillé."""
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return None


def _start_collector(output_path: str, sectors: list, dept_filter, resume: bool) -> None:
    """Crée, configure et démarre un MassiveCollector. Stocke le tout en session."""
    new_collector = MassiveCollector(
        output_path=output_path,
        sectors=sectors,
        dept_filter=dept_filter,
    )
    file_exists = os.path.exists(output_path)
    if resume and file_exists:
        loaded = new_collector.load_existing()
        st.toast(f"♻️ Reprise : {loaded:,} entreprises déjà collectées ignorées.")
    elif file_exists and not resume:
        os.remove(output_path)

    new_collector.start()
    st.session_state["massive_collector"]   = new_collector
    st.session_state["massive_output_file"] = output_path
    st.session_state["massive_sectors"]     = sectors
    st.session_state["massive_dept_filter"] = dept_filter
    st.session_state["massive_start_time"]  = time.time()


# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — COLLECTE MASSIVE
# ══════════════════════════════════════════════════════════════════════════════

with tab_massive:

    collector   = st.session_state.get("massive_collector")
    is_running  = collector is not None and collector.is_running
    is_done     = collector is not None and collector.get_state().get("is_done", False)
    is_stopped  = collector is not None and not is_running and not is_done

    # ══════════════════════════════════════════════════════════════════════════
    # VUE 1 — COLLECTE EN COURS
    # ══════════════════════════════════════════════════════════════════════════
    if is_running:
        state       = collector.get_state()
        now         = time.time()
        start_time  = st.session_state.get("massive_start_time", now)
        elapsed     = now - start_time

        done_combos  = state.get("done_combinations", 0)
        total_combos = state.get("total_combinations", 1)
        progress_val = float(state.get("progress", 0.0))
        current_task = state.get("current_task", "…")
        pct          = progress_val * 100

        total_found  = state.get("total_found", 0)
        with_website = state.get("with_website", 0)
        with_email   = state.get("with_email", 0)
        api_calls    = state.get("api_calls", 0)

        # ── Header ────────────────────────────────────────────────────────────
        hdr_col, stop_col = st.columns([5, 1])
        with hdr_col:
            st.markdown('<span class="status-running">● Collecte en cours</span>', unsafe_allow_html=True)
        with stop_col:
            if st.button("⏹ Arrêter", type="secondary", use_container_width=True):
                collector.stop()
                st.toast("Arrêt demandé — fin après la tâche en cours.")

        st.divider()

        # ── Barre de progression ──────────────────────────────────────────────
        st.progress(max(progress_val, 0.002))  # minimum visible même à 0%

        # Calcul ETA
        if done_combos > 5 and elapsed > 0:
            rate_per_sec = done_combos / elapsed
            remaining    = total_combos - done_combos
            eta_sec      = remaining / rate_per_sec
            eta_str      = _fmt_duration(eta_sec)
            speed_str    = f"{rate_per_sec * 60:.0f} combos/min"
        else:
            eta_str   = "calcul…"
            speed_str = "—"

        prog_col1, prog_col2, prog_col3, prog_col4 = st.columns(4)
        prog_col1.markdown(f"**{pct:.1f}%** complété")
        prog_col2.markdown(f"**{done_combos:,}** / {total_combos:,} combinaisons")
        prog_col3.markdown(f"⏱ **{_fmt_duration(elapsed)}** écoulées")
        prog_col4.markdown(f"🏁 ETA **{eta_str}** · {speed_str}")

        st.markdown(
            f'<div class="task-pill">🔄 {current_task}</div>',
            unsafe_allow_html=True,
        )

        # ── Erreur ────────────────────────────────────────────────────────────
        if state.get("error"):
            st.error(f"Dernière erreur : {state['error']}")

        st.divider()

        # ── Métriques ─────────────────────────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">🏪 Entreprises trouvées</div>'
                f'<div class="metric-value">{total_found:,}</div></div>',
                unsafe_allow_html=True,
            )
        with m2:
            pct_web = f"{with_website / total_found * 100:.0f}%" if total_found else "—"
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">🌐 Avec site web</div>'
                f'<div class="metric-value">{with_website:,}</div></div>',
                unsafe_allow_html=True,
            )
        with m3:
            pct_email = f"{with_email / total_found * 100:.0f}%" if total_found else "—"
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">📧 Avec email pro</div>'
                f'<div class="metric-value">{with_email:,}</div></div>',
                unsafe_allow_html=True,
            )
        with m4:
            st.markdown(
                f'<div class="metric-card"><div class="metric-label">📡 Appels API Google</div>'
                f'<div class="metric-value">{api_calls:,}</div></div>',
                unsafe_allow_html=True,
            )

        # ── Taux de conversion ────────────────────────────────────────────────
        if total_found > 0:
            st.caption(
                f"Taux email : **{with_email / total_found * 100:.1f}%** des entreprises trouvées · "
                f"Taux web : **{with_website / total_found * 100:.1f}%**"
            )

        st.divider()

        # ── Tableau live ──────────────────────────────────────────────────────
        st.markdown('<div class="section-header">Derniers résultats collectés</div>', unsafe_allow_html=True)

        last_results = state.get("last_results", [])
        if last_results:
            df_live = pd.DataFrame(last_results[::-1])
            col_cfg = {
                "nom":       st.column_config.TextColumn("Nom", width="medium"),
                "ville":     st.column_config.TextColumn("Ville", width="small"),
                "secteur":   st.column_config.TextColumn("Secteur", width="small"),
                "email":     st.column_config.TextColumn("Email", width="medium"),
                "telephone": st.column_config.TextColumn("Téléphone", width="small"),
                "site_web":  st.column_config.LinkColumn("Site Web", width="medium", display_text="🔗"),
                "note":      st.column_config.NumberColumn("Note ★", format="%.1f", width="small"),
            }
            st.dataframe(df_live, use_container_width=True, column_config=col_cfg, hide_index=True)
        else:
            st.info("Les premiers résultats apparaîtront dans quelques secondes…")

        # ── Snapshot CSV ──────────────────────────────────────────────────────
        _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
        if os.path.exists(_output_file):
            st.divider()
            csv_data = _read_csv_safe(_output_file)
            if csv_data is not None:
                file_kb = len(csv_data) // 1024
                st.download_button(
                    label=f"📥 Télécharger snapshot CSV ({file_kb} Ko · {total_found:,} entrées)",
                    data=csv_data,
                    file_name=_output_file,
                    mime="text/csv",
                    help="Télécharge les données collectées jusqu'à maintenant.",
                )

        time.sleep(2)
        st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # VUE 2 — COLLECTE TERMINÉE OU ARRÊTÉE
    # ══════════════════════════════════════════════════════════════════════════
    elif is_done or is_stopped:

        state       = collector.get_state()
        _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
        total_found  = state.get("total_found", 0)
        with_email   = state.get("with_email", 0)
        with_website = state.get("with_website", 0)
        api_calls    = state.get("api_calls", 0)
        done_combos  = state.get("done_combinations", 0)
        total_combos = state.get("total_combinations", 1)
        start_time   = st.session_state.get("massive_start_time")
        elapsed      = time.time() - start_time if start_time else 0

        # ── Status ────────────────────────────────────────────────────────────
        if is_done:
            st.success(f"✅ Collecte terminée — {total_found:,} entreprises collectées en {_fmt_duration(elapsed)}")
        else:
            pct_done = done_combos / total_combos * 100 if total_combos else 0
            st.warning(
                f"⏹ Collecte arrêtée à **{pct_done:.1f}%** "
                f"({done_combos:,} / {total_combos:,} combinaisons) — "
                f"{total_found:,} entreprises sauvegardées"
            )

        st.divider()

        # ── Métriques finales ─────────────────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("🏪 Entreprises", f"{total_found:,}")
        m2.metric("🌐 Avec site web", f"{with_website:,}")
        m3.metric("📧 Avec email pro", f"{with_email:,}",
                  delta=f"{with_email / total_found * 100:.1f}%" if total_found else None)
        m4.metric("📡 Appels API", f"{api_calls:,}")

        if state.get("error"):
            st.error(f"Dernière erreur : {state['error']}")

        st.divider()

        # ── Téléchargement ────────────────────────────────────────────────────
        if os.path.exists(_output_file):
            csv_data = _read_csv_safe(_output_file)
            if csv_data is not None:
                file_kb = len(csv_data) // 1024
                st.download_button(
                    label=f"📥 Télécharger le CSV ({file_kb} Ko · {total_found:,} entrées)",
                    data=csv_data,
                    file_name=_output_file,
                    mime="text/csv",
                    use_container_width=True,
                    type="primary",
                )
            else:
                st.warning("Impossible de lire le fichier CSV. Réessayez dans un instant.")
        else:
            st.info("Aucun fichier CSV trouvé.")

        st.divider()

        # ── Actions ───────────────────────────────────────────────────────────
        prev_sectors     = st.session_state.get("massive_sectors")
        prev_dept_filter = st.session_state.get("massive_dept_filter")
        file_exists      = os.path.exists(_output_file)

        action_col1, action_col2 = st.columns(2)

        with action_col1:
            resume_disabled = not (file_exists and prev_sectors)
            if st.button(
                "🔁 Reprendre la collecte",
                use_container_width=True,
                disabled=resume_disabled,
                help="Reprend la collecte là où elle s'est arrêtée (skip les entreprises déjà collectées).",
            ):
                try:
                    _start_collector(_output_file, prev_sectors, prev_dept_filter, resume=True)
                    st.rerun()
                except ValueError as exc:
                    st.error(f"⛔ {exc}")

        with action_col2:
            if st.button("🔄 Nouvelle collecte", use_container_width=True):
                for key in ("massive_collector", "massive_output_file", "massive_sectors",
                            "massive_dept_filter", "massive_start_time"):
                    st.session_state.pop(key, None)
                st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # VUE 3 — CONFIGURATION (état initial)
    # ══════════════════════════════════════════════════════════════════════════
    else:
        st.subheader("⚙️ Configuration")

        cfg_col1, cfg_col2 = st.columns(2)

        with cfg_col1:
            st.markdown('<div class="section-header">Secteurs & zone</div>', unsafe_allow_html=True)

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
            st.markdown('<div class="section-header">Fichier de sortie</div>', unsafe_allow_html=True)

            output_filename = st.text_input(
                "Fichier CSV de sortie",
                value="collecte_france.csv",
                help="Nom du fichier CSV où les résultats seront sauvegardés.",
            )

            file_exists = os.path.exists(output_filename) if output_filename else False

            resume_mode = st.checkbox(
                "♻️ Reprendre une collecte existante",
                value=file_exists,
                help=(
                    "Si le fichier existe déjà, charge les place_ids déjà collectés "
                    "et reprend la collecte sans re-scraper les entreprises connues."
                ),
            )

            if file_exists:
                file_size = os.path.getsize(output_filename)
                st.info(f"📄 Fichier existant : `{output_filename}` ({file_size // 1024} Ko)")
            else:
                st.caption("📄 Le fichier sera créé au démarrage.")

        # ── Estimation ────────────────────────────────────────────────────────
        if selected_sectors:
            st.divider()
            total_types = sum(len(SECTORS[s]["types"]) for s in selected_sectors)
            n_cities = len([
                c for c in FRENCH_CITIES
                if dept_filter is None or c[1] in dept_filter
            ])
            n_combos = n_cities * total_types
            est_min = n_combos * 0.5 / 60
            st.info(
                f"**Estimation :** {n_cities} villes × {total_types} types = "
                f"**{n_combos:,} requêtes Google Places** "
                f"(~{est_min:.0f} min sans scraping email · ~{est_min * 3:.0f} min avec emails)"
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
                _start_collector(output_filename.strip(), selected_sectors, dept_filter, resume=resume_mode)
                st.rerun()
            except ValueError as exc:
                st.error(f"⛔ {exc}")
