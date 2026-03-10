"""
app.py — Interface web Streamlit pour le scraper Google Places

Lancement :
    streamlit run app.py
"""

from __future__ import annotations

import os
import time
import unicodedata

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

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Skypeo Scrapper",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* ── Layout global ────────────────────────────────────────────────────────── */
[data-testid="stAppViewContainer"] { background: #f1f5f9; }
[data-testid="stHeader"]           { background: transparent; }
[data-testid="block-container"]    { padding-top: 1.5rem; padding-bottom: 2rem; }

/* ── Header app ───────────────────────────────────────────────────────────── */
.app-header {
    display: flex;
    align-items: center;
    gap: 14px;
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 14px;
    padding: 18px 24px;
    margin-bottom: 24px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.app-icon  { font-size: 2rem; line-height: 1; }
.app-title { font-size: 1.45rem; font-weight: 700; color: #0f172a; line-height: 1.2; }
.app-sub   { font-size: 0.8rem; color: #64748b; margin-top: 2px; }

/* ── Cards génériques ─────────────────────────────────────────────────────── */
.card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 22px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

/* ── Metric cards ─────────────────────────────────────────────────────────── */
.metric-card {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 18px 16px 14px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    height: 100%;
}
.metric-icon  { font-size: 1.3rem; margin-bottom: 6px; }
.metric-value { font-size: 2rem; font-weight: 800; color: #0f172a; line-height: 1.1; }
.metric-label { font-size: 0.72rem; color: #64748b; font-weight: 500;
                text-transform: uppercase; letter-spacing: 0.04em; margin-top: 4px; }
.metric-sub   { font-size: 0.8rem; color: #3b82f6; font-weight: 600; margin-top: 2px; }

/* ── Barre de progression custom ──────────────────────────────────────────── */
.progress-block {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 18px 22px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    margin-bottom: 4px;
}
.progress-bar-track {
    background: #e2e8f0;
    border-radius: 99px;
    height: 10px;
    overflow: hidden;
    margin: 12px 0 14px;
}
.progress-bar-fill {
    background: linear-gradient(90deg, #3b82f6, #6366f1);
    border-radius: 99px;
    height: 100%;
    transition: width 0.6s ease;
}
.progress-stats {
    display: flex;
    gap: 0;
    justify-content: space-between;
}
.progress-stat { text-align: center; flex: 1; }
.progress-stat-value { font-size: 1rem; font-weight: 700; color: #0f172a; }
.progress-stat-label { font-size: 0.68rem; color: #94a3b8; text-transform: uppercase;
                       letter-spacing: 0.03em; margin-top: 1px; }

/* ── Status badge ─────────────────────────────────────────────────────────── */
.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 14px;
    border-radius: 99px;
    font-size: 0.85rem;
    font-weight: 600;
}
.status-running {
    background: #dcfce7;
    color: #15803d;
}
.status-done    { background: #dbeafe; color: #1d4ed8; }
.status-stopped { background: #fef9c3; color: #854d0e; }

.dot-running {
    width: 8px; height: 8px;
    background: #16a34a;
    border-radius: 50%;
    animation: pulse-dot 1.5s ease-in-out infinite;
}
@keyframes pulse-dot {
    0%,100% { opacity: 1; transform: scale(1); }
    50%      { opacity: 0.5; transform: scale(0.85); }
}

/* ── Tâche courante ───────────────────────────────────────────────────────── */
.task-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #eff6ff;
    color: #2563eb;
    border: 1px solid #bfdbfe;
    border-radius: 99px;
    padding: 4px 14px;
    font-size: 0.8rem;
    font-weight: 500;
    margin-top: 10px;
}

/* ── Sector card (config) ─────────────────────────────────────────────────── */
.sector-card {
    background: white;
    border: 2px solid #e2e8f0;
    border-radius: 10px;
    padding: 14px 16px;
    cursor: pointer;
    transition: border-color 0.15s, box-shadow 0.15s;
}
.sector-card:hover { border-color: #93c5fd; box-shadow: 0 2px 8px rgba(59,130,246,0.12); }

/* ── Section label ────────────────────────────────────────────────────────── */
.section-label {
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #94a3b8;
    margin-bottom: 10px;
    margin-top: 6px;
}

/* ── Summary card (fin de collecte) ──────────────────────────────────────── */
.summary-card {
    background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
    border-radius: 14px;
    padding: 24px 28px;
    color: white;
    margin-bottom: 8px;
}
.summary-card.stopped {
    background: linear-gradient(135deg, #78350f 0%, #d97706 100%);
}
.summary-title { font-size: 1.2rem; font-weight: 700; margin-bottom: 4px; }
.summary-sub   { font-size: 0.85rem; opacity: 0.85; }

/* ── Masquer éléments Streamlit parasites ─────────────────────────────────── */
[data-testid="stDecoration"] { display: none; }
footer { display: none; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s   = divmod(rem, 60)
    if h:   return f"{h}h {m:02d}m"
    if m:   return f"{m}m {s:02d}s"
    return  f"{s}s"


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode()
    text = text.lower().replace("&", "").replace("-", "_").replace("'", "").replace(" ", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def _generate_filename(sectors: list[str], dept_code: str | None) -> str:
    sectors_part = "_".join(_slugify(SECTORS[s]["label"]) for s in sectors) if sectors else "collecte"
    dept_part    = "france" if dept_code is None else _slugify(FRENCH_DEPARTMENTS.get(dept_code, dept_code))
    return f"{sectors_part}_{dept_part}.csv"


def _read_csv_safe(path: str) -> bytes | None:
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError:
        return None


def _start_collector(output_path: str, sectors: list, dept_filter, resume: bool) -> None:
    new_collector = MassiveCollector(output_path=output_path, sectors=sectors, dept_filter=dept_filter)
    file_exists   = os.path.exists(output_path)
    if resume and file_exists:
        loaded = new_collector.load_existing()
        st.toast(f"Reprise : {loaded:,} entreprises déjà collectées ignorées.")
    elif file_exists and not resume:
        os.remove(output_path)
    new_collector.start()
    st.session_state.update({
        "massive_collector":   new_collector,
        "massive_output_file": output_path,
        "massive_sectors":     sectors,
        "massive_dept_filter": dept_filter,
        "massive_start_time":  time.time(),
    })


def _metric_card(icon: str, label: str, value: str, sub: str = "") -> str:
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ""
    return (
        f'<div class="metric-card">'
        f'<div class="metric-icon">{icon}</div>'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-label">{label}</div>'
        f'{sub_html}</div>'
    )


# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="app-header">
  <div class="app-icon">🔍</div>
  <div>
    <div class="app-title">Skypeo Scrapper</div>
    <div class="app-sub">Collecte d'emails professionnels · Google Places API · 100% gratuit</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── État global ────────────────────────────────────────────────────────────────

collector  = st.session_state.get("massive_collector")
is_running = collector is not None and collector.is_running
is_done    = collector is not None and collector.get_state().get("is_done", False)
is_stopped = collector is not None and not is_running and not is_done


# ══════════════════════════════════════════════════════════════════════════════
# VUE 1 — EN COURS
# ══════════════════════════════════════════════════════════════════════════════
if is_running:
    state        = collector.get_state()
    now          = time.time()
    start_time   = st.session_state.get("massive_start_time", now)
    elapsed      = now - start_time

    done_combos  = state.get("done_combinations", 0)
    total_combos = state.get("total_combinations", 1)
    progress_val = float(state.get("progress", 0.0))
    current_task = state.get("current_task", "…")
    pct          = progress_val * 100

    total_found  = state.get("total_found", 0)
    with_website = state.get("with_website", 0)
    with_email   = state.get("with_email", 0)
    api_calls    = state.get("api_calls", 0)

    # ETA
    if done_combos > 5 and elapsed > 0:
        rate      = done_combos / elapsed
        eta_str   = _fmt_duration((total_combos - done_combos) / rate)
        speed_str = f"{rate * 60:.0f}/min"
    else:
        eta_str = speed_str = "…"

    # ── Barre de statut + bouton stop ─────────────────────────────────────────
    hdr_l, hdr_r = st.columns([5, 1])
    with hdr_l:
        st.markdown(
            '<span class="status-badge status-running">'
            '<span class="dot-running"></span>Collecte en cours'
            '</span>',
            unsafe_allow_html=True,
        )
    with hdr_r:
        if st.button("⏹ Arrêter", type="secondary", use_container_width=True):
            collector.stop()
            st.toast("Arrêt demandé — fin après la tâche en cours.")

    # ── Bloc progression ──────────────────────────────────────────────────────
    bar_pct = max(pct, 0.3)
    st.markdown(f"""
<div class="progress-block">
  <div style="display:flex;justify-content:space-between;align-items:baseline">
    <span style="font-weight:700;font-size:1.05rem;color:#0f172a">{pct:.1f}%</span>
    <span style="font-size:0.78rem;color:#64748b">{done_combos:,} / {total_combos:,} combinaisons</span>
  </div>
  <div class="progress-bar-track">
    <div class="progress-bar-fill" style="width:{bar_pct:.2f}%"></div>
  </div>
  <div class="progress-stats">
    <div class="progress-stat">
      <div class="progress-stat-value">⏱ {_fmt_duration(elapsed)}</div>
      <div class="progress-stat-label">Écoulé</div>
    </div>
    <div class="progress-stat">
      <div class="progress-stat-value">🏁 {eta_str}</div>
      <div class="progress-stat-label">Restant</div>
    </div>
    <div class="progress-stat">
      <div class="progress-stat-value">⚡ {speed_str}</div>
      <div class="progress-stat-label">Vitesse</div>
    </div>
  </div>
</div>
<div class="task-pill">🔄 {current_task}</div>
""", unsafe_allow_html=True)

    if state.get("error"):
        st.error(f"Dernière erreur : {state['error']}")

    st.write("")

    # ── Métriques ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    pct_web   = f"{with_website / total_found * 100:.0f}% ont un site" if total_found else ""
    pct_email = f"{with_email / total_found * 100:.0f}% ont un email" if total_found else ""

    with c1: st.markdown(_metric_card("🏪", "Entreprises trouvées", f"{total_found:,}"), unsafe_allow_html=True)
    with c2: st.markdown(_metric_card("🌐", "Avec site web",        f"{with_website:,}", pct_web),   unsafe_allow_html=True)
    with c3: st.markdown(_metric_card("📧", "Avec email",           f"{with_email:,}",   pct_email), unsafe_allow_html=True)
    with c4: st.markdown(_metric_card("📡", "Appels API Google",    f"{api_calls:,}"),   unsafe_allow_html=True)

    st.write("")

    # ── Tableau live ──────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Derniers résultats</div>', unsafe_allow_html=True)

    last_results = state.get("last_results", [])
    if last_results:
        df = pd.DataFrame(last_results[::-1])
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "nom":       st.column_config.TextColumn("Nom", width="medium"),
                "email":     st.column_config.TextColumn("Email", width="medium"),
                "telephone": st.column_config.TextColumn("Téléphone", width="small"),
                "ville":     st.column_config.TextColumn("Ville", width="small"),
                "secteur":   st.column_config.TextColumn("Secteur", width="small"),
                "site_web":  st.column_config.LinkColumn("Site", width="small", display_text="🔗"),
                "note":      st.column_config.NumberColumn("★", format="%.1f", width="small"),
            },
            column_order=["nom", "email", "telephone", "ville", "secteur", "site_web", "note"],
        )
    else:
        st.info("Les premiers résultats apparaîtront dans quelques secondes…")

    # ── Snapshot CSV ──────────────────────────────────────────────────────────
    _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
    if os.path.exists(_output_file):
        st.write("")
        csv_data = _read_csv_safe(_output_file)
        if csv_data:
            st.download_button(
                label=f"📥 Télécharger snapshot · {len(csv_data)//1024} Ko · {total_found:,} entrées",
                data=csv_data,
                file_name=_output_file,
                mime="text/csv",
            )

    time.sleep(2)
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# VUE 2 — TERMINÉE / ARRÊTÉE
# ══════════════════════════════════════════════════════════════════════════════
elif is_done or is_stopped:

    state        = collector.get_state()
    _output_file = st.session_state.get("massive_output_file", "collecte_france.csv")
    total_found  = state.get("total_found", 0)
    with_email   = state.get("with_email", 0)
    with_website = state.get("with_website", 0)
    api_calls    = state.get("api_calls", 0)
    done_combos  = state.get("done_combinations", 0)
    total_combos = state.get("total_combinations", 1)
    start_time   = st.session_state.get("massive_start_time")
    elapsed      = time.time() - start_time if start_time else 0

    # ── Banner ────────────────────────────────────────────────────────────────
    if is_done:
        pct_email_str = f"{with_email / total_found * 100:.1f}%" if total_found else "—"
        st.markdown(f"""
<div class="summary-card">
  <div class="summary-title">✅ Collecte terminée</div>
  <div class="summary-sub">
    {total_found:,} entreprises · {with_email:,} emails ({pct_email_str}) · durée {_fmt_duration(elapsed)}
  </div>
</div>
""", unsafe_allow_html=True)
    else:
        pct_done = done_combos / total_combos * 100 if total_combos else 0
        st.markdown(f"""
<div class="summary-card stopped">
  <div class="summary-title">⏹ Collecte arrêtée à {pct_done:.1f}%</div>
  <div class="summary-sub">
    {total_found:,} entreprises sauvegardées · {done_combos:,} / {total_combos:,} combinaisons traitées
  </div>
</div>
""", unsafe_allow_html=True)

    st.write("")

    # ── Métriques ─────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    pct_web   = f"{with_website / total_found * 100:.1f}% du total" if total_found else ""
    pct_email = f"{with_email   / total_found * 100:.1f}% du total" if total_found else ""
    with c1: st.markdown(_metric_card("🏪", "Entreprises",    f"{total_found:,}"),  unsafe_allow_html=True)
    with c2: st.markdown(_metric_card("🌐", "Avec site web",  f"{with_website:,}", pct_web),   unsafe_allow_html=True)
    with c3: st.markdown(_metric_card("📧", "Avec email",     f"{with_email:,}",   pct_email), unsafe_allow_html=True)
    with c4: st.markdown(_metric_card("📡", "Appels API",     f"{api_calls:,}"),   unsafe_allow_html=True)

    if state.get("error"):
        st.write("")
        st.error(f"Dernière erreur : {state['error']}")

    st.write("")

    # ── Téléchargement ────────────────────────────────────────────────────────
    if os.path.exists(_output_file):
        csv_data = _read_csv_safe(_output_file)
        if csv_data:
            st.download_button(
                label=f"📥 Télécharger le CSV — {len(csv_data)//1024} Ko · {total_found:,} entrées",
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

    st.write("")

    # ── Actions ───────────────────────────────────────────────────────────────
    prev_sectors     = st.session_state.get("massive_sectors")
    prev_dept_filter = st.session_state.get("massive_dept_filter")
    file_exists      = os.path.exists(_output_file)

    col1, col2 = st.columns(2)
    with col1:
        if st.button(
            "🔁 Reprendre la collecte",
            use_container_width=True,
            disabled=not (file_exists and prev_sectors),
            help="Repart là où la collecte s'est arrêtée, sans re-scraper les entreprises déjà collectées.",
        ):
            try:
                _start_collector(_output_file, prev_sectors, prev_dept_filter, resume=True)
                st.rerun()
            except ValueError as exc:
                st.error(f"⛔ {exc}")
    with col2:
        if st.button("🔄 Nouvelle collecte", use_container_width=True):
            for k in ("massive_collector","massive_output_file","massive_sectors","massive_dept_filter","massive_start_time"):
                st.session_state.pop(k, None)
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# VUE 3 — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
else:

    left, right = st.columns([3, 2], gap="large")

    # ── Colonne gauche : secteurs + zone ──────────────────────────────────────
    with left:
        st.markdown('<div class="section-label">Secteurs à collecter</div>', unsafe_allow_html=True)

        sector_cols = st.columns(2)
        selected_sectors: list[str] = []
        for i, (key, sector) in enumerate(SECTORS.items()):
            with sector_cols[i % 2]:
                if st.checkbox(
                    f"{sector['icon']} {sector['label']}",
                    value=True,
                    key=f"sector_{key}",
                ):
                    selected_sectors.append(key)

        st.write("")
        st.markdown('<div class="section-label">Zone géographique</div>', unsafe_allow_html=True)

        dept_options = {"🌍 France entière": None}
        dept_options.update({
            f"{code} — {name}": code
            for code, name in sorted(FRENCH_DEPARTMENTS.items(), key=lambda x: x[0])
        })
        selected_dept_label = st.selectbox(
            "Zone",
            options=list(dept_options.keys()),
            index=0,
            label_visibility="collapsed",
        )
        dept_filter_value = dept_options[selected_dept_label]
        dept_filter       = [dept_filter_value] if dept_filter_value else None

        # Estimation
        if selected_sectors:
            total_types = sum(len(SECTORS[s]["types"]) for s in selected_sectors)
            n_cities    = sum(1 for c in FRENCH_CITIES if dept_filter is None or c[1] in dept_filter)
            n_combos    = n_cities * total_types
            est_min     = n_combos * 0.5 / 60
            st.write("")
            st.markdown(f"""
<div class="card" style="background:#eff6ff;border-color:#bfdbfe;">
  <div style="font-size:0.78rem;font-weight:600;color:#1d4ed8;margin-bottom:6px">📊 Estimation</div>
  <div style="font-size:0.85rem;color:#1e40af">
    <b>{n_cities:,} villes</b> × <b>{total_types} types</b> = <b>{n_combos:,} requêtes</b>
  </div>
  <div style="font-size:0.78rem;color:#3b82f6;margin-top:4px">
    ~{est_min:.0f} min (API seule) · ~{est_min*3:.0f} min (avec scraping emails)
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Colonne droite : fichier + lancement ──────────────────────────────────
    with right:
        st.markdown('<div class="section-label">Fichier de sortie</div>', unsafe_allow_html=True)

        auto_filename   = _generate_filename(selected_sectors, dept_filter_value)
        output_filename = st.text_input(
            "Nom du fichier CSV",
            value=auto_filename,
            label_visibility="collapsed",
            help="Généré automatiquement · modifiable",
        )

        file_exists = os.path.exists(output_filename) if output_filename else False

        if file_exists:
            file_size = os.path.getsize(output_filename)
            st.markdown(f"""
<div style="font-size:0.78rem;color:#0369a1;background:#e0f2fe;
            border-radius:8px;padding:8px 12px;margin:6px 0;">
  📄 Fichier existant : <code>{output_filename}</code> — {file_size//1024} Ko
</div>
""", unsafe_allow_html=True)
        else:
            st.caption("Le fichier sera créé au démarrage.")

        resume_mode = st.checkbox(
            "♻️ Reprendre une collecte existante",
            value=file_exists,
            help="Skip les entreprises déjà présentes dans le fichier.",
        )

        st.write("")

        # Résumé de la sélection
        if selected_sectors:
            sector_labels = " · ".join(f"{SECTORS[s]['icon']} {SECTORS[s]['label']}" for s in selected_sectors)
            zone_label    = selected_dept_label if dept_filter_value else "France entière"
            st.markdown(f"""
<div class="card" style="margin-bottom:16px;padding:14px 16px;">
  <div style="font-size:0.72rem;color:#94a3b8;font-weight:700;text-transform:uppercase;
              letter-spacing:0.06em;margin-bottom:8px">Récapitulatif</div>
  <div style="font-size:0.83rem;color:#374151;line-height:1.6">
    <b>Secteurs :</b> {sector_labels}<br>
    <b>Zone :</b> {zone_label}<br>
    <b>Fichier :</b> <code style="font-size:0.78rem">{output_filename or "—"}</code>
  </div>
</div>
""", unsafe_allow_html=True)

        start_disabled = not selected_sectors or not (output_filename or "").strip()
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

        if start_disabled and not selected_sectors:
            st.caption("⚠️ Sélectionnez au moins un secteur.")
