"""Dashboard Streamlit: precios en tiempo real de dólares y granos
desde Matba Rofex (pyRofex WebSocket) con persistencia en Supabase.

Diseño basado en tarjetas dinámicas que se refrescan automáticamente
a medida que entran nuevos ticks (desde el WebSocket → memoria + Supabase)."""
from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime
from itertools import combinations

import pandas as pd
import pytz
import streamlit as st

import db
from rofex_manager import RofexManager
from symbol_utils import keep_for_dashboard, display_symbol, parse_symbol, sort_key

st.set_page_config(
    page_title="Matba Rofex Dashboard",
    page_icon="📈",
    layout="wide",
)

BA_TZ = pytz.timezone("America/Argentina/Buenos_Aires")

POSITIVE_COLOR = "#16a34a"
NEGATIVE_COLOR = "#dc2626"
NEUTRAL_COLOR  = "#6b7280"

# ── orden de familias en la pestaña de pases agropecuarios ──────────────────
FAMILIA_ORDEN = ["SOJ", "MAI", "TRI", "GIR", "SOR", "CEB"]

CARD_CSS = """
<style>
.metric-card {
    background: linear-gradient(180deg, #ffffff 0%, #f9fafb 100%);
    border: 1px solid #e5e7eb;
    border-radius: 14px;
    padding: 18px 20px;
    margin-bottom: 14px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    transition: transform 0.08s ease, box-shadow 0.08s ease;
}
.metric-card:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 10px rgba(0,0,0,0.07);
}
.metric-card.positive { border-left: 4px solid #16a34a; }
.metric-card.negative { border-left: 4px solid #dc2626; }
.metric-card.neutral  { border-left: 4px solid #9ca3af; }

.metric-card .symbol {
    font-size: 0.85rem;
    color: #6b7280;
    font-weight: 600;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    margin-bottom: 6px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.metric-card .price {
    font-size: 1.65rem;
    font-weight: 700;
    color: #111827;
    line-height: 1.1;
}
.metric-card .change {
    font-size: 1.0rem;
    font-weight: 700;
    margin-top: 6px;
}
.metric-card .change.positive { color: #16a34a; }
.metric-card .change.negative { color: #dc2626; }
.metric-card .change.neutral  { color: #6b7280; }

.metric-card .abs-change {
    font-size: 0.85rem;
    font-weight: 600;
    margin-left: 4px;
    opacity: 0.85;
}

.metric-card .footer {
    margin-top: 10px;
    padding-top: 8px;
    border-top: 1px dashed #e5e7eb;
    display: flex;
    justify-content: space-between;
    font-size: 0.78rem;
    color: #6b7280;
}
.metric-card .footer .label { color: #9ca3af; }
.metric-card .footer .value { color: #374151; font-weight: 600; }

.section-title {
    font-size: 1.35rem;
    font-weight: 700;
    color: #111827;
    margin: 6px 0 12px 0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.section-title .badge {
    background: #eef2ff;
    color: #4f46e5;
    border-radius: 999px;
    padding: 2px 10px;
    font-size: 0.78rem;
    font-weight: 700;
}
.empty-card {
    border: 1px dashed #d1d5db;
    border-radius: 12px;
    padding: 18px;
    color: #6b7280;
    text-align: center;
    font-size: 0.9rem;
    background: #fafafa;
}

.pase-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-left: 4px solid #4f46e5;
    border-radius: 12px;
    padding: 14px 16px;
    margin-bottom: 12px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}
.pase-card .pair {
    font-size: 0.92rem;
    font-weight: 700;
    color: #111827;
    margin-bottom: 6px;
}
.pase-card .pair .arrow { color: #6b7280; margin: 0 6px; }
.pase-card .legs {
    font-size: 0.78rem;
    color: #6b7280;
    margin-bottom: 8px;
}
.pase-card .legs .num { color: #111827; font-weight: 600; }
.pase-card .metrics {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    padding-top: 8px;
    border-top: 1px dashed #e5e7eb;
}
.pase-card .metric .label {
    display: block;
    font-size: 0.7rem;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
.pase-card .metric .value {
    font-size: 1.05rem;
    font-weight: 700;
    color: #111827;
}
.pase-card .metric .value.positive { color: #16a34a; }
.pase-card .metric .value.negative { color: #dc2626; }
</style>
"""

GRAIN_NAMES = {
    "SOJ": "Soja",
    "MAI": "Maíz",
    "TRI": "Trigo",
    "GIR": "Girasol",
    "SOR": "Sorgo",
    "CEB": "Cebada",
    "DLR": "Dólar",
}

MONTHS_SHORT = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

GRAIN_FAMILY_MAP = {
    "SOJ": ["SOJ.ROS", "SOJ"],
    "MAI": ["MAI.ROS", "MAI"],
    "TRI": ["TRI.ROS", "TRI"],
}


# ── helpers de formato ───────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Conectando a Matba Rofex (ROFEX)...")
def get_manager() -> RofexManager:
    mgr = RofexManager.get()
    mgr.initialize()
    return mgr


def _fmt_price(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{v:,.2f}"


def _fmt_int(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_change(v) -> tuple[str, str]:
    if v is None or pd.isna(v):
        return "—", "neutral"
    if v > 0:
        return f"▲ +{v:.2f}%", "positive"
    if v < 0:
        return f"▼ {v:.2f}%", "negative"
    return f"{v:.2f}%", "neutral"


def _fmt_abs_change(last, prev) -> str:
    if last is None or prev is None or pd.isna(last) or pd.isna(prev):
        return ""
    diff = last - prev
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:,.2f}"


# ── tarjetas de precio ───────────────────────────────────────────────────────

def _render_card(row: dict) -> str:
    symbol    = display_symbol(row.get("symbol", "—"))
    last      = row.get("last_price")
    change    = row.get("change_pct")
    volume    = row.get("trade_volume")
    prev_close = row.get("settlement_price")

    change_text, change_cls = _fmt_change(change)
    abs_change = _fmt_abs_change(last, prev_close)
    abs_html = (
        f'<span class="abs-change {change_cls}">({abs_change})</span>'
        if abs_change else ""
    )
    return f"""
    <div class="metric-card {change_cls}">
        <div class="symbol" title="{symbol}">{symbol}</div>
        <div class="price">{_fmt_price(last)}</div>
        <div class="change {change_cls}">{change_text} {abs_html}</div>
        <div class="footer">
            <span><span class="label">Vol.</span> <span class="value">{_fmt_int(volume)}</span></span>
            <span><span class="label">Cierre prev.</span> <span class="value">{_fmt_price(prev_close)}</span></span>
        </div>
    </div>
    """


# ── utilidades de expiración ─────────────────────────────────────────────────

def _exp_label(exp: tuple[int, int]) -> str:
    y, m = exp
    return f"{MONTHS_SHORT.get(m, '?')}{str(y)[-2:]}"


def _exp_to_date(exp: tuple[int, int]) -> date:
    y, m = exp
    return date(y, m, monthrange(y, m)[1])


# ── construcción de pases ────────────────────────────────────────────────────

def build_pases(rows: list[dict], consecutive_only: bool = True) -> list[dict]:
    families: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        symbol = r.get("symbol", "")
        info = parse_symbol(symbol)
        if info.is_option or info.is_spread or info.is_dispo:
            continue
        if not info.first_exp:
            continue
        if r.get("last_price") in (None, 0):
            continue
        if not consecutive_only and not (r.get("trade_volume") or 0) > 0:
            continue
        families[info.family].append({
            "symbol": symbol,
            "underlying": r.get("underlying", ""),
            "expiration": info.first_exp,
            "last_price": r.get("last_price"),
            "trade_volume": r.get("trade_volume") or 0,
        })

    pases: list[dict] = []
    today = date.today()
    for family, items in families.items():
        unique: dict[tuple[int, int], dict] = {}
        for it in items:
            key = it["expiration"]
            if key not in unique:
                unique[key] = it
        sorted_items = sorted(unique.values(), key=lambda x: x["expiration"])

        pairs = (
            list(zip(sorted_items, sorted_items[1:]))
            if consecutive_only
            else list(combinations(sorted_items, 2))
        )

        for short, long in pairs:
            p_s = short["last_price"]
            p_l = long["last_price"]
            if not p_s or not p_l:
                continue
            spread = p_l - p_s
            d_short = _exp_to_date(short["expiration"])
            d_long  = _exp_to_date(long["expiration"])
            days = (d_long - d_short).days
            tna = None
            if days > 0 and p_s > 0:
                tna = ((p_l / p_s) - 1) * (365 / days) * 100
            pases.append({
                "family":       family,
                "family_name":  GRAIN_NAMES.get(family, family),
                "underlying":   short.get("underlying", family),
                "short_symbol": short["symbol"],
                "long_symbol":  long["symbol"],
                "short_exp":    short["expiration"],
                "long_exp":     long["expiration"],
                "short_label":  _exp_label(short["expiration"]),
                "long_label":   _exp_label(long["expiration"]),
                "p_short": p_s,
                "p_long":  p_l,
                "spread":  spread,
                "days":    days,
                "tna":     tna,
                "expired": d_short < today,
            })

    # Ordenar: por familia_name, luego short_exp, luego long_exp
    pases.sort(key=lambda p: (p["family_name"], p.get("short_exp", (0, 0)), p.get("long_exp", (0, 0))))
    return pases


def _build_pases_disponible(granos: list[dict], precios_dispo: dict) -> list[dict]:
    """Arma pases entre precio disponible (BCR) y cada futuro disponible."""
    pases = []
    today = date.today()
    for familia, precio_dispo in precios_dispo.items():
        if not precio_dispo:
            continue
        prefixes = GRAIN_FAMILY_MAP.get(familia, [familia])
        futuros = []
        for r in granos:
            sym = r.get("symbol", "")
            if not any(sym.upper().startswith(p.upper()) for p in prefixes):
                continue
            info = parse_symbol(sym)
            if info.is_spread or info.is_option or not info.first_exp:
                continue
            last = r.get("last_price")
            if not last:
                continue
            futuros.append({"symbol": sym, "expiration": info.first_exp, "last_price": last})
        futuros.sort(key=lambda x: x["expiration"])
        for fut in futuros:
            p_fut  = fut["last_price"]
            spread = p_fut - precio_dispo
            d_fut  = _exp_to_date(fut["expiration"])
            days   = (d_fut - today).days
            tna    = None
            if days > 0 and precio_dispo > 0:
                tna = ((p_fut / precio_dispo) - 1) * (365 / days) * 100
            pases.append({
                "family":       familia,
                "family_name":  GRAIN_NAMES.get(familia, familia),
                "underlying":   familia,
                "short_symbol": f"{familia}/DISPO",
                "long_symbol":  fut["symbol"],
                "short_exp":    (today.year, today.month),
                "long_exp":     fut["expiration"],
                "short_label":  "Dispo",
                "long_label":   _exp_label(fut["expiration"]),
                "p_short": precio_dispo,
                "p_long":  p_fut,
                "spread":  spread,
                "days":    days,
                "tna":     tna,
                "expired": False,
            })
    return pases


# ── render de tarjeta de pase ────────────────────────────────────────────────

def _render_pase_card(p: dict) -> str:
    spread     = p["spread"]
    spread_cls = "positive" if spread > 0 else ("negative" if spread < 0 else "")
    spread_sign = "+" if spread >= 0 else ""

    tna = p.get("tna")
    if tna is None:
        tna_str, tna_cls = "—", ""
    else:
        tna_cls  = "positive" if tna > 0 else ("negative" if tna < 0 else "")
        tna_str  = f"{'+'if tna>=0 else ''}{tna:.2f}%"

    # Rendimiento directo: spread / p_short * 100
    p_short = p.get("p_short")
    if p_short and p_short > 0:
        directo     = (spread / p_short) * 100
        directo_cls = "positive" if directo > 0 else ("negative" if directo < 0 else "")
        directo_str = f"{'+'if directo>=0 else ''}{directo:.2f}%"
    else:
        directo_str, directo_cls = "—", ""

    return f"""
    <div class="pase-card">
        <div class="pair">
            {p['family_name']} {p['short_label']}
            <span class="arrow">→</span>
            {p['family_name']} {p['long_label']}
        </div>
        <div class="legs">
            <span class="num">{_fmt_price(p['p_short'])}</span> ({p['short_symbol']})
            &nbsp;·&nbsp;
            <span class="num">{_fmt_price(p['p_long'])}</span> ({p['long_symbol']})
            &nbsp;·&nbsp;
            {p['days']} días
        </div>
        <div class="metrics">
            <div class="metric">
                <span class="label">Diferencia</span>
                <span class="value {spread_cls}">{spread_sign}{spread:,.2f}</span>
            </div>
            <div class="metric">
                <span class="label">Directo</span>
                <span class="value {directo_cls}">{directo_str}</span>
            </div>
            <div class="metric">
                <span class="label">TNA implícita</span>
                <span class="value {tna_cls}">{tna_str}</span>
            </div>
        </div>
    </div>
    """


# ── render de pases monedas (sin cambios) ────────────────────────────────────

def _render_pases(pases: list[dict], cols_per_row: int = 3) -> None:
    st.markdown(
        f'<div class="section-title">🔁 Pases calculados '
        f'<span class="badge">{len(pases)}</span></div>',
        unsafe_allow_html=True,
    )
    if not pases:
        st.markdown(
            '<div class="empty-card">No hay suficientes contratos con precio '
            "para armar pases todavía.</div>",
            unsafe_allow_html=True,
        )
        return

    by_family: dict[str, list[dict]] = defaultdict(list)
    for p in pases:
        by_family[p["family_name"]].append(p)

    for family_name, items in by_family.items():
        st.markdown(f"**{family_name}**")
        for start in range(0, len(items), cols_per_row):
            chunk = items[start:start + cols_per_row]
            cols  = st.columns(cols_per_row)
            for col, p in zip(cols, chunk):
                with col:
                    st.markdown(_render_pase_card(p), unsafe_allow_html=True)


# ── render de pases agropecuarios (nuevo layout) ─────────────────────────────

def _render_pases_agro(
    granos: list[dict],
    precios_dispo: dict[str, float | None],
    cols_per_row: int = 3,
    max_fut_pases: int = 5,
) -> None:
    """Layout por familia:
    - Columna izquierda : Disponible → cada futuro (precio viene de session_state)
    - Columna derecha   : Futuros entre sí, top N por volumen combinado
    """
    pases_fut   = build_pases(granos, consecutive_only=False)
    pases_dispo = _build_pases_disponible(granos, precios_dispo)

    by_family_fut: dict[str, list[dict]] = defaultdict(list)
    for p in pases_fut:
        by_family_fut[p["family"]].append(p)

    by_family_dispo: dict[str, list[dict]] = defaultdict(list)
    for p in pases_dispo:
        by_family_dispo[p["family"]].append(p)

    # Familias presentes en el orden definido
    familias_presentes: list[str] = []
    for fam in FAMILIA_ORDEN:
        if fam in by_family_fut or fam in by_family_dispo:
            familias_presentes.append(fam)
    for fam in set(list(by_family_fut) + list(by_family_dispo)):
        if fam not in familias_presentes:
            familias_presentes.append(fam)

    total = (
        sum(len(v) for v in by_family_fut.values())
        + sum(len(v) for v in by_family_dispo.values())
    )
    st.markdown(
        f'<div class="section-title">🔁 Pases agropecuarios '
        f'<span class="badge">{total}</span></div>',
        unsafe_allow_html=True,
    )

    if not familias_presentes:
        st.markdown(
            '<div class="empty-card">No hay suficientes contratos con precio '
            "para armar pases todavía.</div>",
            unsafe_allow_html=True,
        )
        return

    for fam in familias_presentes:
        fam_name  = GRAIN_NAMES.get(fam, fam)
        fut_items = by_family_fut.get(fam, [])

        # Ordenar por volumen combinado (short + long) descendente, luego tomar top N
        def _vol_key(p):
            rows_by_sym = {r.get("symbol", ""): r for r in granos}
            v_s = (rows_by_sym.get(p["short_symbol"]) or {}).get("trade_volume") or 0
            v_l = (rows_by_sym.get(p["long_symbol"])  or {}).get("trade_volume") or 0
            return v_s + v_l

        fut_items.sort(key=_vol_key, reverse=True)
        fut_items = fut_items[:max_fut_pases]
        # Re-ordenar los seleccionados por vencimiento para mostrarlos en orden
        fut_items.sort(key=lambda p: (p.get("short_exp", (0, 0)), p.get("long_exp", (0, 0))))

        # Precio disponible desde session_state (lo carga el widget de fuera del fragment)
        precio_dispo_fam = st.session_state.get(f"dispo_{fam}", 0.0) or 0.0
        dispo_items = (
            _build_pases_disponible(granos, {fam: precio_dispo_fam})
            if precio_dispo_fam > 0
            else []
        )

        st.markdown(f"### {fam_name}")

        has_dispo = bool(dispo_items)
        has_fut   = bool(fut_items)

        if has_dispo and has_fut:
            col_dispo, col_fut = st.columns(2)
        elif has_dispo:
            col_dispo = st.container()
            col_fut   = None
        elif has_fut:
            col_dispo = None
            col_fut   = st.container()
        else:
            st.markdown(
                '<div class="empty-card">Sin contratos con precio para esta familia.</div>',
                unsafe_allow_html=True,
            )
            continue

        if has_dispo and col_dispo is not None:
            with col_dispo:
                st.markdown("**Disponible → Futuro**")
                for p in dispo_items:
                    st.markdown(_render_pase_card(p), unsafe_allow_html=True)

        if has_fut and col_fut is not None:
            with col_fut:
                st.markdown("**Entre futuros**")
                for p in fut_items:
                    st.markdown(_render_pase_card(p), unsafe_allow_html=True)


# ── helpers varios ───────────────────────────────────────────────────────────

def _weighted_avg(items: list[dict], price_key: str, weight_key: str) -> float | None:
    num = den = 0.0
    for it in items:
        p = it.get(price_key)
        w = it.get(weight_key)
        if p is None or w is None:
            continue
        try:
            p, w = float(p), float(w)
        except (TypeError, ValueError):
            continue
        if w <= 0 or p <= 0:
            continue
        num += p * w
        den += w
    return num / den if den else None


def _render_dolares_financieros(
    mep_rows: list[dict],
    ccl_rows: list[dict],
    bonos_rows: list[dict],
    dlr_spot_row: dict | None = None,
    mayorista_data: dict | None = None,
) -> None:
    bonds_by_symbol = {
        str(r.get("symbol", "")).upper(): r
        for r in bonos_rows
        if r.get("symbol")
    }

    al30  = bonds_by_symbol.get("AL30")
    al30c = bonds_by_symbol.get("AL30C")
    al30d = bonds_by_symbol.get("AL30D")

    def _price(r):
        if r is None:
            return None
        v = r.get("c") or r.get("mark") or r.get("px_bid")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    p_al30  = _price(al30)
    p_al30c = _price(al30c)
    p_al30d = _price(al30d)

    mep = (p_al30 / p_al30d) if (p_al30 and p_al30d and p_al30d > 0) else None
    ccl = (p_al30 / p_al30c) if (p_al30 and p_al30c and p_al30c > 0) else None

    def _pct(r):
        if r is None:
            return None
        v = r.get("pct_change")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    pct_al30  = _pct(al30)
    pct_al30c = _pct(al30c)
    pct_al30d = _pct(al30d)

    mep_pct = (pct_al30 - pct_al30d) if (pct_al30 is not None and pct_al30d is not None) else None
    ccl_pct = (pct_al30 - pct_al30c) if (pct_al30 is not None and pct_al30c is not None) else None

    def _prev(precio, pct):
        if precio is None or pct is None:
            return None
        try:
            return precio / (1 + pct / 100)
        except ZeroDivisionError:
            return None

    mep_prev = _prev(mep, mep_pct)
    ccl_prev = _prev(ccl, ccl_pct)

    brecha_ccl_mep = ((ccl / mep - 1) * 100) if (mep and ccl and mep > 0) else None

    spot = spot_pct = spot_prev = None
    if mayorista_data:
        spot = mayorista_data.get("venta")
        spot_pct = mayorista_data.get("variacion")
        spot_prev = (spot / (1 + spot_pct / 100)) if (spot and spot_pct) else None

    brecha_mep_spot = ((mep / spot - 1) * 100) if (mep and spot and spot > 0) else None

    st.markdown(
        '<div class="section-title">📊 Dólares financieros</div>',
        unsafe_allow_html=True,
    )

    def _fin_card(label, precio, pct, prev, sub):
        price_str   = f"${precio:,.2f}" if precio else "—"
        change_text, change_cls = _fmt_change(pct)
        abs_change  = _fmt_abs_change(precio, prev)
        abs_html    = (
            f'<span class="abs-change {change_cls}">({abs_change})</span>'
            if abs_change else ""
        )
        return f"""
        <div class="metric-card {change_cls}">
            <div class="symbol">{label}</div>
            <div class="price">{price_str}</div>
            <div class="change {change_cls}">{change_text} {abs_html}</div>
            <div class="footer"><span><span class="label">{sub}</span></span></div>
        </div>
        """

    def _brecha_card(label, brecha, sub):
        if brecha is None:
            cls, val = "neutral", "—"
        else:
            cls = "positive" if brecha >= 0 else "negative"
            val = f"{'+' if brecha >= 0 else ''}{brecha:.2f}%"
        return f"""
        <div class="metric-card {cls}" style="padding:10px 14px; margin-bottom:7px;">
            <div class="symbol" style="margin-bottom:3px;">{label}</div>
            <div class="price" style="font-size:1.25rem;">{val}</div>
            <div style="font-size:0.72rem; color:#9ca3af; margin-top:4px;">{sub}</div>
        </div>
        """

    col_mep, col_spot, col_ccl, col_brechas = st.columns(4)

    with col_mep:
        sub_mep = (
            f"AL30 ÷ AL30D · {p_al30:.2f} ÷ {p_al30d:.2f}"
            if (p_al30 and p_al30d) else "AL30 ÷ AL30D · sin datos"
        )
        st.markdown(_fin_card("Dólar MEP", mep, mep_pct, mep_prev, sub_mep), unsafe_allow_html=True)

    with col_spot:
        sub_spot = "dolarapi.com · mayorista" if spot else "mayorista · sin datos"
        st.markdown(_fin_card("Dólar A3500", spot, spot_pct, spot_prev, sub_spot), unsafe_allow_html=True)

    with col_ccl:
        sub_ccl = (
            f"AL30 ÷ AL30C · {p_al30:.2f} ÷ {p_al30c:.2f}"
            if (p_al30 and p_al30c) else "AL30 ÷ AL30C · sin datos"
        )
        st.markdown(_fin_card("Dólar CCL", ccl, ccl_pct, ccl_prev, sub_ccl), unsafe_allow_html=True)

    with col_brechas:
        st.markdown(
            _brecha_card("Brecha CCL / MEP",   brecha_ccl_mep,  "(CCL ÷ MEP) − 1") +
            _brecha_card("Brecha MEP / A3500", brecha_mep_spot, "(MEP ÷ A3500) − 1"),
            unsafe_allow_html=True,
        )


def _render_byma_card(item: dict) -> str:
    symbol     = item.get("ticker") or item.get("ticker_ar") or item.get("symbol") or "—"
    last       = item.get("c") or item.get("mark")
    pct        = item.get("pct_change")
    vol        = item.get("v")
    prev_close = item.get("close")

    if pct is None and last and prev_close:
        try:
            pct = (float(last) - float(prev_close)) / float(prev_close) * 100
        except (TypeError, ValueError, ZeroDivisionError):
            pct = None

    pct_text, pct_cls = _fmt_change(pct)
    abs_change = _fmt_abs_change(last, prev_close)
    abs_html   = (
        f'<span class="abs-change {pct_cls}">({abs_change})</span>'
        if abs_change else ""
    )
    return f"""
    <div class="metric-card {pct_cls}">
        <div class="symbol" title="{symbol}">{symbol}</div>
        <div class="price">{_fmt_price(last)}</div>
        <div class="change {pct_cls}">{pct_text} {abs_html}</div>
        <div class="footer">
            <span><span class="label">Vol.</span> <span class="value">{_fmt_int(vol)}</span></span>
            <span><span class="label">Cierre prev.</span> <span class="value">{_fmt_price(prev_close)}</span></span>
        </div>
    </div>
    """


# Capitalización de mercado en millones de ARS
_CAP_MERC: dict[str, float] = {
    "YPFD": 25270, "GGAL": 10380, "TECO2": 7290, "BMA": 7090, "TGSU2": 6810,
    "PAMP": 6500, "BBAR": 4410, "CEPU": 3270, "TXAR": 3150, "ALUA": 2780,
    "BYMA": 2380, "TGNO4": 1870, "LOMA": 1820, "IRSA": 1740, "TRAN": 1700,
    "EDN": 1670, "BPAT": 1460, "CVH": 1320, "MOLA": 1200, "CRES": 1180,
    "SUPV": 1170, "METR": 1090, "CAPX": 770.45, "VALO": 768.02, "CTIO": 756.28,
    "CGPA2": 719.89, "A3": 716.30, "ECOG": 664.77, "GBAN": 659.22, "HARG": 634.56,
    "PATA": 615, "MOLI": 542, "BHIP": 512.21, "GCLA": 408.42, "LEDE": 342.54,
    "MIRG": 338.38, "COME": 334.81, "DGCU2": 331.86, "CECO2": 322.91,
    "AUSO": 318.18, "INVJ": 317.09, "DGCE": 315.3, "HAVA": 277.16,
    "RICH": 130.41, "GRIM": 122.95, "OEST": 120.48, "BOLT": 109.42,
    "FERR": 98.11, "GAMI": 92.65, "SAMI": 81.98, "RIGO": 69.49,
    "CADO": 67.88, "SEMI": 66.42, "AGRO": 54.84, "IEB": 47.6,
    "INTR": 38.02, "FIPL": 33.6, "CELU": 27.86, "CARC": 25.65,
    "GCDI": 14.51, "GARO": 11.59, "LONG": 9.82, "MORI": 8.34,
    "ROSE": 6.94, "POLL": 1.7,
}

_SECTORES: dict[str, str] = {
    "YPFD": "Energía", "PAMP": "Energía", "TGSU2": "Energía",
    "TGNO4": "Energía", "CEPU": "Energía", "TRAN": "Energía",
    "EDN": "Energía", "CAPX": "Energía", "DGCU2": "Energía",
    "CECO2": "Energía", "HARG": "Energía", "CGPA2": "Energía",
    "ROSE": "Energía",
    "GGAL": "Financiero", "BMA": "Financiero", "BBAR": "Financiero",
    "SUPV": "Financiero", "VALO": "Financiero", "BYMA": "Financiero",
    "BHIP": "Financiero", "GCLA": "Financiero", "BPAT": "Financiero",
    "GBAN": "Financiero", "INVJ": "Financiero", "IEB": "Financiero",
    "INTR": "Financiero",
    "TECO2": "Telecom", "CVH": "Telecom", "CTIO": "Telecom",
    "ALUA": "Materiales", "LOMA": "Materiales", "TXAR": "Materiales",
    "BOLT": "Materiales", "FERR": "Materiales", "GARO": "Materiales",
    "CARC": "Materiales",
    "IRSA": "Real Estate", "MOLA": "Real Estate", "CRES": "Real Estate",
    "GCDI": "Real Estate", "LONG": "Real Estate",
    "MOLI": "Consumo", "LEDE": "Consumo", "COME": "Consumo",
    "RICH": "Consumo", "GRIM": "Consumo", "PATA": "Consumo",
    "AGRO": "Consumo", "CADO": "Consumo", "POLL": "Consumo",
    "SAMI": "Consumo", "RIGO": "Consumo", "MORI": "Consumo",
    "AUSO": "Industrial", "MIRG": "Industrial", "FIPL": "Industrial",
    "GAMI": "Industrial",
    "METR": "Utilities", "ECOG": "Utilities", "OEST": "Utilities",
    "DGCE": "Utilities", "A3": "Utilities",
    "HAVA": "Salud", "CELU": "Salud",
    "SEMI": "Tecnología",
}


def _render_heatmap(acciones: list[dict]) -> None:
    import plotly.graph_objects as go

    rows = {
        str(r.get("symbol", "") or r.get("ticker", "")).upper(): r
        for r in acciones
        if (r.get("symbol") or r.get("ticker")) and r.get("c")
    }

    ids, labels, parents, values, colors, custom = [], [], [], [], [], []

    sectores_cap: dict[str, float] = {}
    for ticker, cap in _CAP_MERC.items():
        if ticker not in rows:
            continue
        sector = _SECTORES.get(ticker, "Otros")
        sectores_cap[sector] = sectores_cap.get(sector, 0) + cap

    for sector, total_cap in sectores_cap.items():
        ids.append(sector)
        labels.append(f"<b>{sector}</b>")
        parents.append("")
        values.append(total_cap)
        colors.append(0.0)
        custom.append(f"<b>{sector}</b>")

    for ticker, cap in _CAP_MERC.items():
        r = rows.get(ticker)
        if r is None:
            continue
        sector = _SECTORES.get(ticker, "Otros")
        precio = float(r.get("c") or 0)
        pct    = float(r.get("pct_change") or 0)
        vol    = int(r.get("v") or 0)
        ids.append(ticker)
        labels.append(f"{ticker}<br>{'+'if pct>=0 else ''}{pct:.2f}%")
        parents.append(sector)
        values.append(cap)
        colors.append(pct)
        custom.append(
            f"<b>{ticker}</b><br>"
            f"Precio: ${precio:,.2f}<br>"
            f"Var: {'+'if pct>=0 else ''}{pct:.2f}%<br>"
            f"Cap. Merc.: ${cap:,.0f}M<br>"
            f"Vol: {vol:,}"
        )

    if not ids:
        st.info("Sin datos de acciones todavía.")
        return

    max_abs = max((abs(c) for c in colors if c != 0.0), default=3)
    max_abs = max(max_abs, 1)

    colorscale = [
        [0.0,  "#9A0000"],
        [0.2,  "#CC0000"],
        [0.38, "#880000"],
        [0.48, "#222222"],
        [0.5,  "#1a1a1a"],
        [0.52, "#003300"],
        [0.62, "#007700"],
        [0.8,  "#00AA00"],
        [1.0,  "#00CC00"],
    ]

    fig = go.Figure(go.Treemap(
        ids=ids,
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        marker=dict(
            colors=colors,
            colorscale=colorscale,
            cmin=-max_abs,
            cmid=0,
            cmax=max_abs,
            showscale=False,
            line=dict(width=1, color="#000000"),
        ),
        customdata=custom,
        hovertemplate="%{customdata}<extra></extra>",
        textfont=dict(size=13, color="white", family="Arial Black, Arial, sans-serif"),
        textposition="middle center",
        pathbar=dict(visible=False),
        tiling=dict(packing="squarify", pad=2),
    ))

    fig.update_layout(
        margin=dict(t=0, l=0, r=0, b=0),
        height=680,
        paper_bgcolor="#000000",
        plot_bgcolor="#000000",
        font=dict(color="white"),
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_byma_panel(title: str, emoji: str, items: list[dict],
                       cols_per_row: int = 4, top_n: int = 60,
                       buscar: str = "") -> None:
    if buscar:
        q = buscar.strip().upper()
        items = [
            it for it in items
            if q in str(it.get("ticker") or it.get("ticker_ar") or it.get("symbol") or "").upper()
        ]

    def _monto(it):
        price = it.get("c") or it.get("mark") or it.get("close") or 0
        vol   = it.get("v") or it.get("v_ars") or 0
        try:
            return float(price) * float(vol)
        except (TypeError, ValueError):
            return 0.0

    items_view = sorted(items, key=_monto, reverse=True)[:top_n]

    st.markdown(
        f'<div class="section-title">{emoji} {title} '
        f'<span class="badge">{len(items_view)} / {len(items)}</span></div>',
        unsafe_allow_html=True,
    )

    if not items_view:
        st.markdown(
            '<div class="empty-card">Sin datos todavía. Esperando respuesta de data912…</div>',
            unsafe_allow_html=True,
        )
        return

    for start in range(0, len(items_view), cols_per_row):
        chunk = items_view[start:start + cols_per_row]
        cols  = st.columns(cols_per_row)
        for col, it in zip(cols, chunk):
            with col:
                st.markdown(_render_byma_card(it), unsafe_allow_html=True)


def _render_group(title: str, emoji: str, rows: list[dict], cols_per_row: int = 4) -> None:
    st.markdown(
        f'<div class="section-title">{emoji} {title} '
        f'<span class="badge">{len(rows)}</span></div>',
        unsafe_allow_html=True,
    )

    if not rows:
        st.markdown(
            '<div class="empty-card">Sin datos para mostrar todavía. '
            "Esperando primeros ticks del mercado…</div>",
            unsafe_allow_html=True,
        )
        return

    for start in range(0, len(rows), cols_per_row):
        chunk = rows[start:start + cols_per_row]
        cols  = st.columns(cols_per_row)
        for col, row in zip(cols, chunk):
            with col:
                st.markdown(_render_card(row), unsafe_allow_html=True)
        for col in cols[len(chunk):]:
            with col:
                st.empty()


TABLE_CSS_RAVA = """
<style>
.rava-wrap{background:#0f1117;border-radius:12px;padding:16px;margin-bottom:20px}
.rava-title{font-size:.85rem;font-weight:700;color:#9ca3af;text-transform:uppercase;
    letter-spacing:.06em;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #1f2937}
.rava-table{width:100%;border-collapse:collapse;font-size:.83rem}
.rava-table th{color:#6b7280;font-weight:600;font-size:.72rem;text-transform:uppercase;
    letter-spacing:.05em;padding:6px 10px;text-align:right;border-bottom:1px solid #1f2937}
.rava-table th:first-child{text-align:left}
.rava-table td{padding:7px 10px;text-align:right;color:#d1d5db;border-bottom:1px solid #1a1f2e;font-weight:500}
.rava-table td:first-child{text-align:left;color:#f9fafb;font-weight:700}
.rava-table tr:last-child td{border-bottom:none}
.rava-table tr:hover td{background:#1a1f2e}
.rv-pos{color:#22c55e!important;font-weight:700!important}
.rv-neg{color:#ef4444!important;font-weight:700!important}
.rv-neu{color:#6b7280!important}
</style>
"""


def _render_tabla_rava(titulo, items, symbol_field="symbol", price_field="c",
                       pct_field="pct_change", vol_field="v", prev_field="close"):
    def _p(v):
        if v is None:
            return '<span class="rv-neu">—</span>'
        try:
            return f"{float(v):,.2f}"
        except Exception:
            return "—"

    def _pct(v):
        if v is None:
            return '<span class="rv-neu">—</span>'
        try:
            f = float(v)
            cls = "rv-pos" if f > 0 else ("rv-neg" if f < 0 else "rv-neu")
            return f'<span class="{cls}">{"+" if f > 0 else ""}{f:.2f}%</span>'
        except Exception:
            return "—"

    def _vol(v):
        if v is None:
            return '<span class="rv-neu">—</span>'
        try:
            vi = int(float(v))
            if vi >= 1_000_000:
                return f"{vi/1_000_000:.1f}M"
            if vi >= 1_000:
                return f"{vi/1_000:.0f}K"
            return str(vi)
        except Exception:
            return "—"

    rows_html = ""
    for it in items:
        sym = it.get(symbol_field) or it.get("ticker") or "—"
        rows_html += (
            f"<tr>"
            f"<td>{sym}</td>"
            f"<td>{_p(it.get(price_field) or it.get('mark'))}</td>"
            f"<td>{_pct(it.get(pct_field))}</td>"
            f"<td>{_vol(it.get(vol_field))}</td>"
            f"<td>{_p(it.get(prev_field))}</td>"
            f"</tr>"
        )
    if not rows_html:
        rows_html = '<tr><td colspan="5" style="color:#6b7280;text-align:center;padding:12px;">Sin datos</td></tr>'

    st.markdown(
        f"""{TABLE_CSS_RAVA}
        <div class="rava-wrap">
            <div class="rava-title">{titulo}</div>
            <table class="rava-table">
                <thead><tr>
                    <th>Especie</th><th>Último</th><th>% Día</th>
                    <th>Volumen</th><th>Cierre ant.</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>""",
        unsafe_allow_html=True,
    )



# ── scraping precios pizarra BCR ─────────────────────────────────────────────

def _fetch_bcr_pizarra() -> dict[str, float]:
    """Scrapea precios de pizarra BCR (U$S/t) desde cac.bcr.com.ar.
    Devuelve {"SOJ": x, "MAI": x, "TRI": x, "GIR": x, "SOR": x} con lo que encuentre.
    """
    import re
    import requests
    from bs4 import BeautifulSoup
    try:
        r = requests.get(
            "https://www.cac.bcr.com.ar/es/precios-de-pizarra",
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        PATRONES = {
            "TRI": r"Trigo[\s\S]{0,120}?US\$\s*([\d.,]+)",
            "MAI": r"Ma[íi]z[\s\S]{0,120}?US\$\s*([\d.,]+)",
            "GIR": r"Girasol[\s\S]{0,120}?US\$\s*(?:\(E\)\s*)?([\d.,]+)",
            "SOJ": r"Soja[\s\S]{0,120}?US\$\s*([\d.,]+)",
            "SOR": r"Sorgo[\s\S]{0,120}?US\$\s*([\d.,]+)",
        }
        result = {}
        for fam, pat in PATRONES.items():
            m = re.search(pat, text)
            if m:
                val = m.group(1).replace(".", "").replace(",", ".")
                try:
                    result[fam] = float(val)
                except ValueError:
                    pass
        return result
    except Exception:
        return {}

# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    st.markdown(CARD_CSS, unsafe_allow_html=True)
    st.title("Matba Rofex — Dashboard en tiempo real")
    st.caption("Dólares y granos · WebSocket pyRofex · persistencia en Supabase")

    mgr = get_manager()

    with st.sidebar:
        st.header("Estado")
        if mgr.error:
            st.error(mgr.error)
        elif mgr.initialized:
            st.success("Conectado a REMARKETS")
        else:
            st.warning("Inicializando...")

        st.metric("Instrumentos detectados", len(mgr.symbols))
        if db.is_connected():
            s = db.stats()
            st.metric("Supabase", f"✓ ON ({s['ok']} ok / {s['errors']} err)")
        else:
            st.metric("Supabase", "✗ OFF")
            err = db.init_error()
            if err:
                st.error(err)

        if mgr.snapshot_total:
            if mgr.snapshot_finished:
                st.success(f"Snapshot ✓ {mgr.snapshot_saved}/{mgr.snapshot_total} filas guardadas")
            else:
                st.progress(
                    mgr.snapshot_done / mgr.snapshot_total if mgr.snapshot_total else 0,
                    text=f"Snapshot REST {mgr.snapshot_done}/{mgr.snapshot_total} (guardados: {mgr.snapshot_saved})",
                )

        if mgr.ws_subscribed:
            st.caption("WebSocket suscripto ✓")
        else:
            st.caption("WebSocket: conectando...")

        if mgr.last_update:
            local = mgr.last_update.astimezone(BA_TZ)
            st.write(f"Último tick: **{local.strftime('%H:%M:%S')}**")

        st.divider()
        st.subheader("APIs externas (data912)")
        if mgr.external_last_update:
            local_ext = mgr.external_last_update.astimezone(BA_TZ)
            st.caption(f"Última actualización: **{local_ext.strftime('%H:%M:%S')}**")
        ext_status = []
        for key in ("MEP", "CCL", "ACCIONES", "BONOS", "CEDEARS"):
            n   = len(mgr.get_external(key))
            err = mgr.external_errors.get(key)
            ext_status.append(f"❌ {key}: {err[:40]}" if err else f"✓ {key}: {n} filas")
        st.caption("\n".join(ext_status))

        st.divider()
        st.subheader("Filtros")
        underlyings_disponibles = sorted(
            {meta.get("underlying", "") for meta in mgr.instrument_meta.values() if meta.get("underlying")}
        )
        underlying_filter = st.multiselect(
            "Subyacente",
            options=underlyings_disponibles,
            default=[],
            help="Vacío = todos",
        )
        ocultar_opciones  = st.checkbox("Ocultar opciones (calls/puts)", value=True)
        ocultar_mayorista = st.checkbox("Ocultar contratos Mayorista", value=True)
        max_pase = st.slider(
            "Pases: máxima distancia (meses)",
            min_value=0, max_value=12, value=1,
            help="0 = sin pases. 1 = solo pares consecutivos. 12 = todos.",
        )
        buscar = st.text_input("Buscar instrumento", placeholder="ej: DLR/AGO o SOJ.ROS")

        st.divider()
        cols_per_row  = st.slider("Tarjetas por fila", 2, 6, 4)
        refresh_secs  = st.slider("Refresco (seg)", 1, 10, 2)

    if mgr.error:
        st.stop()

    # Cargar precios BCR automáticamente al inicio de la sesión
    if "bcr_loaded" not in st.session_state:
        with st.spinner("Cargando precios pizarra BCR..."):
            bcr = _fetch_bcr_pizarra()
        for fam, precio in bcr.items():
            st.session_state[f"dispo_{fam}"] = precio
        st.session_state["bcr_loaded"] = True
        st.session_state["bcr_precios"] = bcr

    bcr_ok = bool(st.session_state.get("bcr_precios"))
    bcr_caption = "✅ Precargado desde cac.bcr.com.ar · editá si querés ajustar" if bcr_ok else "⚠️ No se pudo cargar BCR · ingresá manualmente"

    # Inputs de disponible fuera del fragment
    with st.expander("🌾 Precios disponibles (BCR)", expanded=True):
        st.caption(bcr_caption)
        _c1, _c2, _c3 = st.columns(3)
        with _c1:
            st.number_input("Soja (U$S/t)",  min_value=0.0, value=float(st.session_state.get("dispo_SOJ", 0.0)), step=0.5, format="%.2f", key="dispo_SOJ")
        with _c2:
            st.number_input("Maíz (U$S/t)",  min_value=0.0, value=float(st.session_state.get("dispo_MAI", 0.0)), step=0.5, format="%.2f", key="dispo_MAI")
        with _c3:
            st.number_input("Trigo (U$S/t)", min_value=0.0, value=float(st.session_state.get("dispo_TRI", 0.0)), step=0.5, format="%.2f", key="dispo_TRI")

    placeholder = st.empty()

    @st.fragment(run_every=refresh_secs)
    def render():
        rows = mgr.snapshot()

        if underlying_filter:
            rows = [r for r in rows if r.get("underlying") in underlying_filter]

        rows = [
            r for r in rows
            if keep_for_dashboard(
                r.get("symbol", ""),
                max_spread_gap=max_pase,
                hide_options=ocultar_opciones,
                hide_mayorista=ocultar_mayorista,
            )
        ]

        if buscar:
            q = buscar.strip().upper()
            rows = [r for r in rows if q in r.get("symbol", "").upper()]

        rows.sort(key=lambda r: sort_key(r.get("symbol", ""), r.get("category", "")))

        monedas = [r for r in rows if r.get("category") == "DOLAR"]
        granos  = [
            r for r in rows
            if r.get("category") == "GRANO"
            and (r.get("trade_volume") or 0) > 0
            and "MINI" not in r.get("symbol", "").upper()
        ]

        monedas_puros = [
            r for r in monedas
            if (info := parse_symbol(r.get("symbol", "")))
            and not info.is_spread
            and not info.is_dispo
            and "SPOT" not in r.get("symbol", "").upper()
        ]

        _all_rows    = mgr.snapshot()
        dlr_spot_row = next(
            (r for r in _all_rows if r.get("symbol", "").upper() in ("DLR/SPOT", "DLR/DISPO")),
            None,
        )

        mep_rows      = mgr.get_external("MEP")
        ccl_rows      = mgr.get_external("CCL")
        try:
            import requests as _req
            _r = _req.get("https://dolarapi.com/v1/dolares/mayorista", timeout=5)
            mayorista_data = _r.json() if _r.status_code == 200 else None
        except Exception:
            mayorista_data = None
        acciones = mgr.get_external("ACCIONES")
        bonos    = mgr.get_external("BONOS")
        cedears  = mgr.get_external("CEDEARS")

        pases_monedas = build_pases(monedas_puros, consecutive_only=True)
        pases_granos  = build_pases(granos, consecutive_only=False)

        with placeholder.container():
            now_ba = datetime.now(BA_TZ).strftime("%H:%M:%S")
            st.caption(
                f"Actualizado: {now_ba} (Buenos Aires) · "
                f"Refresco automático cada {refresh_secs}s"
            )

            _render_dolares_financieros(mep_rows, ccl_rows, bonos, dlr_spot_row, mayorista_data)
            st.divider()

            (
                tab_monedas, tab_pmon, tab_granos, tab_pgran,
                tab_acc, tab_bon, tab_ced, tab_heat, tab_tabla,
            ) = st.tabs([
                f"💵 Monedas ({len(monedas_puros)})",
                f"🔁 Pases monedas ({len(pases_monedas)})",
                f"🌾 Granos ({len(granos)})",
                f"🔁 Pases agropecuarios ({len(pases_granos)})",
                f"🏢 Acciones ({len(acciones)})",
                f"🏛️ Bonos ({len(bonos)})",
                f"🍎 CEDEARs ({len(cedears)})",
                "🗺️ Mapa de Calor BYMA",
                "📊 Mi Tabla",
            ])

            with tab_monedas:
                _render_group("Monedas", "💵", monedas_puros, cols_per_row=cols_per_row)

            with tab_pmon:
                _render_pases(pases_monedas, cols_per_row=min(cols_per_row, 3))

            with tab_granos:
                _render_group("Granos", "🌾", granos, cols_per_row=cols_per_row)

            with tab_pgran:
                # Los precios del sidebar son el valor inicial;
                # el input inline dentro de _render_pases_agro permite editarlos por familia
                # precios_dispo no se pasan: _render_pases_agro los lee de session_state
                _render_pases_agro(granos, {}, cols_per_row=cols_per_row)

            with tab_acc:
                _render_byma_panel("Acciones BYMA", "🏢", acciones,
                                   cols_per_row=cols_per_row, buscar=buscar)

            with tab_bon:
                _render_byma_panel("Bonos soberanos", "🏛️", bonos,
                                   cols_per_row=cols_per_row, buscar=buscar)

            with tab_ced:
                _render_byma_panel("CEDEARs", "🍎", cedears,
                                   cols_per_row=cols_per_row, buscar=buscar)

            with tab_heat:
                _render_heatmap(acciones)

            with tab_tabla:
                st.markdown("### 📊 Mi Tabla")
                col1, col2 = st.columns(2)
                with col1:
                    acc_top = sorted(
                        acciones,
                        key=lambda x: float(x.get("c") or 0) * float(x.get("v") or 0),
                        reverse=True,
                    )[:20]
                    _render_tabla_rava("🏢 Acciones — Top 20 por monto", acc_top)
                with col2:
                    bon_top = sorted(
                        bonos,
                        key=lambda x: float(x.get("c") or 0) * float(x.get("v") or 0),
                        reverse=True,
                    )[:20]
                    _render_tabla_rava("🏛️ Bonos soberanos — Top 20", bon_top)

    render()


if __name__ == "__main__":
    main()
