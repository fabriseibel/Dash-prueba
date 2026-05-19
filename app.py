"""Dashboard Streamlit: precios en tiempo real de dólares y granos
desde Matba Rofex (pyRofex WebSocket) con persistencia en Supabase."""
from __future__ import annotations

import time
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime

import pandas as pd
import pytz
import streamlit as st

import db
from rofex_manager import RofexManager
from symbol_utils import keep_for_dashboard, display_symbol, parse_symbol, sort_key, obtener_dolar_mayorista_realtime

st.set_page_config(
    page_title="Matba Rofex Dashboard",
    page_icon="📈",
    layout="wide",
)

BA_TZ = pytz.timezone("America/Argentina/Buenos_Aires")

POSITIVE_COLOR = "#16a34a"
NEGATIVE_COLOR = "#dc2626"
NEUTRAL_COLOR = "#6b7280"

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


def _render_card(row: dict) -> str:
    symbol = display_symbol(row.get("symbol", "—"))
    last = row.get("last_price")
    change = row.get("change_pct")
    volume = row.get("trade_volume")
    prev_close = row.get("prev_close")

    change_text, change_cls = _fmt_change(change)
    abs_change = _fmt_abs_change(last, prev_close)
    abs_html = f'<span class="abs-change {change_cls}">({abs_change})</span>' if abs_change else ""
    card_cls = change_cls

    return f"""
    <div class="metric-card {card_cls}">
        <div class="symbol" title="{symbol}">{symbol}</div>
        <div class="price">{_fmt_price(last)}</div>
        <div class="change {change_cls}">{change_text} {abs_html}</div>
        <div class="footer">
            <span><span class="label">Vol.</span> <span class="value">{_fmt_int(volume)}</span></span>
            <span><span class="label">Cierre prev.</span> <span class="value">{_fmt_price(prev_close)}</span></span>
        </div>
    </div>
    """


def _exp_label(exp: tuple[int, int]) -> str:
    y, m = exp
    return f"{MONTHS_SHORT.get(m, '?')}{str(y)[-2:]}"


def _exp_to_date(exp: tuple[int, int]) -> date:
    y, m = exp
    return date(y, m, monthrange(y, m)[1])


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

        if consecutive_only:
            pairs = list(zip(sorted_items, sorted_items[1:]))
        else:
            from itertools import combinations
            pairs = list(combinations(sorted_items, 2))

        for short, long in pairs:
            p_s = short["last_price"]
            p_l = long["last_price"]
            if not p_s or not p_l:
                continue
            spread = p_l - p_s
            d_short = _exp_to_date(short["expiration"])
            d_long = _exp_to_date(long["expiration"])
            days = (d_long - d_short).days
            tna = None
            if days > 0 and p_s > 0:
                tna = ((p_l / p_s) - 1) * (365 / days) * 100
            pases.append({
                "family": family,
                "family_name": GRAIN_NAMES.get(family, family),
                "underlying": short.get("underlying", family),
                "short_symbol": short["symbol"],
                "long_symbol": long["symbol"],
                "short_label": _exp_label(short["expiration"]),
                "long_label": _exp_label(long["expiration"]),
                "p_short": p_s,
                "p_long": p_l,
                "spread": spread,
                "days": days,
                "tna": tna,
                "expired": d_short < today,
            })
    pases.sort(key=lambda p: (p["family_name"], p["short_symbol"]))
    return pases


def _render_pase_card(p: dict) -> str:
    spread = p["spread"]
    spread_cls = "positive" if spread > 0 else ("negative" if spread < 0 else "")
    spread_sign = "+" if spread >= 0 else ""

    tna = p.get("tna")
    if tna is None:
        tna_str = "—"
        tna_cls = ""
    else:
        tna_cls = "positive" if tna > 0 else ("negative" if tna < 0 else "")
        tna_sign = "+" if tna >= 0 else ""
        tna_str = f"{tna_sign}{tna:.2f}%"

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
                <span class="label">TNA implícita</span>
                <span class="value {tna_cls}">{tna_str}</span>
            </div>
        </div>
    </div>
    """


def _render_pases(pases: list[dict], cols_per_row: int = 3) -> None:
    st.markdown(f'<div class="section-title">🔁 Pases calculados <span class="badge">{len(pases)}</span></div>', unsafe_allow_html=True)
    if not pases:
        st.markdown('<div class="empty-card">No hay suficientes contratos con precio para armar pases todavía.</div>', unsafe_allow_html=True)
        return

    by_family: dict[str, list[dict]] = defaultdict(list)
    for p in pases:
        by_family[p["family_name"]].append(p)

    for family_name, items in by_family.items():
        st.markdown(f"**{family_name}**")
        for start in range(0, len(items), cols_per_row):
            chunk = items
