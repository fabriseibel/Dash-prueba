"""Dashboard Streamlit: precios en tiempo real de dólares y granos
desde Matba Rofex (pyRofex WebSocket) con persistencia en Supabase.

Diseño basado en tarjetas dinámicas que se refrescan automáticamente
a medida que entran nuevos ticks (desde el WebSocket → memoria + Supabase)."""
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
.metric-card.neutral  { border-left: 4px solid #9ca3af; }

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
.metric-card .change.neutral  { color: #6b7280; }

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
    """Devuelve (texto, clase css) para la variación porcentual."""
    if v is None or pd.isna(v):
        return "—", "neutral"
    if v > 0:
        return f"▲ +{v:.2f}%", "positive"
    if v < 0:
        return f"▼ {v:.2f}%", "negative"
    return f"{v:.2f}%", "neutral"


def _fmt_abs_change(last, prev) -> str:
    """Diferencia absoluta entre el último precio y el cierre previo."""
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
    abs_html = (
        f'<span class="abs-change {change_cls}">({abs_change})</span>'
        if abs_change else ""
    )
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
    """Aproxima el vencimiento al último día calendario del mes."""
    y, m = exp
    return date(y, m, monthrange(y, m)[1])


def build_pases(rows: list[dict], consecutive_only: bool = True) -> list[dict]:
    """Arma pases (calendar spreads) entre futuros de la misma familia.

    - `consecutive_only=True` (monedas): solo pares mes-a-mes consecutivos
      (Abr→May, May→Jun, etc.).
    - `consecutive_only=False` (granos): todos los pares posibles,
      pero solo si ambas patas tienen `trade_volume > 0`.
    """
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
        # Para granos: ambas patas deben tener volumen
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
            # Solo pares adyacentes en la lista ordenada
            pairs = list(zip(sorted_items, sorted_items[1:]))
        else:
            # Todos los pares posibles (combinaciones de 2)
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

    # Agrupar por familia para que se vean ordenados
    by_family: dict[str, list[dict]] = defaultdict(list)
    for p in pases:
        by_family[p["family_name"]].append(p)

    for family_name, items in by_family.items():
        st.markdown(f"**{family_name}**")
        for start in range(0, len(items), cols_per_row):
            chunk = items[start:start + cols_per_row]
            cols = st.columns(cols_per_row)
            for col, p in zip(cols, chunk):
                with col:
                    st.markdown(_render_pase_card(p), unsafe_allow_html=True)


def _weighted_avg(items: list[dict], price_key: str, weight_key: str) -> float | None:
    """Promedio ponderado de `price_key` por `weight_key`. Ignora valores inválidos."""
    num = 0.0
    den = 0.0
    for it in items:
        p = it.get(price_key)
        w = it.get(weight_key)
        if p is None or w is None:
            continue
        try:
            p = float(p)
            w = float(w)
        except (TypeError, ValueError):
            continue
        if w <= 0 or p <= 0:
            continue
        num += p * w
        den += w
    if den == 0:
        return None
    return num / den


def _render_dolares_financieros(
    mep_rows: list[dict],
    ccl_rows: list[dict],
    bonos_rows: list[dict],
    spot_from_api: float | None = None,
) -> None:
    """Calcula MEP y CCL desde arg_bonds usando AL30, AL30C y AL30D.
    Muestra también el Dólar A3500 (desde DolarApi en tiempo real) y brechas.

    MEP = AL30 (ARS) ÷ AL30D (USD MEP)
    CCL = AL30 (ARS) ÷ AL30C (USD cable)
    """
    bonds_by_symbol = {
        str(r.get("symbol", "")).upper(): r
        for r in bonos_rows
        if r.get("symbol")
    }

    al30  = bonds_by_symbol.get("AL30")
    al30c = bonds_by_symbol.get("AL30C")
    al30d = bonds_by_symbol.get("AL30D")

    def _price(r: dict | None) -> float | None:
        if r is None:
            return None
        v = r.get("c") or r.get("mark") or r.get("px_bid")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    p_al30  = _price(al30)
    p_al30c = _price(al30c)
    p_al30d = _price(al30d)

    mep = None
    if p_al30 and p_al30d and p_al30d > 0:
        mep = p_al30 / p_al30d

    ccl = None
    if p_al30 and p_al30c and p_al30c > 0:
        ccl = p_al30 / p_al30c

    def _pct(r: dict | None) -> float | None:
        if r is None:
            return None
        v = r.get("pct_change")
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    pct_al30  = _pct(al30)
    pct_al30c = _pct(al30c)
    pct_al30d = _pct(al30d)

    mep_pct = (pct_al30 - pct_al30d) if (pct_al30 is not None and pct_al30d is not None) else None
    ccl_pct = (pct_al30 - pct_al30c) if (pct_al30 is not None and pct_al30c is not None) else None

    def _prev(precio: float | None, pct: float | None) -> float | None:
        if precio is None or pct is None:
            return None
        try:
            return precio / (1 + pct / 100)
        except ZeroDivisionError:
            return None

    mep_prev = _prev(mep, mep_pct)
    ccl_prev = _prev(ccl, ccl_pct)

    brecha_ccl_mep = None
    if mep and ccl and mep > 0:
        brecha_ccl_mep = (ccl / mep - 1) * 100

    # Usamos directamente el spot traído de DolarApi
    spot = spot_from_api
    spot_pct = 0.0  # DolarApi provee el intradiario neto, seteamos neutro o podés calcular variación si guardás el cierre
    spot_prev = spot

    # Brecha MEP / A3500
    brecha_mep_spot = None
    if mep and spot and spot > 0:
        brecha_mep_spot = (mep / spot - 1) * 100

    st.markdown(
        '<div class="section-title">📊 Dólares financieros</div>',
        unsafe_allow_html=True,
    )

    def _fin_card(label: str, precio: float | None, pct: float | None,
                  prev: float | None, sub: str) -> str:
        price_str = f"${precio:,.2f}" if precio else "—"
        change_text, change_cls = _fmt_change(pct)
        abs_change = _fmt_abs_change(precio, prev)
        abs_html = (
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

    def _brecha_card(label: str, brecha: float | None, sub: str) -> str:
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
        sub_mep = f"AL30 ÷ AL30D · {p_al30:.2f} ÷ {p_al30d:.2f}" if (p_al30 and p_al30d) else "AL30 ÷ AL30D · sin datos"
        st.markdown(_fin_card("Dólar MEP", mep, mep_pct, mep_prev, sub_mep), unsafe_allow_html=True)

    with col_spot:
        sub_spot = "Mayorista · DolarApi tiempo real" if spot else "DolarApi · sin datos"
        st.markdown(_fin_card("Dólar A3500", spot, spot_pct, spot_prev, sub_spot), unsafe_allow_html=True)

    with col_ccl:
        sub_ccl = f"AL30 ÷ AL30C · {p_al30:.2f} ÷ {p_al30c:.2f}" if (p_al30 and p_al30c) else "AL30 ÷ AL30C · sin datos"
        st.markdown(_fin_card("Dólar CCL", ccl, ccl_pct, ccl_prev, sub_ccl), unsafe_allow_html=True)

    with col_brechas:
        st.markdown(
            _brecha_card("Brecha CCL / MEP", brecha_ccl_mep, "(CCL ÷ MEP) − 1") +
            _brecha_card("Brecha MEP / A3500", brecha_mep_spot, "(MEP ÷ A3500) − 1"),
            unsafe_allow_html=True,
        )


def _render_byma_card(item: dict) -> str:
    symbol = item.get("ticker") or item.get("ticker_ar") or item.get("symbol") or "—"
    last = item.get("c") or item.get("mark")
    pct = item.get("pct_change")
    vol = item.get("v")
    prev_close = item.get("close")

    if pct is None and last and prev_close:
        try:
            pct = (float(last) - float(prev_close)) / float(prev_close) * 100
        except (TypeError, ValueError, ZeroDivisionError):
            pct = None

    pct_text, pct_cls = _fmt_change(pct)
    abs_change = _fmt_abs_change(last, prev_close)
    abs_html = (
        f'<span class="abs-change {pct_cls}">({abs_change})</span>'
        if abs_change else ""
    )
    card_cls = pct_cls

    return f"""
    <div class="metric-card {card_cls}">
        <div class="symbol" title="{symbol}">{symbol}</div>
        <div class="price">{_fmt_price(last)}</div>
        <div class="change {pct_cls}">{pct_text} {abs_html}</div>
        <div class="footer">
            <span><span class="label">Vol.</span> <span class="value">{_fmt_int(vol)}</span></span>
            <span><span class="label">Cierre prev.</span> <span class="value">{_fmt_price(prev_close)}</span></span>
        </div>
    </div>
    """


_CAP_MERC: dict
