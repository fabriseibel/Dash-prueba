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
import requests
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
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 18px;
    color: #6b7280;
    text-align: center;
    font-size: 0.9rem;
    background: #fafafa;
    margin-bottom: 15px;
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

GRAIN_FAMILY_ORDER = {
    "SOJ": 1,
    "MAI": 2,
    "TRI": 3,
}

GRAIN_FAMILY_MAP = {
    "SOJ": ["SOJ.ROS", "SOJ"],
    "MAI": ["MAI.ROS", "MAI"],
    "TRI": ["TRI.ROS", "TRI"],
}


@st.cache_resource(show_spinner="Conectando a Matba Rofex (ROFEX)...")
def get_manager() -> RofexManager:
    mgr = RofexManager.get()
    mgr.initialize()
    return mgr


@st.cache_data(ttl=30, show_spinner=False)
def obtener_spot_api_cached() -> float | None:
    url = "https://dolarapi.com/v1/dolares/mayorista"
    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            val = float(data.get("venta", 0))
            if val > 1397.00:
                return val
    except Exception:
        pass
    return None


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


# --- COMPONENTE VISUAL 1: DEFINICIÓN DE TARJETAS FINANCIERAS ---
def _fin_card(label: str, precio: float | None, pct: float | None, prev: float | None, sub: str) -> str:
    price_str = f"${precio:,.2f}" if precio else "—"
    change_text, change_cls = _fmt_change(pct)
    abs_change = _fmt_abs_change(precio, prev)
    abs_html = f'<span class="abs-change {change_cls}">({abs_change})</span>' if abs_change else ""
    return f"""
    <div class="metric-card {change_cls}">
        <div class="symbol">{label}</div>
        <div class="price">{price_str}</div>
        <div class="change {change_cls}">{change_text} {abs_html}</div>
        <div class="footer"><span><span class="label">{sub}</span></span></div>
    </div>
    """


# --- COMPONENTE VISUAL 2: DEFINICIÓN DE BRECHAS ---
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
            
            directo = (spread / p_s) * 100 if p_s > 0 else 0.0
            tna = directo * (365 / days) if days > 0 else None

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
                "directo": directo,
                "tna": tna,
                "expired": d_short < date.today(),
            })
            
    pases.sort(key=lambda p: (
        GRAIN_FAMILY_ORDER.get(p["family"], 99),
        parse_symbol(p["short_symbol"]).first_exp or (0, 0),
        parse_symbol(p["long_symbol"]).first_exp or (0, 0)
    ))
    return pases


def _build_pases_disponible(granos: list[dict], precios_dispo: dict) -> list[dict]:
    pases = []
    today = date.today()
    for familia, precio_dispo in precios_dispo.items():
        if not precio_dispo or precio_dispo <= 0: 
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
            p_fut = fut["last_price"]
            spread = p_fut - precio_dispo
            d_fut = _exp_to_date(fut["expiration"])
            days = (d_fut - today).days
            
            directo = (spread / precio_dispo) * 100 if precio_dispo > 0 else 0.0
            tna = directo * (365 / days) if days > 0 else None
            
            pases.append({
                "family": familia,
                "family_name": GRAIN_NAMES.get(familia, familia),
                "underlying": familia,
                "short_symbol": f"{familia} DISPO",
                "long_symbol": fut["symbol"],
                "short_label": "DISPONIBLE",
                "long_label": _exp_label(fut["expiration"]),
                "p_short": precio_dispo,
                "p_long": p_fut,
                "spread": spread,
                "days": days,
                "directo": directo,
                "tna": tna,
                "expired": False,
            })
    return pases


def _render_pases_agro_grid(pases_dispo: list[dict], pases_futuros: list[dict]) -> None:
    for fam_code in ["SOJ", "MAI", "TRI"]:
        fam_name = GRAIN_NAMES.get(fam_code, fam_code)
        f_dispo = [p for p in pases_dispo if p["family"] == fam_code]
        f_futuros = [p for p in pases_futuros if p["family"] == fam_code]
        
        if not f_dispo and not f_futuros:
            continue
            
        st.markdown(f"### **{fam_name.upper()}**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"**Pases con Disponible**")
            if f_dispo:
                for p in f_dispo: st.markdown(_render_card_style_pase(p), unsafe_allow_html=True)
            else:
                st.markdown('<div class="empty-card">Completá el recuadro blanco de Disponible en la barra lateral.</div>', unsafe_allow_html=True)
                
        with col2:
            st.markdown("**Spreads Futuros (Tramo Corto)**")
            if f_futuros:
                mitad = (len(f_futuros) + 1) // 2
                for p in f_futuros[:mitad]: st.markdown(_render_card_style_pase(p), unsafe_allow_html=True)
            else:
                st.markdown('<div class="empty-card">Sin spreads disponibles.</div>', unsafe_allow_html=True)
                
        with col3:
            st.markdown("**Spreads Futuros (Tramo Largo)**")
            if f_futuros and len(f_futuros) > 1:
                mitad = (len(f_futuros) + 1) // 2
                for p in f_futuros[mitad:]: st.markdown(_render_card_style_pase(p), unsafe_allow_html=True)
            else:
                st.markdown('<div class="empty-card">—</div>', unsafe_allow_html=True)
        st.divider()


def _render_card_style_pase(p: dict) -> str:
    spread = p["spread"]
    spread_cls = "positive" if spread > 0 else ("negative" if spread < 0 else "")
    spread_sign = "+" if spread >= 0 else ""
    directo = p.get("directo", 0.0)
    directo_sign = "+" if directo >= 0 else ""
    tna = p.get("tna")
    tna_str = f"{'+' if tna >= 0 else ''}{tna:.2f}%" if tna is not None else "—"
    tna_cls = "positive" if (tna or 0) > 0 else ("negative" if (tna or 0) < 0 else "")

    return f"""
    <div class="pase-card">
        <div class="pair" style="font-size:0.88rem;">
            <b>{p['short_label']}</b> <span class="arrow">→</span> <b>{p['long_label']}</b>
        </div>
        <div class="legs" style="font-size:0.75rem; color:#4b5563; margin-bottom:5px;">
            {_fmt_price(p['p_short'])} <span style="color:#9ca3af;">·</span> {_fmt_price(p['p_long'])} <span style="color:#9ca3af;">·</span> {p['days']} días
        </div>
        <div class="metrics" style="padding-top:4px;">
            <div class="metric"><span class="label">Diferencia</span><span class="value {spread_cls}" style="font-size:0.9rem;">{spread_sign}{spread:,.2f}</span></div>
            <div class="metric"><span class="label">Directo</span><span class="value {spread_cls}" style="font-size:0.9rem;">{directo_sign}{directo:.2f}%</span></div>
            <div class="metric"><span class="label">TNA Impl.</span><span class="value {tna_cls}" style="font-size:0.9rem;">{tna_str}</span></div>
        </div>
    </div>
    """


def _render_pases(pases: list[dict], cols_per_row: int = 3) -> None:
    if not pases:
        st.markdown('<div class="empty-card">No hay suficientes contratos para armar pases.</div>', unsafe_allow_html=True)
        return
    by_family = defaultdict(list)
    for p in pases:
        by_family[p["family_name"]].append(p)
    for family_name, items in by_family.items():
        st.markdown(f"**{family_name}**")
        for start in range(0, len(items), cols_per_row):
            chunk = items[start:start + cols_per_row]
            cols = st.columns(cols_per_row)
            for col, p in zip(cols, chunk):
                with col: st.markdown(_render_pase_card(p), unsafe_allow_html=True)


def _render_dolares_financieros(
    mep_rows: list[dict],
    ccl_rows: list[dict],
    bonos_rows: list[dict],
    dlr_spot_row: dict | None = None,
    spot_from_api: float | None = None,
) -> None:
    bonds_by_symbol = {str(r.get("symbol", "")).upper(): r for r in bonos_rows if r.get("symbol")}
    al30, al30c, al30d = bonds_by_symbol.get("AL30"), bonds_by_symbol.get("AL30C"), bonds_by_symbol.get("AL30D")

    def _price(r: dict | None) -> float | None:
        if r is None: return None
        v = r.get("c") or r.get("mark") or r.get("px_bid")
        try: return float(v) if v is not None else None
        except: return None

    p_al30, p_al30c, p_al30d = _price(al30), _price(al30c), _price(al30d)
    mep = p_al30 / p_al30d if p_al30 and p_al30d and p_al30d > 0 else None
    ccl = p_al30 / p_al30c if p_al30 and p_al30c and p_al30c > 0 else None

    def _pct(r: dict | None) -> float | None:
        if r is None: return None
        try: return float(r.get("pct_change")) if r.get("pct_change") is not None else None
        except: return None

    pct_al30, pct_al30c, pct_al30d = _pct(al30), _pct(al30c), _pct(al30d)
    mep_pct = (pct_al30 - pct_al30d) if (pct_al30 is not None and pct_al30d is not None) else None
    ccl_pct = (pct_al30 - pct_al30c) if (pct_al30 is not None and pct_al30c is not None) else None

    def _prev(precio: float | None, pct: float | None) -> float | None:
        if precio is None or pct is None: return None
        try: return precio / (1 + pct / 100)
        except: return None

    mep_prev, ccl_prev = _prev(mep, mep_pct), _prev(ccl, ccl_pct)
    brecha_ccl_mep = (ccl / mep - 1) * 100 if mep and ccl and mep > 0 else None

    spot = spot_from_api
    spot_pct = 0.0
    spot_prev = spot

    if spot is None and dlr_spot_row:
        spot = (dlr_spot_row.get("last_price") or dlr_spot_row.get("offer") or dlr_spot_row.get("bid") or dlr_spot_row.get("prev_close"))
        spot_prev = dlr_spot_row.get("prev_close") or dlr_spot_row.get("closing_price")
        if spot and spot_prev:
            try: spot_pct = (spot - spot_prev) / spot_prev * 100
            except: pass

    brecha_mep_spot = (mep / spot - 1) * 100 if mep and spot and spot > 0 else None

    st.markdown('<div class="section-title">📊 Dólares financieros</div>', unsafe_allow_html=True)

    # Formateo seguro para no reventar las llaves de Streamlit
    sub_mep_txt = f"AL30 ÷ AL30D · {p_al30:.2f} ÷ {p_al30d:.2f}" if (p_al30 and p_al30d) else "AL30 ÷ AL30D · sin datos"
    sub_ccl_txt = f"AL30 ÷ AL30C · {p_al30:.2f} ÷ {p_al30c:.2f}" if (p_al30 and p_al30c) else "AL30 ÷ AL30C · sin datos"

    col_mep, col_spot, col_ccl, col_brechas = st.columns(4)
    with col_mep:
        st.markdown(_fin_card("Dólar MEP", mep, mep_pct, mep_prev, sub_mep_txt), unsafe_allow_html=True)
    with col_spot:
        st.markdown(_fin_card("Dólar A3500", spot, spot_pct, spot_prev, "Mayorista · Exacto Real-Time" if spot_from_api else "DLR/SPOT · Fallback Rofex"), unsafe_allow_html=True)
    with col_ccl:
        st.markdown(_fin_card("Dólar CCL", ccl, ccl_pct, ccl_prev, sub_ccl_txt), unsafe_allow_html=True)
    with col_brechas:
        st.markdown(_brecha_card("Brecha CCL / MEP", brecha_ccl_mep, "(CCL ÷ MEP) − 1") + _brecha_card("Brecha MEP / A3500", brecha_mep_spot, "(MEP ÷ A3500) − 1"), unsafe_allow_html=True)


def _render_byma_panel(title: str, emoji: str, items: list[dict],
                       cols_per_row: int = 4, top_n: int = 60,
                       buscar: str = "") -> None:
    if buscar:
        items = [it for it in items if buscar.strip().upper() in str(it.get("ticker") or it.get("ticker_ar") or it.get("symbol") or "").upper()]

    def _monto(it: dict) -> float:
        price = it.get("c") or it.get("mark") or it.get("close") or 0
        vol = it.get("v") or it.get("v_ars") or 0
        try: return float(price) * float(vol)
        except: return 0.0

    items_sorted = sorted(items, key=_monto, reverse=True)
    items_view = items_sorted[:top_n]

    st.markdown(f'<div class="section-title">{emoji} {title} <span class="badge">{len(items_view)} / {len(items)}</span></div>', unsafe_allow_html=True)

    if not items_view:
        st.markdown('<div class="empty-card">Sin datos todavía. Esperando respuesta de data912…</div>', unsafe_allow_html=True)
        return

    for start in range(0, len(items_view), cols_per_row):
        chunk = items_view[start:start + cols_per_row]
        cols = st.columns(cols_per_row)
        for col, it in zip(cols, chunk):
            with col: st.markdown(_render_byma_card(it), unsafe_allow_html=True)


def main() -> None:
    st.markdown(CARD_CSS, unsafe_allow_html=True)
    st.title("Matba Rofex — Dashboard en tiempo real")
    st.caption("Dólares y granos · WebSocket pyRofex · persistencia en Supabase")

    mgr = get_manager()

    with st.sidebar:
        st.header("Estado")
        if mgr.error: st.error(mgr.error)
        elif mgr.initialized:
            if hasattr(mgr, 'ws_subscribed_veta'):
                st.success(f"✓ Veta (DLR): {len(mgr.symbols_veta)} contratos") if mgr.ws_subscribed_veta else st.warning(f"⏳ Veta: conectando...")
                eco_syms = getattr(mgr, 'symbols_eco', [])
                st.success(f"✓ Eco (Granos): {len(eco_syms)} contratos") if getattr(mgr, 'ws_subscribed_remarkets', False) else st.warning(f"⏳ Eco: conectando...")
            else: st.success("Conectado a REMARKETS")
        else: st.warning("Inicializando...")

        st.metric("Instrumentos detectados", len(mgr.symbols))
        if db.is_connected(): st.metric("Supabase", f"✓ ON ({db.stats()['ok']} ok)")
        else: st.metric("Supabase", "✗ OFF")

        if mgr.last_update: st.write(f"Último tick: **{mgr.last_update.astimezone(BA_TZ).strftime('%H:%M:%S')}**")

        st.divider()
        st.subheader("APIs externas (data912 / DolarApi)")
        ext_status = []
        for key in ("MEP", "CCL", "ACCIONES", "BONOS", "CEDEARS"):
            n = len(mgr.get_external(key))
            err = mgr.external_errors.get(key)
            ext_status.append(f"❌ {key}: {err[:40]}" if err else f"✓ {key}: {n} filas")
        
        spot_api = obtener_spot_api_cached()
        ext_status.append(f"✓ DolarApi: ${spot_api:,.2f} (Cached)" if spot_api else "⚠️ DolarApi: Sin datos intradiarios nuevos.")
        st.caption("\n".join(ext_status))

        st.divider()
        st.subheader("🌾 Precios disponibles (BCR)")
        precio_soja = st.number_input("Soja disponible (U$S/t)", min_value=0.0, value=0.0, step=0.5, format="%.2f")
        precio_maiz = st.number_input("Maíz disponible (U$S/t)", min_value=0.0, value=0.0, step=0.5, format="%.2f")
        precio_trigo = st.number_input("Trigo disponible (U$S/t)", min_value=0.0, value=0.0, step=0.5, format="%.2f")

        st.divider()
        st.subheader("Filtros")
        underlyings_disponibles = sorted({meta.get("underlying", "") for meta in mgr.instrument_meta.values() if meta.get("underlying")})
        underlying_filter = st.multiselect("Subyacente", options=underlyings_disponibles, default=[])
        ocultar_opciones = st.checkbox("Ocultar opciones (calls/puts)", value=True)
        ocultar_mayorista = st.checkbox("Ocultar contratos Mayorista", value=True)
        max_pase = st.slider("Pases: máxima distancia (meses)", min_value=0, max_value=12, value=1)
        buscar = st.text_input("Buscar instrumento", placeholder="ej: DLR/AGO")

        cols_per_row = st.slider("Tarjetas por fila", 2, 6, 4)
        refresh_secs = st.slider("Refresco (seg)", 1, 10, 2)

    placeholder = st.empty()

    @st.fragment(run_every=refresh_secs)
    def render():
        spot_value = obtener_spot_api_cached()
        rows = mgr.snapshot()

        if underlying_filter: rows = [r for r in rows if r.get("underlying") in underlying_filter]
        rows = [r for r in rows if keep_for_dashboard(r.get("symbol", ""), max_spread_gap=max_pase, hide_options=ocultar_opciones, hide_mayorista=ocultar_mayorista)]
        if buscar: rows = [r for r in rows if buscar.strip().upper() in r.get("symbol", "").upper()]

        rows.sort(key=lambda r: sort_key(r.get("symbol", ""), r.get("category", "")))
        monedas = [r for r in rows if r.get("category") == "DOLAR"]
        granos = [r for r in rows if r.get("category") == "GRANO"]

        monedas_puros = [r for r in monedas if (info := parse_symbol(r.get("symbol", ""))) and not info.is_spread and not info.is_dispo and "SPOT" not in r.get("symbol", "").upper()]
        
        _all_rows = mgr.snapshot()
        dlr_spot_row = next((r for r in _all_rows if r.get("symbol", "").upper() in ("DLR/SPOT", "DLR/DISPO")), None)

        mep_rows, ccl_rows = mgr.get_external("MEP"), mgr.get_external("CCL")
        acciones, bonos, cedears = mgr.get_external("ACCIONES"), mgr.get_external("BONOS"), mgr.get_external("CEDEARS")

        with placeholder.container():
            st.caption(f"Actualizado: {datetime.now(BA_TZ).strftime('%H:%M:%S')} · Refresco cada {refresh_secs}s")
            _render_dolares_financieros(mep_rows, ccl_rows, bonos, dlr_spot_row, spot_value)
            st.divider()

            pases_monedas = build_pases(monedas_puros, consecutive_only=True)
            pases_granos_futuros = build_pases(granos, consecutive_only=False)
            
            precios_dispo = {
                "SOJ": precio_soja if precio_soja > 0 else None,
                "MAI": precio_maiz if precio_maiz > 0 else None,
                "TRI": precio_trigo if precio_trigo > 0 else None,
            }
            pases_dispo = _build_pases_disponible(granos, precios_dispo)

            (tab_monedas, tab_pmon, tab_pgran, tab_granos, tab_acc, tab_bon, tab_ced, tab_heat, tab_tabla) = st.tabs([
                f"💵 Monedas ({len(monedas_puros)})", f"🔁 Pases monedas ({len(pases_monedas)})",
                f"📊 Pases agro ({len(pases_dispo) + len(pases_granos_futuros)})", f"🌾 Granos ({len(granos)})",
                f"🏢 Acciones ({len(acciones)})", f"🏛️ Bonos ({len(bonos)})", f"🍎 CEDEARs ({len(cedears)})",
                "🗺️ Mapa de Calor BYMA", "📊 Mi Tabla"
            ])

            with tab_monedas: _render_group("Monedas", "💵", monedas_puros, cols_per_row=cols_per_row)
            with tab_pmon: _render_pases(pases_monedas, cols_per_row=min(cols_per_row, 3))
            with tab_pgran: _render_pases_agro_grid(pases_dispo, pases_granos_futuros)
            with tab_granos: _render_group("Granos", "🌾", granos, cols_per_row=cols_per_row)
            with tab_acc: _render_byma_panel("Acciones BYMA", "🏢", acciones, cols_per_row=cols_per_row, buscar=buscar)
            with tab_bon: _render_byma_panel("Bonos soberanos", "🏛️", bonos, cols_per_row=cols_per_row, buscar=buscar)
            with tab_ced: _render_byma_panel("CEDEARs", "🍎", cedears, cols_per_row=cols_per_row, buscar=buscar)
            with tab_heat: _render_heatmap(acciones)
            with tab_tabla:
                col1, col2 = st.columns(2)
                with col1: _render_tabla_rava("🏢 Acciones — Top 20", sorted(acciones, key=lambda x: float(x.get("c") or 0)*float(x.get("v") or 0), reverse=True)[:20])
                with col2: _render_tabla_rava("🏛️ Bonos soberanos — Top 20", sorted(bonos, key=lambda x: float(x.get("c") or 0)*float(x.get("v") or 0), reverse=True)[:20])


# --- COMPONENTE VISUAL EXTRA: DEFINICIÓN TARJETA INDIVIDUAL BYMA ---
def _render_byma_card(item: dict) -> str:
    symbol = item.get("ticker") or item.get("ticker_ar") or item.get("symbol") or "—"
    last = item.get("c") or item.get("mark")
    pct = item.get("pct_change")
    vol = item.get("v")
    prev_close = item.get("close")
    if pct is None and last and prev_close:
        try: pct = (float(last) - float(prev_close)) / float(prev_close) * 100
        except: pct = None
    pct_text, pct_cls = _fmt_change(pct)
    abs_change = _fmt_abs_change(last, prev_close)
    abs_html = f'<span class="abs-change {pct_cls}">({abs_change})</span>' if abs_change else ""
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

if __name__ == "__main__":
    main()
