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


@st.cache_data(ttl=60, show_spinner=False)
def obtener_spot_api_cached() -> float | None:
    val = obtener_dolar_mayorista_realtime()
    if val and val > 0:
        return val
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
        return f"▲ +{v:.2
