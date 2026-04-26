"""Helpers para parsear símbolos de Matba Rofex.

Distingue futuros, opciones (calls/puts) y pases/spreads (calendar spreads).
También extrae mes/año de vencimiento para ordenar y agrupar."""
from __future__ import annotations

import re
from dataclasses import dataclass

MONTHS_ES: dict[str, int] = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12,
}

OPTION_RE = re.compile(r"\s\d+(?:[.,]\d+)?\s*[CP]\s*$", re.IGNORECASE)
DATE_RE = re.compile(r"([A-Z]{3})(\d{2})")
# Sufijo "M" (con o sin espacio) después de una fecha tipo MES+AA → contrato Mayorista.
# Ej: DLR/ABR26M, DLR/MAY26/JUN26 M, SOJ.ROS/JUL26M
MAYORISTA_RE = re.compile(r"\d{2}\s?M$")

# Símbolos DLR que deben ocultarse del dashboard (contratos duplicados o sin liquidez).
# FEB y MAR con sufijo "A" son una segunda serie del mismo mes → se ocultan.
_DLR_HIDDEN_RE = re.compile(
    r"^DLR/(?:FEB|MAR)\d{2}A$", re.IGNORECASE
)
# DLR/DIC26A → se muestra como DLR/DIC26 (la "A" es la serie estándar de diciembre).
_DLR_DICA_RE = re.compile(r"^(DLR/DIC\d{2})A$", re.IGNORECASE)


def display_symbol(symbol: str) -> str:
    """Devuelve el nombre a mostrar en el dashboard.
    DLR/DIC26A → DLR/DIC26. Resto: sin cambios."""
    m = _DLR_DICA_RE.match(symbol.strip())
    return m.group(1) if m else symbol


GRAIN_FAMILY_ORDER = {
    "SOJ": 1,  # soja
    "MAI": 2,  # maíz
    "TRI": 3,  # trigo
    "GIR": 4,  # girasol
    "SOR": 5,  # sorgo
    "CEB": 6,  # cebada
}


@dataclass
class SymbolInfo:
    raw: str
    is_option: bool
    is_spread: bool
    expirations: list[tuple[int, int]]  # [(year, month), ...]
    spread_gap_months: int | None
    family: str  # underlying root (e.g. "DLR", "SOJ", "MAI")
    is_dispo: bool

    @property
    def first_exp(self) -> tuple[int, int] | None:
        return self.expirations[0] if self.expirations else None


def _family(symbol: str) -> str:
    head = symbol.split("/")[0]
    return head.split(".")[0].upper()


def parse_symbol(symbol: str) -> SymbolInfo:
    raw = symbol.strip()
    is_opt = bool(OPTION_RE.search(raw))
    is_dispo = "DISPO" in raw.upper()
    exps: list[tuple[int, int]] = []
    for mon, yr in DATE_RE.findall(raw):
        m = MONTHS_ES.get(mon.upper())
        if m:
            exps.append((2000 + int(yr), m))
    is_spread = (not is_opt) and len(exps) >= 2
    gap = None
    if is_spread:
        (y1, m1), (y2, m2) = exps[0], exps[1]
        gap = (y2 - y1) * 12 + (m2 - m1)
    return SymbolInfo(
        raw=raw,
        is_option=is_opt,
        is_spread=is_spread,
        expirations=exps,
        spread_gap_months=gap,
        family=_family(raw),
        is_dispo=is_dispo,
    )


def is_mayorista(symbol: str) -> bool:
    """True si el símbolo es un contrato Mayorista (sufijo `M` luego de la fecha)."""
    return bool(MAYORISTA_RE.search(symbol.strip()))


def keep_for_dashboard(
    symbol: str,
    max_spread_gap: int = 1,
    hide_options: bool = True,
    hide_mayorista: bool = True,
) -> bool:
    """True si el símbolo debe aparecer en el dashboard.

    - Excluye opciones si `hide_options=True`.
    - Excluye contratos Mayorista (sufijo `M`) si `hide_mayorista=True`.
    - Excluye pases con gap > `max_spread_gap` meses.
    - Mantiene futuros normales y DISPO.
    """
    # DLR/FEB27A y DLR/MAR27A: contratos duplicados, siempre ocultos
    if _DLR_HIDDEN_RE.match(symbol.strip()):
        return False
    info = parse_symbol(symbol)
    if hide_options and info.is_option:
        return False
    if hide_mayorista and is_mayorista(symbol):
        return False
    if info.is_spread and (info.spread_gap_months or 0) > max_spread_gap:
        return False
    return True


def sort_key(symbol: str, category: str) -> tuple:
    """Clave de orden:
    - Granos: primero por familia (soja → maíz → trigo → resto), después por vencimiento.
    - Dólares (y resto): por vencimiento.
    DISPO siempre primero. Pases (spreads) van después del futuro de su mes.
    """
    info = parse_symbol(symbol)
    if category == "GRANO":
        fam_order = GRAIN_FAMILY_ORDER.get(info.family, 99)
    else:
        fam_order = 0

    if info.is_dispo:
        date_key: tuple[int, int] = (0, 0)
    elif info.first_exp:
        date_key = info.first_exp
    else:
        date_key = (9999, 12)

    spread_key = 1 if info.is_spread else 0
    return (fam_order, date_key, spread_key, symbol)