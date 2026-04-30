"""Singleton manager para pyRofex con doble conexión:
- VETA (broker real): DLR/ → datos en tiempo real
- REMARKETS (demo): granos (SOJ, MAI, TRI, etc.) → settlement/cierre

Mantiene estado en memoria para que Streamlit lea.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import pyRofex
import requests

import db


# ─── APIs externas (data912) ────────────────────────────────────────────────
EXTERNAL_API_URLS = {
    "MEP":      "https://data912.com/live/mep",
    "CCL":      "https://data912.com/live/ccl",
    "ACCIONES": "https://data912.com/live/arg_stocks",
    "BONOS":    "https://data912.com/live/arg_bonds",
    "CEDEARS":  "https://data912.com/live/arg_cedears",
}
EXTERNAL_REFRESH_SECS = 5

# ─── Configuración Veta ──────────────────────────────────────────────────────
VETA_BASE_URL = "https://api.veta.xoms.com.ar"
VETA_WS_URL   = "wss://api.veta.xoms.com.ar"

SNAPSHOT_SLEEP_VETA      = 0.1
SNAPSHOT_SLEEP_REMARKETS = 0.1

# ─── Clasificación ───────────────────────────────────────────────────────────
DOLLAR_PREFIXES = ("DLR/",)
GRAIN_PREFIXES  = ("SOJ.", "MAI.", "TRI.", "SOR.", "GIR.", "CEB.")


def _classify(symbol: str) -> str | None:
    s = symbol.upper()
    if s.startswith(DOLLAR_PREFIXES):
        return "DOLAR"
    for p in GRAIN_PREFIXES:
        if s.startswith(p):
            return "GRANO"
    return None


logger = logging.getLogger("rofex_manager")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)


_MD_ENTRIES = [
    pyRofex.MarketDataEntry.BIDS,
    pyRofex.MarketDataEntry.OFFERS,
    pyRofex.MarketDataEntry.LAST,
    pyRofex.MarketDataEntry.OPENING_PRICE,
    pyRofex.MarketDataEntry.CLOSING_PRICE,
    pyRofex.MarketDataEntry.SETTLEMENT_PRICE,
    pyRofex.MarketDataEntry.TRADE_VOLUME,
    pyRofex.MarketDataEntry.OPEN_INTEREST,
    pyRofex.MarketDataEntry.NOMINAL_VOLUME,
]


class RofexManager:
    _instance: "RofexManager | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.initialized    = False
        self.error: str | None = None

        self.symbols_veta:      list[str] = []
        self.symbols_remarkets: list[str] = []
        self.symbols:           list[str] = []

        self.instrument_meta: dict[str, dict[str, Any]] = {}
        self.market_data:     dict[str, dict[str, Any]] = {}
        self.last_update:     datetime | None = None

        self.snapshot_total    = 0
        self.snapshot_done     = 0
        self.snapshot_saved    = 0
        self.snapshot_finished = False

        self.ws_subscribed_veta      = False
        self.ws_subscribed_remarkets = False

        self.external_data:        dict[str, list[dict[str, Any]]] = {}
        self.external_last_update: datetime | None = None
        self.external_errors:      dict[str, str]  = {}

        self._md_lock = threading.Lock()

    @property
    def ws_subscribed(self) -> bool:
        return self.ws_subscribed_veta or self.ws_subscribed_remarkets

    @classmethod
    def get(cls) -> "RofexManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def initialize(self) -> None:
        if self.initialized:
            return

        veta_user     = _env("VETA_USER")
        veta_password = _env("VETA_PASSWORD")
        veta_account  = _env("VETA_ACCOUNT")

        rm_user     = _env("PYROFEX_USER")
        rm_password = _env("PYROFEX_PASSWORD")
        rm_account  = _env("PYROFEX_ACCOUNT")

        veta_ok      = bool(veta_user and veta_password and veta_account)
        remarkets_ok = bool(rm_user and rm_password and rm_account)

        if not veta_ok and not remarkets_ok:
            self.error = (
                "Faltan credenciales.\n"
                "- VETA_USER / VETA_PASSWORD / VETA_ACCOUNT (dólar futuro)\n"
                "- PYROFEX_USER / PYROFEX_PASSWORD / PYROFEX_ACCOUNT (granos)"
            )
            return

        # Conectar Veta
        if veta_ok:
            try:
                # Seteamos la URL de Veta via variable de entorno que pyRofex lee internamente
                os.environ["PRIMARY_API_URL"] = VETA_BASE_URL
                os.environ["PRIMARY_WS_URL"]  = VETA_WS_URL
                pyRofex.initialize(
                    user=veta_user,
                    password=veta_password,
                    account=veta_account,
                    environment=pyRofex.Environment.REMARKET,
                )
                logger.info("Conectado a VETA OK")
                self._discover_instruments("VETA", "DOLAR")
            except Exception as e:
                logger.exception("Fallo Veta: %s", e)
                self.error = f"Veta: {e}"
            finally:
                # Limpiar variables para que Remarkets use su propia URL
                os.environ.pop("PRIMARY_API_URL", None)
                os.environ.pop("PRIMARY_WS_URL", None)

        # Conectar Remarkets
        if remarkets_ok:
            try:
                pyRofex.initialize(
                    user=rm_user,
                    password=rm_password,
                    account=rm_account,
                    environment=pyRofex.Environment.REMARKET,
                )
                logger.info("Conectado a REMARKETS OK")
                self._discover_instruments("REMARKETS", "GRANO")
            except Exception as e:
                logger.exception("Fallo Remarkets: %s", e)
                self.error = (self.error + f" | Remarkets: {e}") if self.error else f"Remarkets: {e}"

        self.symbols = sorted(set(self.symbols_veta + self.symbols_remarkets))
        self.initialized = True
        logger.info(
            "RofexManager listo — Veta DLR: %d, Remarkets granos: %d",
            len(self.symbols_veta), len(self.symbols_remarkets),
        )

        if veta_ok and self.symbols_veta:
            threading.Thread(
                target=self._snapshot_and_ws,
                args=("VETA", self.symbols_veta, SNAPSHOT_SLEEP_VETA),
                daemon=True,
            ).start()

        if remarkets_ok and self.symbols_remarkets:
            threading.Thread(
                target=self._snapshot_and_ws,
                args=("REMARKETS", self.symbols_remarkets, SNAPSHOT_SLEEP_REMARKETS),
                daemon=True,
            ).start()

        threading.Thread(target=self._external_polling_loop, daemon=True).start()

    def _discover_instruments(self, source: str, category_filter: str) -> None:
        resp        = pyRofex.get_all_instruments()
        instruments = resp.get("instruments", []) if isinstance(resp, dict) else []
        for inst in instruments:
            ident  = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
            symbol = ident.get("symbol")
            if not symbol:
                continue
            if _classify(symbol) != category_filter:
                continue
            self.instrument_meta[symbol] = {
                "symbol":     symbol,
                "category":   category_filter,
                "underlying": symbol.split("/")[0],
                "market":     ident.get("marketId"),
                "cficode":    inst.get("cficode"),
                "source":     source,
            }
            db.upsert_instrument({
                "symbol":     symbol,
                "category":   category_filter,
                "underlying": symbol.split("/")[0],
            })
            if source == "VETA":
                self.symbols_veta.append(symbol)
            else:
                self.symbols_remarkets.append(symbol)

        self.symbols_veta      = sorted(set(self.symbols_veta))
        self.symbols_remarkets = sorted(set(self.symbols_remarkets))
        logger.info("Descubiertos %s (%s): %d", category_filter, source,
                    len(self.symbols_veta if source == "VETA" else self.symbols_remarkets))

    def _snapshot_and_ws(self, source: str, symbols: list[str], sleep_secs: float) -> None:
        self._snapshot(source, symbols, sleep_secs)
        self._connect_ws(source, symbols)

    def _snapshot(self, source: str, symbols: list[str], sleep_secs: float) -> None:
        self.snapshot_total += len(symbols)
        logger.info("Snapshot %s: %d instrumentos", source, len(symbols))
        for symbol in symbols:
            try:
                resp = pyRofex.get_market_data(ticker=symbol, entries=_MD_ENTRIES)
                if not isinstance(resp, dict) or resp.get("status") != "OK":
                    continue
                row = self._parse_md(symbol, resp.get("marketData") or {})
                with self._md_lock:
                    self.market_data[symbol] = {**self.market_data.get(symbol, {}), **row}
                    self.last_update = datetime.now(tz=timezone.utc)
                db.insert_tick(self._tick_row(row))
                self.snapshot_saved += 1
            except Exception:
                logger.exception("Snapshot %s error en %s", source, symbol)
            finally:
                self.snapshot_done += 1
                time.sleep(sleep_secs)
        self.snapshot_finished = (self.snapshot_done >= self.snapshot_total)
        logger.info("Snapshot %s completo", source)

    def _connect_ws(self, source: str, symbols: list[str]) -> None:
        while True:
            try:
                pyRofex.init_websocket_connection(
                    market_data_handler=self._on_market_data,
                    error_handler=lambda m, s=source: self._on_ws_error(s, m),
                    exception_handler=lambda e, s=source: self._on_ws_exc(s, e),
                )
                pyRofex.market_data_subscription(tickers=symbols, entries=_MD_ENTRIES)
                if source == "VETA":
                    self.ws_subscribed_veta = True
                else:
                    self.ws_subscribed_remarkets = True
                logger.info("WS %s suscripto a %d instrumentos", source, len(symbols))
                self._heartbeat_loop(source)
            except Exception:
                logger.exception("WS %s falló — reintentando en 30s", source)

            if source == "VETA":
                self.ws_subscribed_veta = False
            else:
                self.ws_subscribed_remarkets = False
            time.sleep(30)

    def _heartbeat_loop(self, source: str) -> None:
        while True:
            active = self.ws_subscribed_veta if source == "VETA" else self.ws_subscribed_remarkets
            if not active:
                break
            try:
                pyRofex.heartbeat()
                logger.debug("Heartbeat %s OK", source)
            except Exception as e:
                logger.warning("Heartbeat %s falló: %s", source, e)
                if source == "VETA":
                    self.ws_subscribed_veta = False
                else:
                    self.ws_subscribed_remarkets = False
                break
            time.sleep(30)

    def _on_market_data(self, message: dict[str, Any]) -> None:
        try:
            symbol = (message.get("instrumentId") or {}).get("symbol")
            if not symbol:
                return
            row = self._parse_md(symbol, message.get("marketData") or {})
            with self._md_lock:
                merged = {**self.market_data.get(symbol, {})}
                for k, v in row.items():
                    if v is not None:
                        merged[k] = v
                prev = merged.get("settlement_price") or merged.get("closing_price")
                ref  = merged.get("last_price") or merged.get("offer") or merged.get("bid")
                if prev and ref:
                    try:
                        merged["change_pct"] = (ref - prev) / prev * 100
                    except ZeroDivisionError:
                        merged["change_pct"] = None
                merged["prev_close"] = prev
                merged["ts"]         = datetime.now(tz=timezone.utc).isoformat()
                merged["symbol"]     = symbol
                self.market_data[symbol] = merged
                self.last_update = datetime.now(tz=timezone.utc)
            db.insert_tick(self._tick_row(merged))
        except Exception:
            logger.exception("Error procesando market data")

    def _on_ws_error(self, source: str, message: Any) -> None:
        logger.error("WS %s error: %s", source, message)
        if source == "VETA":
            self.ws_subscribed_veta = False
        else:
            self.ws_subscribed_remarkets = False

    def _on_ws_exc(self, source: str, exc: Exception) -> None:
        logger.exception("WS %s exception: %s", source, exc)
        if source == "VETA":
            self.ws_subscribed_veta = False
        else:
            self.ws_subscribed_remarkets = False

    def _external_polling_loop(self) -> None:
        while True:
            for key, url in EXTERNAL_API_URLS.items():
                try:
                    resp = requests.get(url, timeout=8)
                    resp.raise_for_status()
                    data = resp.json()
                    if not isinstance(data, list):
                        data = []
                    with self._md_lock:
                        self.external_data[key] = data
                        self.external_last_update = datetime.now(tz=timezone.utc)
                        self.external_errors.pop(key, None)
                except Exception as e:
                    with self._md_lock:
                        self.external_errors[key] = str(e)
                    logger.warning("Fallo %s: %s", key, e)
            time.sleep(EXTERNAL_REFRESH_SECS)

    def get_external(self, key: str) -> list[dict[str, Any]]:
        with self._md_lock:
            return list(self.external_data.get(key, []))

    def _parse_md(self, symbol: str, md: dict[str, Any]) -> dict[str, Any]:
        last_price  = _price(md, "LA")
        bid_price   = _price(md, "BI")
        bid_size    = _size(md,  "BI")
        offer_price = _price(md, "OF")
        offer_size  = _size(md,  "OF")
        opening     = _val(md,   "OP")
        closing     = _val(md,   "CL")
        settlement  = _val(md,   "SE")
        trade_vol   = _val(md,   "TV")
        open_int    = _val(md,   "OI")
        nominal_vol = _val(md,   "NV")
        prev_close  = settlement or closing
        ref         = last_price or offer_price or bid_price
        change_pct  = None
        if prev_close and ref:
            try:
                change_pct = (ref - prev_close) / prev_close * 100
            except ZeroDivisionError:
                pass
        return {
            "symbol":           symbol,
            "ts":               datetime.now(tz=timezone.utc).isoformat(),
            "last_price":       last_price,
            "bid":              bid_price,
            "bid_size":         bid_size,
            "offer":            offer_price,
            "offer_size":       offer_size,
            "opening_price":    opening,
            "closing_price":    closing,
            "settlement_price": settlement,
            "trade_volume":     trade_vol,
            "open_interest":    open_int,
            "nominal_volume":   nominal_vol,
            "prev_close":       prev_close,
            "change_pct":       change_pct,
        }

    @staticmethod
    def _tick_row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "ts":               r.get("ts"),
            "symbol":           r.get("symbol"),
            "last_price":       r.get("last_price"),
            "bid":              r.get("bid"),
            "bid_size":         r.get("bid_size"),
            "offer":            r.get("offer"),
            "offer_size":       r.get("offer_size"),
            "volume":           r.get("trade_volume"),
            "open_interest":    r.get("open_interest"),
            "settlement_price": r.get("settlement_price"),
            "prev_close":       r.get("prev_close"),
            "change_pct":       r.get("change_pct"),
        }

    def snapshot(self) -> list[dict[str, Any]]:
        with self._md_lock:
            return [
                {**self.instrument_meta.get(s, {}), **self.market_data.get(s, {})}
                for s in self.symbols
            ]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _env(key: str) -> str:
    return (os.environ.get(key) or "").strip().strip('"').strip("'")


def _price(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, dict):              return v.get("price")
    if isinstance(v, list) and v:
        f = v[0]
        if isinstance(f, dict):          return f.get("price")
    if isinstance(v, (int, float)):      return float(v)
    return None


def _size(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, dict):              return v.get("size")
    if isinstance(v, list) and v:
        f = v[0]
        if isinstance(f, dict):          return f.get("size")
    return None


def _val(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, (int, float)):      return float(v)
    if isinstance(v, dict):
        return v.get("price") or v.get("size") or v.get("value")
    return None
