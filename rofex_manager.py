"""Singleton manager con doble conexión:
- VETA (REST + WebSocket directo): DLR/ → datos en tiempo real sin pyRofex
- REMARKETS (pyRofex): granos (SOJ, MAI, TRI, etc.)

Mantiene estado en memoria para que Streamlit lea.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import pyRofex
import requests

import db


# ─── APIs externas (data912) ─────────────────────────────────────────────────
EXTERNAL_API_URLS = {
    "MEP":      "https://data912.com/live/mep",
    "CCL":      "https://data912.com/live/ccl",
    "ACCIONES": "https://data912.com/live/arg_stocks",
    "BONOS":    "https://data912.com/live/arg_bonds",
    "CEDEARS":  "https://data912.com/live/arg_cedears",
}
EXTERNAL_REFRESH_SECS = 5

# ─── Veta ─────────────────────────────────────────────────────────────────────
VETA_API  = "https://api.veta.xoms.com.ar"
VETA_WS   = "wss://api.veta.xoms.com.ar/websocket/auth"

SNAPSHOT_SLEEP = 0.1

# ─── Clasificación ────────────────────────────────────────────────────────────
DOLLAR_PREFIXES = ("DLR/",)
GRAIN_PREFIXES  = ("SOJ.", "MAI.", "TRI.", "SOR.", "GIR.", "CEB.")


def _classify(symbol: str) -> str | None:
    s = symbol.upper()
    if s.startswith(DOLLAR_PREFIXES):  return "DOLAR"
    for p in GRAIN_PREFIXES:
        if s.startswith(p):            return "GRANO"
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

_MD_ENTRY_CODES = ["BI", "OF", "LA", "OP", "CL", "SE", "TV", "OI", "NV"]


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
        self.snapshot_finished_veta      = False
        self.snapshot_finished_remarkets = False

        self.ws_subscribed_veta      = False
        self.ws_subscribed_remarkets = False

        self.external_data:        dict[str, list[dict[str, Any]]] = {}
        self.external_last_update: datetime | None = None
        self.external_errors:      dict[str, str]  = {}

        # Token Veta (dura 24hs)
        self._veta_token: str | None = None
        self._veta_account: str = ""

        self._md_lock = threading.Lock()

    @property
    def ws_subscribed(self) -> bool:
        return self.ws_subscribed_veta or self.ws_subscribed_remarkets

    @property
    def snapshot_finished(self) -> bool:
        """True si ambos snapshots (los que aplican) terminaron."""
        veta_done = self.snapshot_finished_veta or not self.symbols_veta
        rm_done   = self.snapshot_finished_remarkets or not self.symbols_remarkets
        return veta_done and rm_done

    @classmethod
    def get(cls) -> "RofexManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    # ── Inicialización ────────────────────────────────────────────────────────
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
            self.error = "Faltan credenciales VETA o PYROFEX"
            return

        # ── Conectar Veta directamente (REST) ─────────────────────────────────
        if veta_ok:
            self._veta_account = veta_account
            token = self._veta_get_token(veta_user, veta_password)
            if token:
                self._veta_token = token
                logger.info("Token Veta OK")
                self._veta_discover_instruments()
            else:
                self.error = "Veta: no se pudo obtener token"

        # ── Conectar Remarkets via pyRofex (granos) ───────────────────────────
        if remarkets_ok:
            try:
                pyRofex.initialize(
                    user=rm_user,
                    password=rm_password,
                    account=rm_account,
                    environment=pyRofex.Environment.REMARKET,
                )
                logger.info("Conectado a REMARKETS OK")
                self._remarkets_discover_instruments()
            except Exception as e:
                logger.exception("Fallo Remarkets: %s", e)
                self.error = (self.error + f" | Remarkets: {e}") if self.error else f"Remarkets: {e}"

        self.symbols = sorted(set(self.symbols_veta + self.symbols_remarkets))
        self.initialized = True
        logger.info(
            "RofexManager listo — Veta DLR: %d, Remarkets granos: %d",
            len(self.symbols_veta), len(self.symbols_remarkets),
        )

        if veta_ok and self._veta_token and self.symbols_veta:
            threading.Thread(target=self._veta_snapshot_and_ws, daemon=True).start()

        if remarkets_ok and self.symbols_remarkets:
            threading.Thread(target=self._remarkets_snapshot_and_ws, daemon=True).start()

        threading.Thread(target=self._external_polling_loop, daemon=True).start()

    # ── Veta: autenticación ───────────────────────────────────────────────────
    def _veta_get_token(self, user: str, password: str) -> str | None:
        try:
            resp = requests.post(
                f"{VETA_API}/auth/getToken",
                headers={
                    "X-Username": user,
                    "X-Password": password,
                },
                timeout=10,
            )
            resp.raise_for_status()
            # El token viene en el header X-Auth-Token, no en el body
            token = resp.headers.get("X-Auth-Token")
            if token:
                logger.info("Token Veta obtenido OK")
                return token
            logger.error("Veta: X-Auth-Token no encontrado en headers. Headers: %s", dict(resp.headers))
            return None
        except Exception as e:
            logger.exception("Veta getToken falló: %s", e)
            return None

    # ── Veta: descubrimiento de instrumentos ──────────────────────────────────
    def _veta_discover_instruments(self) -> None:
        try:
            resp = requests.get(
                f"{VETA_API}/rest/instruments/all",
                headers={"X-Auth-Token": self._veta_token},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            instruments = data.get("instruments", data) if isinstance(data, dict) else data
            for inst in (instruments or []):
                ident  = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
                symbol = ident.get("symbol") or inst.get("symbol")
                if not symbol or _classify(symbol) != "DOLAR":
                    continue
                self.instrument_meta[symbol] = {
                    "symbol":     symbol,
                    "category":   "DOLAR",
                    "underlying": symbol.split("/")[0],
                    "source":     "VETA",
                }
                db.upsert_instrument({"symbol": symbol, "category": "DOLAR", "underlying": "DLR"})
                self.symbols_veta.append(symbol)
            self.symbols_veta = sorted(set(self.symbols_veta))
            logger.info("Veta DLR descubiertos: %d", len(self.symbols_veta))
        except Exception as e:
            logger.exception("Veta discover falló: %s", e)

    # ── Veta: snapshot REST ───────────────────────────────────────────────────
    def _veta_snapshot_and_ws(self) -> None:
        self.snapshot_total += len(self.symbols_veta)
        logger.info("Snapshot Veta: %d instrumentos", len(self.symbols_veta))

        entries_param = ",".join(_MD_ENTRY_CODES)
        for symbol in self.symbols_veta:
            try:
                resp = requests.get(
                    f"{VETA_API}/rest/marketdata/get",
                    headers={"X-Auth-Token": self._veta_token},
                    params={"ticker": symbol, "entries": entries_param},
                    timeout=3,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    md   = data.get("marketData", {}) or {}
                    row  = self._parse_md(symbol, md)
                    with self._md_lock:
                        self.market_data[symbol] = {**self.market_data.get(symbol, {}), **row}
                        self.last_update = datetime.now(tz=timezone.utc)
                    db.insert_tick(self._tick_row(row))
                    self.snapshot_saved += 1
            except Exception:
                logger.exception("Snapshot Veta error en %s", symbol)
            finally:
                self.snapshot_done += 1
                time.sleep(SNAPSHOT_SLEEP)

        self.snapshot_finished_veta = True
        logger.info("Snapshot Veta completo")
        self._veta_connect_ws()

    # ── Veta: WebSocket ───────────────────────────────────────────────────────
    def _veta_connect_ws(self) -> None:
        try:
            import websocket
        except ImportError:
            logger.error("websocket-client no instalado — Veta WS no disponible")
            return

        def _on_open(ws: Any) -> None:
            # Autenticar y suscribir
            ws.send(json.dumps({"type": "auth", "token": self._veta_token}))
            msg = {
                "type": "md",
                "entries": _MD_ENTRY_CODES,
                "tickers": self.symbols_veta,
            }
            ws.send(json.dumps(msg))
            self.ws_subscribed_veta = True
            logger.info("Veta WS conectado y suscripto")

        def _on_message(ws: Any, raw: str) -> None:
            try:
                msg = json.loads(raw)
                # Heartbeat de vuelta
                if msg.get("type") == "heartbeat":
                    ws.send(json.dumps({"type": "heartbeat"}))
                    return
                symbol = (msg.get("instrumentId") or {}).get("symbol") or msg.get("symbol")
                if not symbol:
                    return
                md  = msg.get("marketData") or {}
                row = self._parse_md(symbol, md)
                with self._md_lock:
                    merged = {**self.market_data.get(symbol, {})}
                    for k, v in row.items():
                        if v is not None:
                            merged[k] = v
                    self._recalc_change(merged)
                    merged["ts"]     = datetime.now(tz=timezone.utc).isoformat()
                    merged["symbol"] = symbol
                    self.market_data[symbol] = merged
                    self.last_update = datetime.now(tz=timezone.utc)
                db.insert_tick(self._tick_row(merged))
            except Exception:
                logger.exception("Veta WS mensaje error")

        def _on_error(ws: Any, err: Any) -> None:
            logger.error("Veta WS error: %s", err)
            self.ws_subscribed_veta = False

        def _on_close(ws: Any, *args: Any) -> None:
            logger.warning("Veta WS cerrado — reconectando en 30s")
            self.ws_subscribed_veta = False

        while True:
            try:
                ws = websocket.WebSocketApp(
                    VETA_WS,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                logger.exception("Veta WS run_forever falló")
            self.ws_subscribed_veta = False
            time.sleep(30)

    # ── Remarkets: descubrimiento ─────────────────────────────────────────────
    def _remarkets_discover_instruments(self) -> None:
        resp        = pyRofex.get_all_instruments()
        instruments = resp.get("instruments", []) if isinstance(resp, dict) else []
        for inst in instruments:
            ident  = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
            symbol = ident.get("symbol")
            if not symbol or _classify(symbol) != "GRANO":
                continue
            self.instrument_meta[symbol] = {
                "symbol":     symbol,
                "category":   "GRANO",
                "underlying": symbol.split("/")[0],
                "source":     "REMARKETS",
            }
            db.upsert_instrument({"symbol": symbol, "category": "GRANO", "underlying": symbol.split("/")[0]})
            self.symbols_remarkets.append(symbol)
        self.symbols_remarkets = sorted(set(self.symbols_remarkets))
        logger.info("Remarkets granos descubiertos: %d", len(self.symbols_remarkets))

    # ── Remarkets: snapshot + WS ──────────────────────────────────────────────
    def _remarkets_snapshot_and_ws(self) -> None:
        self.snapshot_total += len(self.symbols_remarkets)
        logger.info("Snapshot Remarkets: %d instrumentos", len(self.symbols_remarkets))
        for symbol in self.symbols_remarkets:
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
                logger.exception("Snapshot Remarkets error en %s", symbol)
            finally:
                self.snapshot_done += 1
                time.sleep(SNAPSHOT_SLEEP)
        self.snapshot_finished_remarkets = True
        logger.info("Snapshot Remarkets completo")
        self._remarkets_connect_ws()

    def _remarkets_connect_ws(self) -> None:
        while True:
            try:
                pyRofex.init_websocket_connection(
                    market_data_handler=self._on_remarkets_md,
                    error_handler=self._on_remarkets_error,
                    exception_handler=self._on_remarkets_exc,
                )
                pyRofex.market_data_subscription(
                    tickers=self.symbols_remarkets,
                    entries=_MD_ENTRIES,
                )
                self.ws_subscribed_remarkets = True
                logger.info("Remarkets WS suscripto a %d granos", len(self.symbols_remarkets))
                # Heartbeat loop
                while self.ws_subscribed_remarkets:
                    try:
                        pyRofex.heartbeat()
                    except Exception as e:
                        logger.warning("Remarkets heartbeat falló: %s", e)
                        self.ws_subscribed_remarkets = False
                        break
                    time.sleep(30)
            except Exception:
                logger.exception("Remarkets WS falló — reintentando en 30s")
            self.ws_subscribed_remarkets = False
            time.sleep(30)

    def _on_remarkets_md(self, message: dict[str, Any]) -> None:
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
                self._recalc_change(merged)
                merged["ts"]     = datetime.now(tz=timezone.utc).isoformat()
                merged["symbol"] = symbol
                self.market_data[symbol] = merged
                self.last_update = datetime.now(tz=timezone.utc)
            db.insert_tick(self._tick_row(merged))
        except Exception:
            logger.exception("Error procesando Remarkets market data")

    def _on_remarkets_error(self, message: Any) -> None:
        logger.error("Remarkets WS error: %s", message)
        self.ws_subscribed_remarkets = False

    def _on_remarkets_exc(self, exc: Exception) -> None:
        logger.exception("Remarkets WS exception: %s", exc)
        self.ws_subscribed_remarkets = False

    # ── Polling externo (data912) ─────────────────────────────────────────────
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

    # ── Helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _recalc_change(merged: dict[str, Any]) -> None:
        prev = merged.get("settlement_price") or merged.get("closing_price")
        ref  = merged.get("last_price") or merged.get("offer") or merged.get("bid")
        if prev and ref:
            try:
                merged["change_pct"] = (ref - prev) / prev * 100
            except ZeroDivisionError:
                merged["change_pct"] = None
        merged["prev_close"] = prev

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


# ── Helpers de extracción ─────────────────────────────────────────────────────
def _env(key: str) -> str:
    return (os.environ.get(key) or "").strip().strip('"').strip("'")


def _price(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, dict):         return v.get("price")
    if isinstance(v, list) and v:
        f = v[0]
        if isinstance(f, dict):     return f.get("price")
    if isinstance(v, (int, float)): return float(v)
    return None


def _size(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, dict):         return v.get("size")
    if isinstance(v, list) and v:
        f = v[0]
        if isinstance(f, dict):     return f.get("size")
    return None


def _val(entries: dict, key: str) -> float | None:
    v = entries.get(key)
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, dict):
        return v.get("price") or v.get("size") or v.get("value")
    return None
