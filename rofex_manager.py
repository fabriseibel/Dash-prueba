"""Singleton manager con doble conexión:
- VETA (REST polling cada 2s): DLR/ → datos en tiempo real
- REMARKETS (pyRofex WS): granos (SOJ, MAI, TRI, etc.)
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

EXTERNAL_API_URLS = {
    "MEP":      "https://data912.com/live/mep",
    "CCL":      "https://data912.com/live/ccl",
    "ACCIONES": "https://data912.com/live/arg_stocks",
    "BONOS":    "https://data912.com/live/arg_bonds",
    "CEDEARS":  "https://data912.com/live/arg_cedears",
}
EXTERNAL_REFRESH_SECS = 5

VETA_API    = "https://api.veta.xoms.com.ar"
VETA_MARKET = "ROFX"
VETA_POLL_SECS = 2  # Polling cada 2 segundos

logger = logging.getLogger("rofex_manager")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)

DOLLAR_PREFIXES = ("DLR/",)
GRAIN_PREFIXES  = ("SOJ.", "MAI.", "TRI.", "SOR.", "GIR.", "CEB.")


def _classify(symbol: str) -> str | None:
    s = symbol.upper()
    if s.startswith(DOLLAR_PREFIXES): return "DOLAR"
    for p in GRAIN_PREFIXES:
        if s.startswith(p): return "GRANO"
    return None


_MD_ENTRIES_PYROFEX = [
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
_MD_ENTRIES_VETA = "LA,BI,OF,OP,CL,SE,TV,OI,NV"


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

        self.snapshot_total              = 0
        self.snapshot_done               = 0
        self.snapshot_saved              = 0
        self.snapshot_finished_veta      = False
        self.snapshot_finished_remarkets = False

        self.ws_subscribed_veta      = False
        self.ws_subscribed_remarkets = False

        self._veta_token:    str | None = None
        self._veta_user:     str = ""
        self._veta_password: str = ""

        self.external_data:        dict[str, list[dict[str, Any]]] = {}
        self.external_last_update: datetime | None = None
        self.external_errors:      dict[str, str]  = {}
        self._md_lock = threading.Lock()

    @property
    def ws_subscribed(self) -> bool:
        return self.ws_subscribed_veta or self.ws_subscribed_remarkets

    @property
    def snapshot_finished(self) -> bool:
        v = self.snapshot_finished_veta or not self.symbols_veta
        r = self.snapshot_finished_remarkets or not self.symbols_remarkets
        return v and r

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
        rm_user       = _env("PYROFEX_USER")
        rm_password   = _env("PYROFEX_PASSWORD")
        rm_account    = _env("PYROFEX_ACCOUNT")

        veta_ok      = bool(veta_user and veta_password and veta_account)
        remarkets_ok = bool(rm_user and rm_password and rm_account)

        if not veta_ok and not remarkets_ok:
            self.error = "Faltan credenciales VETA o PYROFEX"
            return

        if veta_ok:
            self._veta_user     = veta_user
            self._veta_password = veta_password
            token = self._veta_login()
            if token:
                self._veta_token = token
                logger.info("Veta token OK")
                self._veta_discover()
            else:
                self.error = "Veta: no se pudo obtener token"

        if remarkets_ok:
            try:
                pyRofex.initialize(
                    user=rm_user,
                    password=rm_password,
                    account=rm_account,
                    environment=pyRofex.Environment.REMARKET,
                )
                logger.info("Remarkets OK")
                self._remarkets_discover()
            except Exception as e:
                logger.exception("Fallo Remarkets: %s", e)
                self.error = (self.error + f" | Remarkets: {e}") if self.error else f"Remarkets: {e}"

        self.symbols   = sorted(set(self.symbols_veta + self.symbols_remarkets))
        self.initialized = True
        logger.info("RofexManager listo — Veta DLR: %d, Remarkets granos: %d",
                    len(self.symbols_veta), len(self.symbols_remarkets))

        if veta_ok and self._veta_token and self.symbols_veta:
            threading.Thread(target=self._veta_run, daemon=True).start()

        if remarkets_ok and self.symbols_remarkets:
            threading.Thread(target=self._remarkets_run, daemon=True).start()

        threading.Thread(target=self._external_polling_loop, daemon=True).start()

    def _veta_login(self) -> str | None:
        try:
            resp = requests.post(
                f"{VETA_API}/auth/getToken",
                headers={"X-Username": self._veta_user, "X-Password": self._veta_password},
                timeout=10,
            )
            token = resp.headers.get("X-Auth-Token")
            if token:
                return token
            logger.error("Veta: X-Auth-Token no encontrado. Status=%s body=%s",
                        resp.status_code, resp.text[:200])
            return None
        except Exception as e:
            logger.exception("Veta login falló: %s", e)
            return None

    def _veta_discover(self) -> None:
        try:
            resp = requests.get(
                f"{VETA_API}/rest/instruments/all",
                headers={"X-Auth-Token": self._veta_token},
                params={"marketId": VETA_MARKET},
                timeout=15,
            )
            data = resp.json()
            instruments = data.get("instruments", []) if isinstance(data, dict) else []
            for inst in instruments:
                iid    = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
                symbol = iid.get("symbol")
                if not symbol or _classify(symbol) != "DOLAR":
                    continue
                self.instrument_meta[symbol] = {
                    "symbol":     symbol,
                    "category":   "DOLAR",
                    "underlying": "DLR",
                    "source":     "VETA",
                }
                db.upsert_instrument({"symbol": symbol, "category": "DOLAR", "underlying": "DLR"})
                self.symbols_veta.append(symbol)
            self.symbols_veta = sorted(set(self.symbols_veta))
            logger.info("Veta DLR descubiertos: %d", len(self.symbols_veta))
        except Exception as e:
            logger.exception("Veta discover falló: %s", e)

    def _veta_run(self) -> None:
        threading.Thread(target=self._veta_token_loop, daemon=True).start()

        # Snapshot inicial
        self.snapshot_total += len(self.symbols_veta)
        logger.info("Snapshot Veta: %d instrumentos", len(self.symbols_veta))
        for symbol in self.symbols_veta:
            try:
                md = self._veta_fetch_one(symbol)
                if md:
                    with self._md_lock:
                        self.market_data[symbol] = {**self.market_data.get(symbol, {}), **md}
                        self.last_update = datetime.now(tz=timezone.utc)
                    db.insert_tick(self._tick_row(md))
                    self.snapshot_saved += 1
            except Exception:
                pass
            finally:
                self.snapshot_done += 1
                time.sleep(0.1)

        self.snapshot_finished_veta = True
        logger.info("Snapshot Veta completo — iniciando polling cada %ds", VETA_POLL_SECS)

        # Polling continuo
        while True:
            for symbol in self.symbols_veta:
                try:
                    md = self._veta_fetch_one(symbol)
                    if md:
                        with self._md_lock:
                            merged = {**self.market_data.get(symbol, {})}
                            for k, v in md.items():
                                if v is not None:
                                    merged[k] = v
                            self._recalc(merged)
                            merged["ts"]     = datetime.now(tz=timezone.utc).isoformat()
                            merged["symbol"] = symbol
                            self.market_data[symbol] = merged
                            self.last_update = datetime.now(tz=timezone.utc)
                        self.ws_subscribed_veta = True
                except Exception:
                    pass
            time.sleep(VETA_POLL_SECS)

    def _veta_fetch_one(self, symbol: str) -> dict[str, Any] | None:
        resp = requests.get(
            f"{VETA_API}/rest/marketdata/get",
            headers={"X-Auth-Token": self._veta_token},
            params={"symbol": symbol, "marketId": VETA_MARKET, "entries": _MD_ENTRIES_VETA},
            timeout=4,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("status") != "OK":
            return None
        return self._parse_veta_md(symbol, data.get("marketData") or {})

    def _veta_token_loop(self) -> None:
        while True:
            time.sleep(23 * 3600)
            new = self._veta_login()
            if new:
                self._veta_token = new
                logger.info("Token Veta renovado")

    def _remarkets_discover(self) -> None:
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
            db.upsert_instrument({"symbol": symbol, "category": "GRANO",
                                  "underlying": symbol.split("/")[0]})
            self.symbols_remarkets.append(symbol)
        self.symbols_remarkets = sorted(set(self.symbols_remarkets))
        logger.info("Remarkets granos: %d", len(self.symbols_remarkets))

    def _remarkets_run(self) -> None:
        threading.Thread(target=self._remarkets_ws, daemon=True).start()

        self.snapshot_total += len(self.symbols_remarkets)
        for symbol in self.symbols_remarkets:
            try:
                resp = pyRofex.get_market_data(ticker=symbol, entries=_MD_ENTRIES_PYROFEX)
                if isinstance(resp, dict) and resp.get("status") == "OK":
                    row = self._parse_pyrofex_md(symbol, resp.get("marketData") or {})
                    with self._md_lock:
                        self.market_data[symbol] = {**self.market_data.get(symbol, {}), **row}
                        self.last_update = datetime.now(tz=timezone.utc)
                    db.insert_tick(self._tick_row(row))
                    self.snapshot_saved += 1
            except Exception:
                pass
            finally:
                self.snapshot_done += 1
                time.sleep(0.1)
        self.snapshot_finished_remarkets = True
        logger.info("Snapshot Remarkets completo")

    def _remarkets_ws(self) -> None:
        while True:
            try:
                pyRofex.init_websocket_connection(
                    market_data_handler=self._on_remarkets_md,
                    error_handler=lambda m: setattr(self, 'ws_subscribed_remarkets', False),
                    exception_handler=lambda e: setattr(self, 'ws_subscribed_remarkets', False),
                )
                pyRofex.market_data_subscription(
                    tickers=self.symbols_remarkets,
                    entries=_MD_ENTRIES_PYROFEX,
                )
                self.ws_subscribed_remarkets = True
                logger.info("Remarkets WS suscripto a %d granos", len(self.symbols_remarkets))
                while self.ws_subscribed_remarkets:
                    try:
                        pyRofex.heartbeat()
                    except Exception:
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
            row = self._parse_pyrofex_md(symbol, message.get("marketData") or {})
            with self._md_lock:
                merged = {**self.market_data.get(symbol, {})}
                for k, v in row.items():
                    if v is not None:
                        merged[k] = v
                self._recalc(merged)
                merged["ts"]     = datetime.now(tz=timezone.utc).isoformat()
                merged["symbol"] = symbol
                self.market_data[symbol] = merged
                self.last_update = datetime.now(tz=timezone.utc)
            db.insert_tick(self._tick_row(merged))
        except Exception:
            logger.exception("Error procesando Remarkets MD")

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
            time.sleep(EXTERNAL_REFRESH_SECS)

    def get_external(self, key: str) -> list[dict[str, Any]]:
        with self._md_lock:
            return list(self.external_data.get(key, []))

    def _parse_veta_md(self, symbol: str, md: dict[str, Any]) -> dict[str, Any]:
        def _v(key: str) -> float | None:
            obj = md.get(key)
            if obj is None:                 return None
            if isinstance(obj, (int, float)): return float(obj)
            if isinstance(obj, dict):       return obj.get("price") or obj.get("size")
            if isinstance(obj, list) and obj:
                f = obj[0]
                if isinstance(f, dict):     return f.get("price")
            return None

        last_price  = _v("LA")
        bid_price   = _v("BI")
        offer_price = _v("OF")
        settlement  = _v("SE")
        closing     = _v("CL")
        trade_vol   = _v("TV")
        open_int    = _v("OI")
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
            "offer":            offer_price,
            "settlement_price": settlement,
            "closing_price":    closing,
            "trade_volume":     trade_vol,
            "open_interest":    open_int,
            "prev_close":       prev_close,
            "change_pct":       change_pct,
        }

    def _parse_pyrofex_md(self, symbol: str, md: dict[str, Any]) -> dict[str, Any]:
        last_price  = _price(md, "LA")
        bid_price   = _price(md, "BI")
        bid_size    = _size(md,  "BI")
        offer_price = _price(md, "OF")
        offer_size  = _size(md,  "OF")
        closing     = _val(md,   "CL")
        settlement  = _val(md,   "SE")
        trade_vol   = _val(md,   "TV")
        open_int    = _val(md,   "OI")
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
            "closing_price":    closing,
            "settlement_price": settlement,
            "trade_volume":     trade_vol,
            "open_interest":    open_int,
            "prev_close":       prev_close,
            "change_pct":       change_pct,
        }

    @staticmethod
    def _recalc(merged: dict[str, Any]) -> None:
        prev = merged.get("settlement_price") or merged.get("closing_price")
        ref  = merged.get("last_price") or merged.get("offer") or merged.get("bid")
        if prev and ref:
            try:
                merged["change_pct"] = (ref - prev) / prev * 100
            except ZeroDivisionError:
                merged["change_pct"] = None
        merged["prev_close"] = prev

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
