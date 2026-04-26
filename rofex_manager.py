"""Singleton manager para pyRofex: conexión, descubrimiento de instrumentos
y suscripción WebSocket. Mantiene estado en memoria para que Streamlit lea."""
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


# APIs externas (data912) para precios financieros que pyRofex no entrega:
# dólares MEP/CCL y panel BYMA (acciones, bonos, CEDEARs).
EXTERNAL_API_URLS = {
    "MEP": "https://data912.com/live/mep",
    "CCL": "https://data912.com/live/ccl",
    "ACCIONES": "https://data912.com/live/arg_stocks",
    "BONOS": "https://data912.com/live/arg_bonds",
    "CEDEARS": "https://data912.com/live/arg_cedears",
}
EXTERNAL_REFRESH_SECS = 5

logger = logging.getLogger("rofex_manager")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)


DOLLAR_PREFIXES = ("DLR/",)
GRAIN_PREFIXES = (
    "SOJ.",
    "MAI.",
    "TRI.",
    "SOR.",
    "GIR.",
    "CEB.",
)


def _classify(symbol: str) -> str | None:
    s = symbol.upper()
    if s.startswith(DOLLAR_PREFIXES):
        return "DOLAR"
    for p in GRAIN_PREFIXES:
        if s.startswith(p):
            return "GRANO"
    return None


class RofexManager:
    _instance: "RofexManager | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.initialized = False
        self.error: str | None = None
        self.symbols: list[str] = []
        self.instrument_meta: dict[str, dict[str, Any]] = {}
        self.market_data: dict[str, dict[str, Any]] = {}
        self.last_update: datetime | None = None
        self.snapshot_total = 0
        self.snapshot_done = 0
        self.snapshot_saved = 0
        self.snapshot_finished = False
        self.ws_subscribed = False
        # Datos de APIs externas (data912): MEP, CCL, acciones, bonos, CEDEARs
        self.external_data: dict[str, list[dict[str, Any]]] = {}
        self.external_last_update: datetime | None = None
        self.external_errors: dict[str, str] = {}
        self._md_lock = threading.Lock()

    @classmethod
    def get(cls) -> "RofexManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def initialize(self) -> None:
        if self.initialized:
            return
        user = (os.environ.get("PYROFEX_USER") or "").strip().strip('"').strip("'")
        password = (os.environ.get("PYROFEX_PASSWORD") or "").strip().strip('"').strip("'")
        account = (os.environ.get("PYROFEX_ACCOUNT") or "").strip().strip('"').strip("'")
        if not user or not password or not account:
            self.error = "Faltan credenciales PYROFEX_USER / PYROFEX_PASSWORD / PYROFEX_ACCOUNT"
            return

        masked_user = (user[:3] + "***" + user[-2:]) if len(user) > 5 else "***"
        logger.info(
            "Intentando conectar a REMARKETS... user=%s (len=%d), password_len=%d, account=%s",
            masked_user,
            len(user),
            len(password),
            account,
        )

        try:
            pyRofex.initialize(
                user=user,
                password=password,
                account=account,
                environment=pyRofex.Environment.REMARKET,
            )
        except Exception as e:
            self.error = (
                f"Fallo al inicializar pyRofex en REMARKETS: {e}\n"
                f"Verificá los Secrets PYROFEX_USER / PYROFEX_PASSWORD / PYROFEX_ACCOUNT en Replit. "
                f"Detalle: usuario detectado='{masked_user}' (largo={len(user)}), "
                f"largo password={len(password)}, cuenta='{account}'."
            )
            logger.exception("pyRofex.initialize falló")
            return

        logger.info("Conectado a REMARKETS OK")

        try:
            self._discover_instruments()
        except Exception as e:
            self.error = f"Fallo al descubrir instrumentos: {e}"
            logger.exception("get_all_instruments falló")
            return

        self.initialized = True
        logger.info("RofexManager listo (snapshot + WS arrancan en background)")

        threading.Thread(target=self._snapshot_then_subscribe, daemon=True).start()
        threading.Thread(target=self._external_polling_loop, daemon=True).start()

    def _external_polling_loop(self) -> None:
        """Loop infinito que cada N segundos consulta data912 (MEP, CCL,
        acciones, bonos, CEDEARs) y guarda el resultado en `external_data`."""
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
                    logger.warning("Fallo consultando %s: %s", key, e)
            time.sleep(EXTERNAL_REFRESH_SECS)

    def get_external(self, key: str) -> list[dict[str, Any]]:
        with self._md_lock:
            return list(self.external_data.get(key, []))

    def _snapshot_then_subscribe(self) -> None:
        """Primero snapshot REST de todos los símbolos (escribe a Supabase),
        después abre WebSocket. Si lo hacemos en paralelo el cliente REST se
        traba con el WS."""
        try:
            self._snapshot_initial_prices()
        except Exception:
            logger.exception("Snapshot inicial falló")

        try:
            pyRofex.init_websocket_connection(
                market_data_handler=self._on_market_data,
                error_handler=self._on_error,
                exception_handler=self._on_exception,
            )
            entries = [
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
            pyRofex.market_data_subscription(tickers=self.symbols, entries=entries)
            self.ws_subscribed = True
            logger.info("WebSocket conectado y suscripto a %d instrumentos", len(self.symbols))
        except Exception:
            logger.exception("Fallo al abrir/suscribir WebSocket")

    def _snapshot_initial_prices(self) -> None:
        """Pide precios actuales por REST para todos los símbolos y los guarda
        en Supabase + memoria. Útil cuando el mercado está cerrado y el WebSocket
        no envía updates."""
        self.snapshot_total = len(self.symbols)
        self.snapshot_done = 0
        self.snapshot_saved = 0
        self.snapshot_finished = False
        logger.info("Snapshot inicial: pidiendo market data REST de %d instrumentos", self.snapshot_total)
        entries = [
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
        for idx, symbol in enumerate(self.symbols, start=1):
            try:
                resp = pyRofex.get_market_data(ticker=symbol, entries=entries)
                if not isinstance(resp, dict) or resp.get("status") != "OK":
                    continue
                md = resp.get("marketData", {}) or {}
                last_price = self._extract_price(md, "LA")
                bid_price = self._extract_price(md, "BI")
                bid_size = self._extract_size(md, "BI")
                offer_price = self._extract_price(md, "OF")
                offer_size = self._extract_size(md, "OF")
                opening = self._extract_value(md, "OP")
                closing = self._extract_value(md, "CL")
                settlement = self._extract_value(md, "SE")
                trade_volume = self._extract_value(md, "TV")
                open_interest = self._extract_value(md, "OI")
                nominal_volume = self._extract_value(md, "NV")

                prev_close = settlement or closing
                ref = last_price or offer_price or bid_price
                change_pct = None
                if prev_close and ref:
                    try:
                        change_pct = (ref - prev_close) / prev_close * 100
                    except ZeroDivisionError:
                        change_pct = None

                ts = datetime.now(tz=timezone.utc).isoformat()
                merged = {
                    "symbol": symbol,
                    "ts": ts,
                    "last_price": last_price,
                    "bid": bid_price,
                    "bid_size": bid_size,
                    "offer": offer_price,
                    "offer_size": offer_size,
                    "opening_price": opening,
                    "closing_price": closing,
                    "settlement_price": settlement,
                    "trade_volume": trade_volume,
                    "open_interest": open_interest,
                    "nominal_volume": nominal_volume,
                    "prev_close": prev_close,
                    "change_pct": change_pct,
                }
                with self._md_lock:
                    self.market_data[symbol] = {**self.market_data.get(symbol, {}), **merged}
                    self.last_update = datetime.now(tz=timezone.utc)

                db.insert_tick(
                    {
                        "ts": ts,
                        "symbol": symbol,
                        "last_price": last_price,
                        "bid": bid_price,
                        "bid_size": bid_size,
                        "offer": offer_price,
                        "offer_size": offer_size,
                        "volume": trade_volume,
                        "open_interest": open_interest,
                        "settlement_price": settlement,
                        "prev_close": prev_close,
                        "change_pct": change_pct,
                    }
                )
                self.snapshot_saved += 1
            except Exception:
                logger.exception("Snapshot inicial: error con %s", symbol)
            finally:
                self.snapshot_done = idx
                if idx % 25 == 0 or idx == self.snapshot_total:
                    logger.info(
                        "Snapshot progreso: %d/%d (guardados=%d)",
                        idx, self.snapshot_total, self.snapshot_saved,
                    )
                time.sleep(0.05)
        self.snapshot_finished = True
        logger.info("Snapshot inicial completo: %d filas guardadas", self.snapshot_saved)

    def _discover_instruments(self) -> None:
        resp = pyRofex.get_all_instruments()
        instruments = resp.get("instruments", []) if isinstance(resp, dict) else []
        symbols: list[str] = []
        for inst in instruments:
            ident = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
            symbol = ident.get("symbol")
            if not symbol:
                continue
            category = _classify(symbol)
            if not category:
                continue
            symbols.append(symbol)
            self.instrument_meta[symbol] = {
                "symbol": symbol,
                "category": category,
                "underlying": symbol.split("/")[0],
                "market": ident.get("marketId"),
                "cficode": inst.get("cficode"),
            }
            db.upsert_instrument(
                {
                    "symbol": symbol,
                    "category": category,
                    "underlying": symbol.split("/")[0],
                }
            )
        symbols.sort()
        self.symbols = symbols
        logger.info(
            "Instrumentos detectados: %d (dolares=%d, granos=%d)",
            len(symbols),
            sum(1 for s in symbols if self.instrument_meta[s]["category"] == "DOLAR"),
            sum(1 for s in symbols if self.instrument_meta[s]["category"] == "GRANO"),
        )

    @staticmethod
    def _extract_price(entries: dict[str, Any], key: str) -> float | None:
        v = entries.get(key)
        if isinstance(v, dict):
            return v.get("price")
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, dict):
                return first.get("price")
        if isinstance(v, (int, float)):
            return float(v)
        return None

    @staticmethod
    def _extract_size(entries: dict[str, Any], key: str) -> float | None:
        v = entries.get(key)
        if isinstance(v, dict):
            return v.get("size")
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, dict):
                return first.get("size")
        return None

    @staticmethod
    def _extract_value(entries: dict[str, Any], key: str) -> float | None:
        v = entries.get(key)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, dict):
            return v.get("price") or v.get("size") or v.get("value")
        return None

    def _on_market_data(self, message: dict[str, Any]) -> None:
        try:
            instrument_id = message.get("instrumentId", {})
            symbol = instrument_id.get("symbol")
            if not symbol:
                return
            md = message.get("marketData", {}) or {}

            last_price = self._extract_price(md, "LA")
            bid_price = self._extract_price(md, "BI")
            bid_size = self._extract_size(md, "BI")
            offer_price = self._extract_price(md, "OF")
            offer_size = self._extract_size(md, "OF")
            opening = self._extract_value(md, "OP")
            closing = self._extract_value(md, "CL")
            settlement = self._extract_value(md, "SE")
            trade_volume = self._extract_value(md, "TV")
            open_interest = self._extract_value(md, "OI")
            nominal_volume = self._extract_value(md, "NV")

            with self._md_lock:
                cur = self.market_data.get(symbol, {})
                merged = {**cur}
                if last_price is not None:
                    merged["last_price"] = last_price
                if bid_price is not None:
                    merged["bid"] = bid_price
                    merged["bid_size"] = bid_size
                if offer_price is not None:
                    merged["offer"] = offer_price
                    merged["offer_size"] = offer_size
                if opening is not None:
                    merged["opening_price"] = opening
                if closing is not None:
                    merged["closing_price"] = closing
                if settlement is not None:
                    merged["settlement_price"] = settlement
                if trade_volume is not None:
                    merged["trade_volume"] = trade_volume
                if open_interest is not None:
                    merged["open_interest"] = open_interest
                if nominal_volume is not None:
                    merged["nominal_volume"] = nominal_volume

                prev_close = merged.get("settlement_price") or merged.get("closing_price")
                ref = merged.get("last_price") or merged.get("offer") or merged.get("bid")
                if prev_close and ref:
                    try:
                        merged["change_pct"] = (ref - prev_close) / prev_close * 100
                    except ZeroDivisionError:
                        merged["change_pct"] = None
                merged["prev_close"] = prev_close
                merged["ts"] = datetime.now(tz=timezone.utc).isoformat()
                merged["symbol"] = symbol
                self.market_data[symbol] = merged
                self.last_update = datetime.now(tz=timezone.utc)

            db.insert_tick(
                {
                    "ts": merged["ts"],
                    "symbol": symbol,
                    "last_price": merged.get("last_price"),
                    "bid": merged.get("bid"),
                    "bid_size": merged.get("bid_size"),
                    "offer": merged.get("offer"),
                    "offer_size": merged.get("offer_size"),
                    "volume": merged.get("trade_volume"),
                    "open_interest": merged.get("open_interest"),
                    "settlement_price": merged.get("settlement_price"),
                    "prev_close": merged.get("prev_close"),
                    "change_pct": merged.get("change_pct"),
                }
            )
        except Exception:
            logger.exception("Error procesando market data")

    def _on_error(self, message: Any) -> None:
        logger.error("WebSocket error: %s", message)

    def _on_exception(self, exc: Exception) -> None:
        logger.exception("WebSocket exception: %s", exc)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._md_lock:
            rows = []
            for symbol in self.symbols:
                meta = self.instrument_meta.get(symbol, {})
                data = self.market_data.get(symbol, {})
                rows.append({**meta, **data})
            return rows
