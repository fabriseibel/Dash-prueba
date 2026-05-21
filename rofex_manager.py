"""Singleton manager con doble conexión:
- VETA (REST polling cada 2s): DLR/ → datos en tiempo real
- ECO (REST polling cada 2s): granos (SOJ, MAI, TRI, etc.)
"""
from __future__ import annotations
import logging, os, threading, time
from datetime import datetime, timezone
from typing import Any
import pyRofex, requests
import db

EXTERNAL_API_URLS = {
    "MEP":"https://data912.com/live/mep","CCL":"https://data912.com/live/ccl",
    "ACCIONES":"https://data912.com/live/arg_stocks","BONOS":"https://data912.com/live/arg_bonds",
    "CEDEARS":"https://data912.com/live/arg_cedears",
    "MAYORISTA":"https://dolarapi.com/v1/dolares/mayorista",
}
EXTERNAL_REFRESH_SECS = 5
VETA_API    = "https://api.veta.xoms.com.ar"
VETA_MARKET = "ROFX"
VETA_POLL_SECS = 2
ECO_API    = "https://api.eco.xoms.com.ar"
ECO_MARKET = "ROFX"
ECO_POLL_SECS = 2

logger = logging.getLogger("rofex_manager")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(h)

DOLLAR_PREFIXES = ("DLR/",)
GRAIN_PREFIXES  = ("SOJ.","MAI.","TRI.","SOR.","GIR.","CEB.")

def _classify(symbol):
    s = symbol.upper()
    if s.startswith(DOLLAR_PREFIXES): return "DOLAR"
    for p in GRAIN_PREFIXES:
        if s.startswith(p): return "GRANO"
    return None

_MD_ENTRIES_VETA = "LA,BI,OF,OP,CL,SE,TV,OI,NV"
_MD_ENTRIES_ECO  = "LA,BI,OF,OP,CL,SE,TV,OI,NV"

class RofexManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.initialized = False
        self.error = None
        self.symbols_veta = []
        self.symbols_eco  = []
        self.symbols = []
        self.instrument_meta = {}
        self.market_data = {}
        self.last_update = None
        self.snapshot_total = 0
        self.snapshot_done  = 0
        self.snapshot_saved = 0
        self.snapshot_finished_veta = False
        self.snapshot_finished_eco  = False
        self.ws_subscribed_veta = False
        self.ws_subscribed_remarkets = False  # alias para compatibilidad con app.py
        self._veta_token = None
        self._veta_user  = ""
        self._veta_password = ""
        self._eco_token  = None
        self._eco_user   = ""
        self._eco_password = ""
        self.external_data = {}
        self.external_last_update = None
        self.external_errors = {}
        self._md_lock = threading.Lock()

    @property
    def ws_subscribed(self):
        return self.ws_subscribed_veta or self.ws_subscribed_remarkets

    @property
    def symbols_remarkets(self):
        return self.symbols_eco

    @property
    def ws_subscribed_eco(self):
        return self.ws_subscribed_remarkets

    @ws_subscribed_eco.setter
    def ws_subscribed_eco(self, v):
        self.ws_subscribed_remarkets = v

    @property
    def snapshot_finished(self):
        return (self.snapshot_finished_veta or not self.symbols_veta) and \
               (self.snapshot_finished_eco  or not self.symbols_eco)

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def initialize(self):
        if self.initialized: return

        veta_user     = _env("VETA_USER")
        veta_password = _env("VETA_PASSWORD")
        eco_user      = _env("ECO_USER")
        eco_password  = _env("ECO_PASSWORD")
        eco_account   = _env("ECO_ACCOUNT") or "659390"

        veta_ok = bool(veta_user and veta_password)
        eco_ok  = bool(eco_user and eco_password)

        if not veta_ok and not eco_ok:
            self.error = "Faltan credenciales VETA o ECO"; return

        if veta_ok:
            self._veta_user = veta_user; self._veta_password = veta_password
            token = self._login(VETA_API, veta_user, veta_password)
            if token:
                self._veta_token = token
                self._discover(VETA_API, self._veta_token, "DOLAR", self.symbols_veta)
            else:
                self.error = "Veta: no se pudo obtener token"

        if eco_ok:
            self._eco_user = eco_user; self._eco_password = eco_password
            token = self._login(ECO_API, eco_user, eco_password)
            if token:
                self._eco_token = token
                self._discover(ECO_API, self._eco_token, "GRANO", self.symbols_eco)
            else:
                err = "Eco: no se pudo obtener token"
                self.error = (self.error + " | " + err) if self.error else err

        self.symbols = sorted(set(self.symbols_veta + self.symbols_eco))
        self.initialized = True
        logger.info("RofexManager listo — Veta DLR: %d, Eco granos: %d",
                    len(self.symbols_veta), len(self.symbols_eco))

        if veta_ok and self._veta_token and self.symbols_veta:
            threading.Thread(target=self._veta_run, daemon=True).start()
        if eco_ok and self._eco_token and self.symbols_eco:
            threading.Thread(target=self._eco_run, daemon=True).start()
        threading.Thread(target=self._external_polling_loop, daemon=True).start()

    def _login(self, api_url, user, password):
        try:
            resp = requests.post(f"{api_url}/auth/getToken",
                headers={"X-Username": user, "X-Password": password}, timeout=10)
            token = resp.headers.get("X-Auth-Token")
            if token: logger.info("Login OK: %s", api_url); return token
            logger.error("No token desde %s. Status=%s", api_url, resp.status_code)
            return None
        except Exception as e:
            logger.exception("Login falló %s: %s", api_url, e); return None

    def _discover(self, api_url, token, category_filter, symbol_list):
        try:
            resp = requests.get(f"{api_url}/rest/instruments/all",
                headers={"X-Auth-Token": token},
                params={"marketId": VETA_MARKET}, timeout=15)
            data = resp.json()
            for inst in (data.get("instruments", []) if isinstance(data, dict) else []):
                iid = inst.get("instrumentId", {}) if isinstance(inst, dict) else {}
                symbol = iid.get("symbol")
                if not symbol or _classify(symbol) != category_filter: continue
                self.instrument_meta[symbol] = {
                    "symbol": symbol, "category": category_filter,
                    "underlying": symbol.split("/")[0] if category_filter == "GRANO" else "DLR",
                    "source": "VETA" if category_filter == "DOLAR" else "ECO",
                }
                db.upsert_instrument({"symbol": symbol, "category": category_filter,
                    "underlying": self.instrument_meta[symbol]["underlying"]})
                symbol_list.append(symbol)
            symbol_list[:] = sorted(set(symbol_list))
            logger.info("Descubiertos %s (%s): %d", category_filter, api_url, len(symbol_list))
        except Exception as e:
            logger.exception("Discover falló %s: %s", api_url, e)

    def _fetch_one(self, api_url, token, symbol):
        resp = requests.get(f"{api_url}/rest/marketdata/get",
            headers={"X-Auth-Token": token},
            params={"symbol": symbol, "marketId": VETA_MARKET, "entries": _MD_ENTRIES_VETA},
            timeout=4)
        if resp.status_code == 401:
            logger.warning("Token expirado %s — renovando...", api_url)
            is_veta = api_url == VETA_API
            new = self._login(api_url,
                self._veta_user if is_veta else self._eco_user,
                self._veta_password if is_veta else self._eco_password)
            if new:
                if is_veta: self._veta_token = new
                else: self._eco_token = new
                token_to_use = new
                resp = requests.get(f"{api_url}/rest/marketdata/get",
                    headers={"X-Auth-Token": token_to_use},
                    params={"symbol": symbol, "marketId": VETA_MARKET, "entries": _MD_ENTRIES_VETA},
                    timeout=4)
            else: return None
        if resp.status_code != 200: return None
        data = resp.json()
        if data.get("status") != "OK": return None
        return self._parse_md(symbol, data.get("marketData") or {})

    def _run_polling(self, api_url, token_getter, symbols, finished_attr, subscribed_attr, poll_secs):
        self.snapshot_total += len(symbols)
        for symbol in symbols:
            try:
                token = token_getter()
                md = self._fetch_one(api_url, token, symbol)
                if md:
                    with self._md_lock:
                        self.market_data[symbol] = {**self.market_data.get(symbol, {}), **md}
                        self.last_update = datetime.now(tz=timezone.utc)
                    db.insert_tick(self._tick_row(md)); self.snapshot_saved += 1
            except Exception: pass
            finally: self.snapshot_done += 1; time.sleep(0.1)
        setattr(self, finished_attr, True)
        logger.info("Snapshot completo: %s", api_url)

        while True:
            for symbol in symbols:
                try:
                    token = token_getter()
                    md = self._fetch_one(api_url, token, symbol)
                    if md:
                        with self._md_lock:
                            merged = {**self.market_data.get(symbol, {})}
                            for k, v in md.items():
                                if v is not None: merged[k] = v
                            self._recalc(merged)
                            merged["ts"] = datetime.now(tz=timezone.utc).isoformat()
                            merged["symbol"] = symbol
                            self.market_data[symbol] = merged
                            self.last_update = datetime.now(tz=timezone.utc)
                        setattr(self, subscribed_attr, True)
                except Exception: pass
            time.sleep(poll_secs)

    def _veta_run(self):
        threading.Thread(target=self._token_refresh_loop,
            args=(VETA_API, lambda: self._veta_user, lambda: self._veta_password,
                  lambda t: setattr(self, '_veta_token', t)), daemon=True).start()
        self._run_polling(VETA_API, lambda: self._veta_token,
            self.symbols_veta, 'snapshot_finished_veta', 'ws_subscribed_veta', VETA_POLL_SECS)

    def _eco_run(self):
        threading.Thread(target=self._token_refresh_loop,
            args=(ECO_API, lambda: self._eco_user, lambda: self._eco_password,
                  lambda t: setattr(self, '_eco_token', t)), daemon=True).start()
        self._run_polling(ECO_API, lambda: self._eco_token,
            self.symbols_eco, 'snapshot_finished_eco', 'ws_subscribed_remarkets', ECO_POLL_SECS)

    def _token_refresh_loop(self, api_url, user_getter, pass_getter, token_setter):
        while True:
            time.sleep(23 * 3600)
            new = self._login(api_url, user_getter(), pass_getter())
            if new: token_setter(new); logger.info("Token renovado: %s", api_url)

    def _external_polling_loop(self):
        while True:
            for key, url in EXTERNAL_API_URLS.items():
                try:
                    resp = requests.get(url, timeout=8); resp.raise_for_status()
                    data = resp.json()
                    if not isinstance(data, list): data = []
                    with self._md_lock:
                        self.external_data[key] = data
                        self.external_last_update = datetime.now(tz=timezone.utc)
                        self.external_errors.pop(key, None)
                except Exception as e:
                    with self._md_lock: self.external_errors[key] = str(e)
            time.sleep(EXTERNAL_REFRESH_SECS)

    def get_external(self, key):
        with self._md_lock: return list(self.external_data.get(key, []))

    def _parse_md(self, symbol, md):
        def _v(key):
            obj = md.get(key)
            if obj is None: return None
            if isinstance(obj, (int, float)): return float(obj)
            if isinstance(obj, dict): return obj.get("price") or obj.get("size")
            if isinstance(obj, list) and obj:
                f = obj[0]
                if isinstance(f, dict): return f.get("price")
            return None
        last = _v("LA"); bid = _v("BI"); offer = _v("OF")
        settlement = _v("SE"); closing = _v("CL")
        trade_vol = _v("TV"); open_int = _v("OI")
        prev_close = settlement or closing
        ref = last or offer or bid
        change_pct = None
        if prev_close and ref:
            try: change_pct = (ref - prev_close) / prev_close * 100
            except: pass
        return {"symbol": symbol, "ts": datetime.now(tz=timezone.utc).isoformat(),
            "last_price": last, "bid": bid, "offer": offer,
            "settlement_price": settlement, "closing_price": closing,
            "trade_volume": trade_vol, "open_interest": open_int,
            "prev_close": prev_close, "change_pct": change_pct}

    @staticmethod
    def _recalc(merged):
        prev = merged.get("settlement_price") or merged.get("closing_price")
        ref  = merged.get("last_price") or merged.get("offer") or merged.get("bid")
        if prev and ref:
            try: merged["change_pct"] = (ref - prev) / prev * 100
            except: merged["change_pct"] = None
        merged["prev_close"] = prev

    @staticmethod
    def _tick_row(r):
        return {"ts": r.get("ts"), "symbol": r.get("symbol"),
            "last_price": r.get("last_price"), "bid": r.get("bid"),
            "offer": r.get("offer"), "volume": r.get("trade_volume"),
            "open_interest": r.get("open_interest"),
            "settlement_price": r.get("settlement_price"),
            "prev_close": r.get("prev_close"), "change_pct": r.get("change_pct")}

    def snapshot(self):
        with self._md_lock:
            return [{**self.instrument_meta.get(s, {}), **self.market_data.get(s, {})}
                    for s in self.symbols]

def _env(key): return (os.environ.get(key) or "").strip().strip('"').strip("'")
