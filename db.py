"""Supabase client + helpers para persistir ticks e instrumentos."""
from __future__ import annotations

import os
from typing import Any

from supabase import Client, create_client


def _clean(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip().strip('"').strip("'")
    return v or None


_init_error: str | None = None


def _client() -> Client | None:
    global _init_error
    url = _clean(os.environ.get("SUPABASE_URL"))
    key = _clean(os.environ.get("SUPABASE_KEY"))
    if not url or not key:
        _init_error = "Faltan SUPABASE_URL o SUPABASE_KEY"
        return None
    if "supabase." not in url and not url.startswith(("http://", "https://")):
        _init_error = (
            f"SUPABASE_URL no parece una URL válida ({url[:30]}...). "
            "Probable swap con SUPABASE_KEY en los Secrets."
        )
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        return create_client(url, key)
    except Exception as e:
        _init_error = f"create_client falló: {e}"
        return None


_supabase: Client | None = _client()

_insert_errors = 0
_insert_ok = 0


def is_connected() -> bool:
    return _supabase is not None


def init_error() -> str | None:
    return _init_error


def stats() -> dict[str, int]:
    return {"ok": _insert_ok, "errors": _insert_errors}


def insert_tick(row: dict[str, Any]) -> None:
    global _insert_ok, _insert_errors
    if _supabase is None:
        return
    try:
        _supabase.table("ticks").insert(row).execute()
        _insert_ok += 1
    except Exception as e:
        _insert_errors += 1
        if _insert_errors <= 3:
            print(f"[db] insert_tick falló ({e})")


def upsert_instrument(row: dict[str, Any]) -> None:
    if _supabase is None:
        return
    try:
        _supabase.table("instruments").upsert(row, on_conflict="symbol").execute()
    except Exception as e:
        print(f"[db] upsert_instrument falló ({e})")


def fetch_recent_ticks(symbol: str, limit: int = 100) -> list[dict[str, Any]]:
    if _supabase is None:
        return []
    try:
        res = (
            _supabase.table("ticks")
            .select("*")
            .eq("symbol", symbol)
            .order("ts", desc=True)
            .limit(limit)
            .execute()
        )
        return list(res.data or [])
    except Exception:
        return []
