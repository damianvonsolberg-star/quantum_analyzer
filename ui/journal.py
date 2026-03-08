from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd


@dataclass
class PositionSnapshot:
    net_qty: float
    avg_entry: float
    realized_pnl: float
    unrealized_pnl: float
    market_value: float


def resolve_data_dir(explicit: str | None = None) -> Path:
    p = explicit or os.getenv("UI_DATA_DIR", "./ui_data")
    d = Path(p).expanduser().resolve()
    d.mkdir(parents=True, exist_ok=True)
    return d


def db_path(data_dir: str | None = None) -> Path:
    return resolve_data_dir(data_dir) / "journal.sqlite"


def init_db(data_dir: str | None = None) -> Path:
    path = db_path(data_dir)
    con = sqlite3.connect(path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              side TEXT NOT NULL,
              symbol TEXT NOT NULL,
              quantity REAL NOT NULL,
              price REAL NOT NULL,
              fees REAL NOT NULL DEFAULT 0,
              note TEXT DEFAULT '',
              advice_ref TEXT DEFAULT ''
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS advice_exec (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              advice_ref TEXT NOT NULL,
              note TEXT DEFAULT ''
            )
            """
        )
        con.commit()
    finally:
        con.close()
    return path


def record_fill(
    *,
    data_dir: str | None,
    ts: str,
    side: str,
    symbol: str,
    quantity: float,
    price: float,
    fees: float,
    note: str,
    advice_ref: str = "",
) -> None:
    init_db(data_dir)
    side_u = side.upper()
    if side_u not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")
    if quantity <= 0:
        raise ValueError("quantity must be > 0")
    if price <= 0:
        raise ValueError("price must be > 0")

    con = sqlite3.connect(db_path(data_dir))
    try:
        con.execute(
            "INSERT INTO fills (ts, side, symbol, quantity, price, fees, note, advice_ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ts, side_u, symbol, float(quantity), float(price), float(fees), note or "", advice_ref or ""),
        )
        con.commit()
    finally:
        con.close()


def mark_executed_advice(data_dir: str | None, advice_ref: str, note: str = "") -> None:
    init_db(data_dir)
    con = sqlite3.connect(db_path(data_dir))
    try:
        con.execute(
            "INSERT INTO advice_exec (ts, advice_ref, note) VALUES (?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), advice_ref or "", note or ""),
        )
        con.commit()
    finally:
        con.close()


def load_fills(data_dir: str | None, symbol: str | None = None) -> pd.DataFrame:
    init_db(data_dir)
    con = sqlite3.connect(db_path(data_dir))
    try:
        if symbol:
            df = pd.read_sql_query("SELECT * FROM fills WHERE symbol = ? ORDER BY ts, id", con, params=(symbol,))
        else:
            df = pd.read_sql_query("SELECT * FROM fills ORDER BY ts, id", con)
        return df
    finally:
        con.close()


def load_exec_log(data_dir: str | None) -> pd.DataFrame:
    init_db(data_dir)
    con = sqlite3.connect(db_path(data_dir))
    try:
        return pd.read_sql_query("SELECT * FROM advice_exec ORDER BY ts DESC, id DESC", con)
    finally:
        con.close()


def compute_position(fills: pd.DataFrame, current_price: float | None) -> PositionSnapshot:
    qty = 0.0
    avg = 0.0
    realized = 0.0

    if fills is None or fills.empty:
        cp = float(current_price or 0.0)
        return PositionSnapshot(net_qty=0.0, avg_entry=0.0, realized_pnl=0.0, unrealized_pnl=0.0, market_value=0.0 * cp)

    for _, r in fills.iterrows():
        side = str(r.get("side", "")).upper()
        q = float(r.get("quantity", 0.0) or 0.0)
        p = float(r.get("price", 0.0) or 0.0)
        fee = float(r.get("fees", 0.0) or 0.0)
        signed = q if side == "BUY" else -q

        # close opposite exposure first
        if qty > 0 and signed < 0:
            close = min(qty, -signed)
            realized += (p - avg) * close
        elif qty < 0 and signed > 0:
            close = min(-qty, signed)
            realized += (avg - p) * close

        new_qty = qty + signed

        # opening / increasing same-direction inventory
        if qty >= 0 and signed > 0:
            avg = ((avg * qty) + (p * signed)) / max(new_qty, 1e-12)
        elif qty <= 0 and signed < 0:
            abs_old = abs(qty)
            abs_new = abs(new_qty)
            avg = ((avg * abs_old) + (p * abs(signed))) / max(abs_new, 1e-12)
        elif qty > 0 and new_qty > 0 and signed < 0:
            # reduced long, avg unchanged
            pass
        elif qty < 0 and new_qty < 0 and signed > 0:
            # reduced short, avg unchanged
            pass
        else:
            # flipped through zero
            if new_qty == 0:
                avg = 0.0
            else:
                avg = p

        qty = new_qty
        realized -= fee

    cp = float(current_price or 0.0)
    unreal = (cp - avg) * qty if qty != 0 else 0.0
    mv = qty * cp
    return PositionSnapshot(net_qty=qty, avg_entry=avg, realized_pnl=realized, unrealized_pnl=unreal, market_value=mv)


def reconcile_wallet(position: PositionSnapshot, wallet_sol: float | None) -> dict[str, Any]:
    ws = float(wallet_sol or 0.0)
    diff = position.net_qty - ws
    threshold = max(0.01, 0.05 * max(abs(ws), 1.0))
    diverged = abs(diff) > threshold
    return {
        "wallet_sol": ws,
        "journal_sol": position.net_qty,
        "diff_sol": diff,
        "threshold": threshold,
        "diverged": diverged,
        "message": "Material divergence" if diverged else "Journal and wallet broadly aligned",
    }
