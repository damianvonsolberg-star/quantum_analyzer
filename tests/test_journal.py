from __future__ import annotations

from pathlib import Path

from ui.journal import compute_position, load_exec_log, load_fills, mark_executed_advice, record_fill, reconcile_wallet


def test_journal_record_and_position(tmp_path: Path):
    data_dir = str(tmp_path)

    record_fill(
        data_dir=data_dir,
        ts="2026-03-08T08:00:00Z",
        side="BUY",
        symbol="SOLUSDT",
        quantity=2.0,
        price=100.0,
        fees=0.2,
        note="entry",
    )
    record_fill(
        data_dir=data_dir,
        ts="2026-03-08T09:00:00Z",
        side="SELL",
        symbol="SOLUSDT",
        quantity=1.0,
        price=110.0,
        fees=0.1,
        note="trim",
    )

    fills = load_fills(data_dir)
    assert len(fills) == 2

    pos = compute_position(fills, current_price=120.0)
    assert round(pos.net_qty, 6) == 1.0
    assert round(pos.avg_entry, 6) == 100.0
    # realized: (110-100)*1 - fees(0.3)
    assert abs(pos.realized_pnl - 9.7) < 1e-6
    # unrealized: (120-100)*1
    assert abs(pos.unrealized_pnl - 20.0) < 1e-6


def test_reconcile_wallet_divergence():
    class P:
        net_qty = 5.0

    r = reconcile_wallet(P(), wallet_sol=4.0)
    assert r["diverged"] is True


def test_mark_executed_advice(tmp_path: Path):
    data_dir = str(tmp_path)
    mark_executed_advice(data_dir, advice_ref="2026-03-08T09:00:00Z", note="manual")
    log = load_exec_log(data_dir)
    assert len(log) == 1
    assert "advice_ref" in log.columns
