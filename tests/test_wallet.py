from __future__ import annotations

from types import SimpleNamespace

import pytest

from ui.portfolio import advice_from_target, build_portfolio_snapshot
from ui.wallet import WalletBalances, fetch_solusdt_price, fetch_wallet_balances


class _Resp:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_fetch_wallet_balances_and_cache(monkeypatch):
    calls = {"n": 0}

    def fake_post(url, json, timeout):  # noqa: A002
        calls["n"] += 1
        if json["method"] == "getBalance":
            return _Resp({"result": {"value": 2_000_000_000}})
        return _Resp(
            {
                "result": {
                    "value": [
                        {
                            "account": {
                                "data": {
                                    "parsed": {
                                        "info": {"tokenAmount": {"uiAmount": 123.45}}
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    w1 = fetch_wallet_balances("https://rpc.example", "wallet123", ttl_s=60)
    w2 = fetch_wallet_balances("https://rpc.example", "wallet123", ttl_s=60)
    assert w1.sol == pytest.approx(2.0)
    assert w1.usdc == pytest.approx(123.45)
    assert w2.usdc == pytest.approx(123.45)
    # 2 calls only (balance + token accounts), second request served from cache
    assert calls["n"] == 2


def test_fetch_sol_price(monkeypatch):
    def fake_get(url, timeout):
        return _Resp({"price": "150.25"})

    monkeypatch.setattr("requests.get", fake_get)
    px = fetch_solusdt_price(ttl_s=0)
    assert px == pytest.approx(150.25)


def test_wallet_delta_output_is_explicit_for_spot_mode():
    bal = WalletBalances(wallet="abc", sol=10.0, usdc=500.0)
    snap = build_portfolio_snapshot(bal, sol_price_usd=100.0)
    assert snap.sol_mtm_usd == pytest.approx(1000.0)
    assert snap.total_nav_usd == pytest.approx(1500.0)

    adv = advice_from_target(snap, target_position=0.5, advisory_mode="spot_only", target_scope="advisory_sleeve")
    assert adv.spot_implementable_target_weight == pytest.approx(0.5)
    assert adv.recommended_delta_usd == pytest.approx(-250.0)
    assert adv.recommended_delta_sol == pytest.approx(-2.5)
    assert adv.action_label == "BUY SPOT"


def test_negative_target_position_spot_warning():
    bal = WalletBalances(wallet="abc", sol=10.0, usdc=500.0)
    snap = build_portfolio_snapshot(bal, sol_price_usd=100.0)
    adv = advice_from_target(snap, target_position=-0.4, advisory_mode="spot_only", target_scope="whole_wallet")
    assert adv.unsupported_in_spot is True
    assert adv.spot_implementable_target_weight == 0.0
    assert any("Not implementable in spot-only workflow" in w for w in adv.warnings)


def test_spot_mode_does_not_silently_clip_without_notice():
    bal = WalletBalances(wallet="abc", sol=0.0, usdc=1500.0)
    snap = build_portfolio_snapshot(bal, sol_price_usd=100.0)
    adv = advice_from_target(snap, target_position=-0.1, advisory_mode="spot_only")
    assert adv.model_target_position == pytest.approx(-0.1)
    assert adv.spot_implementable_target_weight == 0.0
    assert adv.warnings
