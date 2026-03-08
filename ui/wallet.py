from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

import requests

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


@dataclass
class WalletBalances:
    wallet: str
    sol: float
    usdc: float


class WalletFetchError(RuntimeError):
    pass


_CACHE: dict[str, tuple[float, Any]] = {}


def _cached_get(key: str) -> Any | None:
    hit = _CACHE.get(key)
    if not hit:
        return None
    exp, val = hit
    if time.time() > exp:
        _CACHE.pop(key, None)
        return None
    return val


def _cached_set(key: str, value: Any, ttl_s: int) -> None:
    _CACHE[key] = (time.time() + ttl_s, value)


def rpc_call(rpc_url: str, method: str, params: list[Any], timeout_s: int = 12) -> dict[str, Any]:
    try:
        r = requests.post(rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=timeout_s)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise WalletFetchError(f"RPC error: {data['error']}")
        return data
    except requests.RequestException as e:
        raise WalletFetchError(f"RPC request failed: {e}") from e


def fetch_wallet_balances(rpc_url: str, wallet: str, timeout_s: int = 12, ttl_s: int = 15) -> WalletBalances:
    if not wallet:
        raise WalletFetchError("Wallet address is empty")
    key = f"wallet:{rpc_url}:{wallet}"
    cached = _cached_get(key)
    if cached is not None:
        return cached

    bal = rpc_call(rpc_url, "getBalance", [wallet, {"commitment": "processed"}], timeout_s=timeout_s)
    toks = rpc_call(
        rpc_url,
        "getTokenAccountsByOwner",
        [wallet, {"mint": USDC_MINT}, {"encoding": "jsonParsed", "commitment": "processed"}],
        timeout_s=timeout_s,
    )

    sol = float(bal.get("result", {}).get("value", 0.0)) / 1e9
    usdc = 0.0
    for acc in toks.get("result", {}).get("value", []):
        amt = (
            acc.get("account", {})
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
            .get("tokenAmount", {})
            .get("uiAmount", 0.0)
        )
        usdc += float(amt or 0.0)

    out = WalletBalances(wallet=wallet, sol=sol, usdc=usdc)
    _cached_set(key, out, ttl_s)
    return out


def fetch_sol_price_helius(rpc_url: str, timeout_s: int = 8, ttl_s: int = 5) -> tuple[float, float | None]:
    """Fetch SOL price (+ optional 24h change %) from Helius DAS getAsset.

    Uses wrapped SOL mint id and returns (price_usd, change_24h_pct|None).
    """
    key = f"price:helius:{rpc_url}"
    cached = _cached_get(key)
    if cached is not None:
        return float(cached[0]), (float(cached[1]) if cached[1] is not None else None)

    payload = {
        "jsonrpc": "2.0",
        "id": "sol-price",
        "method": "getAsset",
        "params": {"id": "So11111111111111111111111111111111111111112"},
    }
    try:
        r = requests.post(rpc_url, json=payload, timeout=timeout_s)
        r.raise_for_status()
        data = r.json().get("result", {})
        token_info = data.get("token_info", {}) if isinstance(data, dict) else {}
        price_info = token_info.get("price_info", {}) if isinstance(token_info, dict) else {}

        price = price_info.get("price_per_token", None)
        if price is None:
            # some providers expose "price"
            price = price_info.get("price", None)
        if price is None:
            raise WalletFetchError("HELIUS price_info missing")

        chg = (
            price_info.get("price_change_24h")
            if isinstance(price_info, dict)
            else None
        )
        if chg is None and isinstance(price_info, dict):
            chg = price_info.get("price_change_pct_24h", None)

        out = (float(price), (float(chg) if chg is not None else None))
    except (requests.RequestException, KeyError, ValueError, TypeError) as e:
        raise WalletFetchError(f"Failed to fetch SOL price from Helius: {e}") from e

    _cached_set(key, out, ttl_s)
    return out


def fetch_solusdt_price(timeout_s: int = 8, ttl_s: int = 8) -> float:
    key = "price:binance:SOLUSDT"
    cached = _cached_get(key)
    if cached is not None:
        return float(cached)
    url = "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT"
    try:
        r = requests.get(url, timeout=timeout_s)
        r.raise_for_status()
        price = float(r.json()["price"])
    except (requests.RequestException, KeyError, ValueError) as e:
        raise WalletFetchError(f"Failed to fetch SOLUSDT price: {e}") from e
    _cached_set(key, price, ttl_s)
    return price


def fetch_solusdt_24h_change_pct(timeout_s: int = 8, ttl_s: int = 15) -> float:
    key = "price:binance:SOLUSDT:24h"
    cached = _cached_get(key)
    if cached is not None:
        return float(cached)
    url = "https://api.binance.com/api/v3/ticker/24hr?symbol=SOLUSDT"
    try:
        r = requests.get(url, timeout=timeout_s)
        r.raise_for_status()
        chg = float(r.json().get("priceChangePercent"))
    except (requests.RequestException, KeyError, ValueError, TypeError) as e:
        raise WalletFetchError(f"Failed to fetch SOLUSDT 24h change: {e}") from e
    _cached_set(key, chg, ttl_s)
    return chg


def resolve_rpc_url(explicit_rpc_url: str | None = None) -> str:
    return explicit_rpc_url or os.getenv("SOL_RPC_URL", "https://api.mainnet-beta.solana.com")


def resolve_wallet(explicit_wallet: str | None = None) -> str:
    return explicit_wallet or os.getenv("BENCHMARK_WALLET", "")
