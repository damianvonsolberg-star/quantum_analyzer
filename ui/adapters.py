from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

import pandas as pd
import requests

from quantum_analyzer.monitoring.governance import DriftThresholds, evaluate_governance
from ui.contracts import ARTIFACT_SCHEMA_V2, REQUIRED_BUNDLE_SECTIONS_V2
from ui.view_models import (
    UiBacktestSummary,
    UiDriftStatus,
    UiForecastView,
    UiLiveAdvice,
    UiPathTemplate,
    UiPortfolioSnapshot,
)

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class AdapterValidationError(ValueError):
    pass


class ArtifactAdapter:
    def __init__(self, artifact_dir: str) -> None:
        self.base = Path(artifact_dir)

    def paths(self) -> dict[str, Path]:
        return {
            "bundle": self.base / "artifact_bundle.json",
            "summary": self.base / "summary.json",
            "equity": self.base / "equity_curve.csv",
            "actions": self.base / "actions.csv",
            "templates_json": self.base / "templates.json",
            "templates_parquet": self.base / "templates.parquet",
            "doctor": self.base / "doctor_report.json",
        }

    def required_missing(self) -> list[str]:
        p = self.paths()
        required = {"bundle", "summary", "equity", "actions"}
        return [k for k in required if not p[k].exists()]

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | list[Any] | None:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def _read_csv(path: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    def _read_templates(self) -> pd.DataFrame:
        p = self.paths()
        raw = self._read_json(p["templates_json"])
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if p["templates_parquet"].exists():
            try:
                return pd.read_parquet(p["templates_parquet"])
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()

    def load_raw(self) -> dict[str, Any]:
        p = self.paths()
        return {
            "bundle": self._read_json(p["bundle"]),
            "summary": self._read_json(p["summary"]),
            "equity": self._read_csv(p["equity"]),
            "actions": self._read_csv(p["actions"]),
            "templates": self._read_templates(),
            "doctor": self._read_json(p["doctor"]),
        }

    def schema_versions(self) -> list[str]:
        raw = self.load_raw()
        vals: set[str] = set()

        def scan(x: Any) -> None:
            if isinstance(x, dict):
                for k, v in x.items():
                    if k == "schema_version" and isinstance(v, str):
                        vals.add(v)
                    else:
                        scan(v)
            elif isinstance(x, list):
                for z in x:
                    scan(z)

        scan(raw.get("bundle"))
        scan(raw.get("summary"))
        return sorted(vals)

    def _bundle_v2(self) -> dict[str, Any] | None:
        b = self.load_raw().get("bundle")
        if not isinstance(b, dict):
            return None
        if b.get("schema_version") != ARTIFACT_SCHEMA_V2:
            return None
        missing = [k for k in REQUIRED_BUNDLE_SECTIONS_V2 if k not in b]
        if missing:
            raise AdapterValidationError(f"schema v2 bundle missing required sections: {', '.join(missing)}")
        return b

    def _bundle_legacy(self) -> dict[str, Any] | None:
        b = self.load_raw().get("bundle")
        return b if isinstance(b, dict) else None

    @staticmethod
    def _extract_horizons(bundle: dict[str, Any] | None) -> list[str]:
        if not isinstance(bundle, dict):
            return []
        found: set[str] = set()

        def scan(x: Any) -> None:
            if isinstance(x, dict):
                for k, v in x.items():
                    if re.match(r"^h\d+$", str(k)):
                        found.add(str(k))
                    scan(v)
            elif isinstance(x, list):
                for z in x:
                    scan(z)

        scan(bundle)
        return sorted(found)

    @staticmethod
    def _pick_float(row: pd.Series, names: list[str], default: float | None = None) -> float | None:
        for n in names:
            if n in row.index and pd.notna(row[n]):
                try:
                    return float(row[n])
                except Exception:
                    continue
        return default

    @staticmethod
    def _pick_str(row: pd.Series, names: list[str], default: str | None = None) -> str | None:
        for n in names:
            if n in row.index and pd.notna(row[n]):
                return str(row[n])
        return default

    def to_forecast_view(self) -> UiForecastView:
        bundle_v2 = self._bundle_v2()
        if bundle_v2 is not None:
            forecast = bundle_v2["forecast"]
            entropy = forecast.get("entropy") if isinstance(forecast.get("entropy"), (float, int)) else None
            confidence = forecast.get("confidence") if isinstance(forecast.get("confidence"), (float, int)) else None
            calib = forecast.get("calibration_score") if isinstance(forecast.get("calibration_score"), (float, int)) else None
            return UiForecastView(
                horizons=self._extract_horizons({"forecast": forecast}),
                entropy=float(entropy) if entropy is not None else None,
                confidence=float(confidence) if confidence is not None else None,
                calibration_score=float(calib) if calib is not None else None,
                raw=forecast,
            )

        bundle = self._bundle_legacy()
        if not isinstance(bundle, dict):
            return UiForecastView()
        forecast = bundle.get("forecast", {}) if isinstance(bundle.get("forecast"), dict) else {}
        entropy = forecast.get("entropy") if isinstance(forecast.get("entropy"), (float, int)) else None
        confidence = forecast.get("confidence") if isinstance(forecast.get("confidence"), (float, int)) else None
        return UiForecastView(
            horizons=self._extract_horizons(bundle),
            entropy=float(entropy) if entropy is not None else None,
            confidence=float(confidence) if confidence is not None else None,
            calibration_score=None,
            raw=forecast if isinstance(forecast, dict) else {},
        )

    def to_live_advice(self) -> UiLiveAdvice:
        bundle_v2 = self._bundle_v2()
        if bundle_v2 is not None:
            p = bundle_v2["proposal"]
            f = bundle_v2["forecast"]
            required = ["timestamp", "action", "target_position", "expected_edge_bps", "expected_cost_bps"]
            missing = [k for k in required if k not in p]
            if missing:
                raise AdapterValidationError(f"schema v2 proposal missing required fields: {', '.join(missing)}")

            timestamp = str(p.get("timestamp") or "")
            # guard against malformed numeric timestamps from intermediate artifacts
            ts_ok = False
            if timestamp:
                try:
                    _ = pd.to_datetime(timestamp, utc=True, errors="raise")
                    ts_ok = True
                except Exception:
                    ts_ok = False
            if not ts_ok:
                meta_ts = (bundle_v2.get("artifact_meta", {}) or {}).get("produced_at")
                drift_ts = (bundle_v2.get("drift", {}).get("timestamps", {}) or {}).get("as_of") if isinstance(bundle_v2.get("drift"), dict) else None
                cand = str(meta_ts or drift_ts or "")
                try:
                    _ = pd.to_datetime(cand, utc=True, errors="raise")
                    timestamp = cand
                    ts_ok = True
                except Exception:
                    ts_ok = False
            if not ts_ok:
                raise AdapterValidationError("schema v2 proposal timestamp is invalid")
            headline_action = str(p["action"])
            target_position = float(p["target_position"])
            edge = float(p["expected_edge_bps"])
            cost = float(p["expected_cost_bps"])
            confidence = float(f["confidence"]) if isinstance(f.get("confidence"), (float, int)) else None
            entropy = float(f["entropy"]) if isinstance(f.get("entropy"), (float, int)) else None
            reason = str(p.get("reason") or "")
            reasons = p.get("reasons") if isinstance(p.get("reasons"), list) else ([reason] if reason else [])

            if edge > cost and abs(target_position) > 0:
                traffic = "green"
            elif edge > cost * 0.8:
                traffic = "yellow"
            else:
                traffic = "red"

            return UiLiveAdvice(
                timestamp=timestamp,
                headline_action=headline_action,
                traffic_light=traffic,
                target_position=target_position,
                expected_edge_bps=edge,
                expected_cost_bps=cost,
                confidence=confidence,
                entropy=entropy,
                risk_note=reason,
                reasons=reasons,
                advisory_mode=str(p.get("advisory_mode", "spot_only")),
                target_scope=str(p.get("target_scope", "advisory_sleeve")),
                top_alternatives=(p.get("controls", {}) or {}).get("top_alternatives", []),
                invalidation_notes=(p.get("controls", {}) or {}).get("invalidation_reasons", []),
            )

        # legacy fallback path
        raw = self.load_raw()
        actions = raw.get("actions")
        if not isinstance(actions, pd.DataFrame) or actions.empty:
            raise AdapterValidationError("actions.csv missing or empty")

        required = ["action", "target_position", "expected_edge_bps", "expected_cost_bps"]
        missing = [c for c in required if c not in actions.columns]
        if missing:
            raise AdapterValidationError(f"actions.csv missing required fields: {', '.join(missing)}")

        row = actions.iloc[-1]
        timestamp = self._pick_str(row, ["ts", "timestamp"], default="")
        if not timestamp:
            raise AdapterValidationError("actions.csv missing required field: ts/timestamp")

        headline_action = str(row["action"])
        target_position = float(row["target_position"])
        edge = float(row["expected_edge_bps"])
        cost = float(row["expected_cost_bps"])

        forecast_vm = self.to_forecast_view()
        confidence = self._pick_float(row, ["confidence", "signal_confidence"], default=forecast_vm.confidence)
        entropy = self._pick_float(row, ["entropy"], default=forecast_vm.entropy)

        reason = self._pick_str(row, ["reason", "risk_note"], default="") or ""
        reasons_raw = self._pick_str(row, ["reasons"], default="") or ""
        reasons = [x.strip() for x in reasons_raw.split("|") if x.strip()] if reasons_raw else ([reason] if reason else [])

        if edge > cost and abs(target_position) > 0:
            traffic = "green"
        elif edge > cost * 0.8:
            traffic = "yellow"
        else:
            traffic = "red"

        return UiLiveAdvice(
            timestamp=timestamp,
            headline_action=headline_action,
            traffic_light=traffic,
            target_position=target_position,
            expected_edge_bps=edge,
            expected_cost_bps=cost,
            confidence=confidence,
            entropy=entropy,
            risk_note=reason,
            reasons=reasons,
            advisory_mode="spot_only",
            target_scope="advisory_sleeve",
        )

    def to_backtest_summary(self) -> UiBacktestSummary:
        bundle_v2 = self._bundle_v2()
        if bundle_v2 is not None:
            summary = bundle_v2["summary"]
        else:
            summary = self.load_raw().get("summary")
            if not isinstance(summary, dict):
                raise AdapterValidationError("summary.json missing or invalid")

        diag = summary.get("diagnostics", {}) if isinstance(summary.get("diagnostics"), dict) else {}
        return UiBacktestSummary(
            bars=int(summary["bars"]) if "bars" in summary else None,
            test_bars=int(summary["test_bars"]) if "test_bars" in summary else None,
            ending_equity=float(summary["ending_equity"]) if "ending_equity" in summary else None,
            return_pct=float(summary["return_pct"]) if "return_pct" in summary else None,
            max_drawdown=float(diag["max_drawdown"]) if "max_drawdown" in diag else None,
            schema_version=(bundle_v2.get("schema_version") if bundle_v2 else summary.get("schema_version")),
            discovery_survivors=(int(summary.get("discovery_survivors")) if summary.get("discovery_survivors") is not None else None),
            discovery_rejected=(int(summary.get("discovery_rejected")) if summary.get("discovery_rejected") is not None else None),
            raw=summary,
        )

    def to_templates(self) -> list[UiPathTemplate]:
        df = self.load_raw().get("templates")
        if not isinstance(df, pd.DataFrame) or df.empty:
            return []
        out: list[UiPathTemplate] = []
        for _, row in df.iterrows():
            rid = str(row.get("template_id") or row.get("id") or row.get("name") or "unknown")
            out.append(
                UiPathTemplate(
                    template_id=rid,
                    label=row.get("label"),
                    expectancy=float(row["expectancy"]) if "expectancy" in row and pd.notna(row["expectancy"]) else None,
                    support=int(row["support"]) if "support" in row and pd.notna(row["support"]) else None,
                    raw=row.to_dict(),
                )
            )
        return out

    def to_drift_status(self) -> UiDriftStatus:
        raw = self.load_raw()
        bundle = raw.get("bundle") if isinstance(raw.get("bundle"), dict) else {}
        doctor = raw.get("doctor") if isinstance(raw.get("doctor"), dict) else {}

        # canonical governance payload path
        drift_obj = bundle.get("drift") if isinstance(bundle.get("drift"), dict) else {}
        governance = drift_obj.get("governance") if isinstance(drift_obj.get("governance"), dict) else None

        if governance is None and all(k in drift_obj for k in ["overall_status", "kill_switch_active", "kill_switch_reasons"]):
            governance = drift_obj

        if governance is None:
            # derive canonical governance from available metrics/doctor (single shared evaluator)
            m = drift_obj.get("metrics") if isinstance(drift_obj.get("metrics"), dict) else drift_obj
            g = evaluate_governance(
                bad_data=not bool(doctor.get("ok", True)),
                feature_psi_max=float(m.get("feature_drift", 0.0) or 0.0),
                state_drift=float(m.get("state_occupancy_drift", 0.0) or 0.0),
                action_rate_drift=float(m.get("action_rate_drift", 0.0) or 0.0),
                cost_drift_bps=float(m.get("cost_drift_bps", 0.0) or 0.0),
                calibration_drift=float(m.get("calibration_drift", 0.0) or 0.0),
                artifact_timestamp=(doctor.get("latest_timestamp") or (bundle.get("artifact_meta", {}) or {}).get("latest_timestamp") if isinstance(bundle.get("artifact_meta"), dict) else None),
                data_timestamp=(drift_obj.get("latest_live_timestamp") if isinstance(drift_obj, dict) else None),
                th=DriftThresholds(),
            )
            governance = g.to_dict()

        status = str(governance.get("overall_status", "OK")).upper()
        reasons = governance.get("kill_switch_reasons", []) if isinstance(governance.get("kill_switch_reasons"), list) else []

        return UiDriftStatus(
            ok=(status == "OK" and not bool(governance.get("kill_switch_active", False))),
            warnings=[] if status == "OK" else [f"governance_status={status}"],
            hard_failures=reasons,
            latest_timestamp=(bundle.get("artifact_meta", {}) or {}).get("latest_timestamp") if isinstance(bundle.get("artifact_meta"), dict) else doctor.get("latest_timestamp"),
            schema_versions=self.schema_versions(),
            governance_status=status,
            governance_payload=governance,
            raw={"governance": governance},
        )


class WalletAdapter:
    def __init__(self, rpc_url: str) -> None:
        self.rpc_url = rpc_url

    def _rpc_call(self, method: str, params: list[Any]) -> dict[str, Any]:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        r = requests.post(self.rpc_url, json=body, timeout=15)
        r.raise_for_status()
        out = r.json()
        if "error" in out:
            raise RuntimeError(f"RPC error {out['error']}")
        return out

    def wallet_snapshot(self, wallet: str) -> UiPortfolioSnapshot:
        if not wallet:
            return UiPortfolioSnapshot(wallet="", sol=None, usdc=None, ok=False, message="wallet address missing")

        bal = self._rpc_call("getBalance", [wallet, {"commitment": "processed"}])
        toks = self._rpc_call(
            "getTokenAccountsByOwner",
            [wallet, {"mint": USDC_MINT}, {"encoding": "jsonParsed", "commitment": "processed"}],
        )

        sol = float(bal.get("result", {}).get("value", 0)) / 1e9
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
        return UiPortfolioSnapshot(wallet=wallet, sol=sol, usdc=usdc, ok=True, message="ok")
