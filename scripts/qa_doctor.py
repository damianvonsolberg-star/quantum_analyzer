#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sys
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.contracts import ARTIFACT_SCHEMA_V2, REQUIRED_BUNDLE_SECTIONS_V2, ArtifactCheck, DoctorReport  # noqa: E402


MANDATORY = ["artifact_bundle.json", "summary.json", "equity_curve.csv", "actions.csv"]
OPTIONAL_ANY = ["templates.json", "templates.parquet"]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _extract_schema_versions(obj: Any, acc: set[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "schema_version" and isinstance(v, str):
                acc.add(v)
            else:
                _extract_schema_versions(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _extract_schema_versions(x, acc)


def _find_latest_timestamp(equity_df: pd.DataFrame, actions_df: pd.DataFrame) -> str | None:
    candidates: list[pd.Timestamp] = []
    for df in (equity_df, actions_df):
        if not df.empty:
            # prefer explicit ts column, else first column if parseable, else index
            if "ts" in df.columns:
                vals = pd.to_datetime(df["ts"], errors="coerce", utc=True).dropna()
                if not vals.empty:
                    candidates.append(vals.max())
            else:
                first = df.columns[0]
                vals = pd.to_datetime(df[first], errors="coerce", utc=True).dropna()
                if not vals.empty:
                    candidates.append(vals.max())
                else:
                    vals = pd.to_datetime(df.index, errors="coerce", utc=True)
                    vals = vals[~pd.isna(vals)]
                    if len(vals) > 0:
                        candidates.append(max(vals))
    if not candidates:
        return None
    return max(candidates).isoformat()


def _latest_action(actions_df: pd.DataFrame) -> dict[str, Any]:
    if actions_df.empty:
        return {}
    row = actions_df.iloc[-1]
    out = {}
    for k in ["action", "target_position", "expected_edge_bps", "expected_cost_bps", "reason", "ts"]:
        if k in actions_df.columns:
            out[k] = row[k]
    return out


def _forecast_horizons(bundle: dict[str, Any] | None) -> list[str]:
    if not bundle:
        return []
    horizons: set[str] = set()

    # common location: forecast.distributions
    f = bundle.get("forecast", {}) if isinstance(bundle, dict) else {}
    if isinstance(f, dict):
        d = f.get("distributions", {})
        if isinstance(d, dict):
            for k in d.keys():
                if re.match(r"^h\d+$", str(k)):
                    horizons.add(str(k))

    # fallback scan all keys
    def scan(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                if re.match(r"^h\d+$", str(k)):
                    horizons.add(str(k))
                scan(v)
        elif isinstance(x, list):
            for z in x:
                scan(z)

    scan(bundle)
    return sorted(horizons)


def _extract_coverage(bundle: dict[str, Any] | None, artifacts_dir: Path) -> dict[str, float | None]:
    out = {"agg_trades": None, "book_ticker": None, "open_interest": None, "basis": None}

    # source of truth: explicit source_coverage.json (if present)
    cov_file = artifacts_dir / "source_coverage.json"
    if cov_file.exists():
        cov_json = _read_json(cov_file)
        cov = cov_json.get("coverage") if isinstance(cov_json, dict) and isinstance(cov_json.get("coverage"), dict) else None
        if cov:
            for k in out:
                v = cov.get(k)
                if isinstance(v, (float, int)):
                    out[k] = float(v)
            return out

    if not isinstance(bundle, dict):
        return out

    coverage = bundle.get("coverage") if isinstance(bundle.get("coverage"), dict) else None
    if coverage is None and isinstance(bundle.get("diagnostics"), dict):
        maybe = bundle.get("diagnostics", {}).get("coverage")
        coverage = maybe if isinstance(maybe, dict) else None

    if coverage:
        for k in out:
            v = coverage.get(k)
            if isinstance(v, (float, int)):
                out[k] = float(v)
    return out


def validate_artifacts(artifacts_dir: Path) -> DoctorReport:
    report = DoctorReport(artifact_dir=str(artifacts_dir))

    if not artifacts_dir.exists() or not artifacts_dir.is_dir():
        report.hard_failures.append("Artifact directory not found")
        report.missing_files.extend(MANDATORY)
        return report

    # mandatory checks
    for fn in MANDATORY:
        p = artifacts_dir / fn
        if p.exists():
            report.checks.append(ArtifactCheck(name=fn, present=True, status="pass"))
        else:
            report.checks.append(ArtifactCheck(name=fn, present=False, status="fail", message="missing"))
            report.missing_files.append(fn)
            report.hard_failures.append(f"Missing required artifact: {fn}")

    # snapshot/feature manifest checks (explorer-level provenance)
    snap_manifest_path = artifacts_dir / "snapshot_manifest.json"
    feat_manifest_path = artifacts_dir / "feature_manifest.json"
    if snap_manifest_path.exists():
        sm = _read_json(snap_manifest_path)
        if not isinstance(sm, dict) or not sm.get("snapshot_id"):
            report.hard_failures.append("snapshot_manifest.json malformed or missing snapshot_id")
    else:
        report.warnings.append("snapshot_manifest.json missing")

    if feat_manifest_path.exists():
        fm = _read_json(feat_manifest_path)
        if not isinstance(fm, dict) or not fm.get("snapshot_id"):
            report.hard_failures.append("feature_manifest.json malformed or missing snapshot_id")
        if isinstance(fm, dict):
            rows = fm.get("rows")
            if rows is None or int(rows) <= 0:
                report.hard_failures.append("feature_manifest.json indicates empty feature snapshot")
            if not isinstance(fm.get("feature_versions"), dict) or not fm.get("feature_versions"):
                report.hard_failures.append("feature_manifest.json missing feature_versions")
        if isinstance(fm, dict) and isinstance(sm := (_read_json(snap_manifest_path) if snap_manifest_path.exists() else None), dict):
            if fm.get("snapshot_id") != sm.get("snapshot_id"):
                report.hard_failures.append("feature/snapshot manifest snapshot_id mismatch")
            sm_built = sm.get("built_at")
            fm_built = fm.get("built_at")
            if not fm_built:
                report.warnings.append("feature_manifest.json missing built_at")
            elif sm_built and str(fm_built) < str(sm_built):
                report.hard_failures.append("feature store appears older than snapshot manifest")
    else:
        report.warnings.append("feature_manifest.json missing")

    # optional templates
    has_optional = any((artifacts_dir / fn).exists() for fn in OPTIONAL_ANY)
    if not has_optional:
        report.warnings.append("No templates artifact found (templates.json or templates.parquet)")
        report.checks.append(ArtifactCheck(name="templates", present=False, status="warn", message="optional missing"))
    else:
        report.checks.append(ArtifactCheck(name="templates", present=True, status="pass"))

    # parse files if present
    bundle = _read_json(artifacts_dir / "artifact_bundle.json") if (artifacts_dir / "artifact_bundle.json").exists() else None
    summary = _read_json(artifacts_dir / "summary.json") if (artifacts_dir / "summary.json").exists() else None

    schema_versions: set[str] = set()
    if bundle is not None:
        _extract_schema_versions(bundle, schema_versions)
    if summary is not None:
        _extract_schema_versions(summary, schema_versions)

    report.schema_versions = sorted(schema_versions)

    # Canonical schema v2 validation (strict by default).
    if isinstance(bundle, dict):
        bsv = bundle.get("schema_version")
        if bsv == ARTIFACT_SCHEMA_V2:
            missing_sections = [k for k in REQUIRED_BUNDLE_SECTIONS_V2 if k not in bundle]
            if missing_sections:
                report.hard_failures.append(
                    f"schema v2 bundle missing required sections: {', '.join(missing_sections)}"
                )
            else:
                # required key fields
                req_fields = {
                    "forecast": ["confidence", "entropy", "calibration_score", "timestamps"],
                    "proposal": ["timestamp", "action", "target_position", "expected_edge_bps", "expected_cost_bps"],
                    "drift": ["governance_status"],
                }
                for sec, keys in req_fields.items():
                    obj = bundle.get(sec)
                    if not isinstance(obj, dict):
                        report.hard_failures.append(f"schema v2 section {sec} must be an object")
                        continue
                    miss = [k for k in keys if k not in obj]
                    if miss:
                        report.hard_failures.append(f"schema v2 section {sec} missing fields: {', '.join(miss)}")
        else:
            report.hard_failures.append(
                f"artifact_bundle.json schema_version mismatch: {bsv!r} != {ARTIFACT_SCHEMA_V2}"
            )
    else:
        report.hard_failures.append("artifact_bundle.json malformed or unreadable")

    equity_df = pd.DataFrame()
    actions_df = pd.DataFrame()

    eq_path = artifacts_dir / "equity_curve.csv"
    if eq_path.exists():
        try:
            equity_df = pd.read_csv(eq_path)
            if "equity" not in equity_df.columns:
                report.hard_failures.append("equity_curve.csv missing required column: equity")
        except Exception as e:
            report.hard_failures.append(f"Failed reading equity_curve.csv: {e}")

    act_path = artifacts_dir / "actions.csv"
    if act_path.exists():
        try:
            actions_df = pd.read_csv(act_path)
            if "action" not in actions_df.columns:
                report.hard_failures.append("actions.csv missing required column: action")
        except Exception as e:
            report.hard_failures.append(f"Failed reading actions.csv: {e}")

    report.latest_timestamp = _find_latest_timestamp(equity_df, actions_df)
    report.latest_proposal_action = _latest_action(actions_df)
    report.latest_forecast_horizons = _forecast_horizons(bundle)

    if summary:
        report.latest_backtest_metrics = {
            k: summary.get(k)
            for k in ["ending_equity", "return_pct", "bars", "test_bars", "diagnostics"]
            if k in summary
        }
    else:
        report.latest_backtest_metrics = {}

    if not report.latest_forecast_horizons:
        report.warnings.append("No forecast horizons found in artifact bundle")
    if not report.latest_proposal_action:
        report.warnings.append("No latest proposal/action found in actions.csv")

    # Fail closed on missing/inadequate historical coverage diagnostics.
    cov = _extract_coverage(bundle, artifacts_dir)
    report.latest_backtest_metrics["coverage"] = cov
    required_cov = {"agg_trades": 0.7, "book_ticker": 0.7, "open_interest": 0.7, "basis": 0.7}
    for key, threshold in required_cov.items():
        val = cov.get(key)
        if val is None:
            report.hard_failures.append(f"Missing historical coverage metric: {key}")
        elif val < threshold:
            report.hard_failures.append(
                f"Insufficient historical coverage for {key}: {val:.3f} < {threshold:.3f}"
            )

    return report


def print_human(report: DoctorReport) -> None:
    status = "PASS" if report.ok else "FAIL"
    print(f"[QA DOCTOR] {status}")
    print(f"artifact_dir: {report.artifact_dir}")
    print(f"latest_timestamp: {report.latest_timestamp}")
    print(f"latest_forecast_horizons: {report.latest_forecast_horizons}")
    print(f"latest_proposal_action: {report.latest_proposal_action}")
    print(f"latest_backtest_metrics: {report.latest_backtest_metrics}")
    if report.missing_files:
        print(f"missing_files: {report.missing_files}")
    if report.warnings:
        print(f"warnings: {report.warnings}")
    if report.hard_failures:
        print(f"hard_failures: {report.hard_failures}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate quantum_analyzer artifact bundle")
    parser.add_argument("--artifacts", required=True, help="Artifact directory path")
    args = parser.parse_args(argv)

    artifacts_dir = Path(args.artifacts)
    report = validate_artifacts(artifacts_dir)

    out_path = artifacts_dir / "doctor_report.json"
    out_path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")

    print_human(report)
    print(f"doctor_report: {out_path}")

    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
