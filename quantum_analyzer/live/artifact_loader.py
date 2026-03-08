from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from quantum_analyzer.contracts import ARTIFACT_SCHEMA_V2, ArtifactBundleV2
from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.state.latent_model import GaussianHMMBaseline


class ArtifactValidationError(RuntimeError):
    pass


@dataclass
class ArtifactValidationResult:
    ok: bool
    reason: str
    details: dict[str, Any]


class ArtifactLoader:
    def __init__(self, artifacts_root: str | Path):
        self.artifacts_root = Path(artifacts_root)

    def latest_bundle_path(self) -> Path:
        bundles = sorted(self.artifacts_root.rglob("artifact_bundle.json"), key=lambda p: p.stat().st_mtime)
        if not bundles:
            raise ArtifactValidationError("No artifact_bundle.json found")
        return bundles[-1]

    def load_bundle(self, bundle_path: str | Path | None = None) -> dict[str, Any]:
        p = Path(bundle_path) if bundle_path else self.latest_bundle_path()
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    def load_templates(self, bundle_dir: str | Path) -> list[PathTemplate]:
        p = Path(bundle_dir) / "templates.json"
        if not p.exists():
            return []
        raw = json.loads(p.read_text())
        return [PathTemplate(**r) for r in raw]

    def load_state_model(self, bundle_dir: str | Path) -> GaussianHMMBaseline | None:
        p = Path(bundle_dir) / "latent_model.pkl"
        if not p.exists():
            return None
        return GaussianHMMBaseline.load(p)

    def validate_production_artifacts(self, bundle: dict[str, Any], bundle_dir: Path) -> ArtifactValidationResult:
        # schema
        try:
            ArtifactBundleV2.from_dict(bundle)
        except Exception as e:  # noqa: BLE001
            return ArtifactValidationResult(False, f"bundle_schema_invalid: {e}", {"schema_version": bundle.get("schema_version")})

        # required model/config artifacts
        if not (bundle_dir / "latent_model.pkl").exists():
            return ArtifactValidationResult(False, "missing_latent_model", {"path": str(bundle_dir / 'latent_model.pkl')})
        if not (bundle_dir / "templates.json").exists():
            return ArtifactValidationResult(False, "missing_templates", {"path": str(bundle_dir / 'templates.json')})

        cfg = bundle.get("config") if isinstance(bundle.get("config"), dict) else {}
        if not isinstance(cfg.get("policy", {}), dict):
            return ArtifactValidationResult(False, "missing_policy_config", {})

        f = bundle.get("forecast") if isinstance(bundle.get("forecast"), dict) else {}
        for req in ["confidence", "entropy", "calibration_score"]:
            if req not in f:
                return ArtifactValidationResult(False, f"missing_forecast_field:{req}", {})

        return ArtifactValidationResult(True, "ok", {"schema_version": ARTIFACT_SCHEMA_V2})

    def load_production_artifacts(self) -> tuple[dict[str, Any], Path, list[PathTemplate], GaussianHMMBaseline]:
        bundle_path = self.latest_bundle_path()
        bundle = self.load_bundle(bundle_path)
        bundle_dir = Path(bundle_path).parent
        validation = self.validate_production_artifacts(bundle, bundle_dir)
        if not validation.ok:
            raise ArtifactValidationError(validation.reason)

        model = self.load_state_model(bundle_dir)
        if model is None:
            raise ArtifactValidationError("missing_latent_model")
        templates = self.load_templates(bundle_dir)
        return bundle, bundle_dir, templates, model
