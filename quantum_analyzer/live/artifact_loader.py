from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from quantum_analyzer.paths.archetypes import PathTemplate
from quantum_analyzer.state.latent_model import GaussianHMMBaseline


class ArtifactLoader:
    def __init__(self, artifacts_root: str | Path):
        self.artifacts_root = Path(artifacts_root)

    def latest_bundle_path(self) -> Path:
        bundles = sorted(self.artifacts_root.rglob("artifact_bundle.json"), key=lambda p: p.stat().st_mtime)
        if not bundles:
            raise FileNotFoundError("No artifact_bundle.json found")
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
