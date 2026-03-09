from __future__ import annotations

from ui.research_ops import acquire_lock, release_lock, run_research_cycle


class _R:
    def __init__(self, code=0):
        self.returncode = code
        self.stdout = "ok"
        self.stderr = ""


def test_lock_acquire_release(tmp_path):
    ok, msg = acquire_lock(tmp_path)
    assert ok is True
    ok2, msg2 = acquire_lock(tmp_path)
    assert ok2 is False
    release_lock(tmp_path)
    ok3, _ = acquire_lock(tmp_path)
    assert ok3 is True
    release_lock(tmp_path)


def test_run_research_cycle_success(monkeypatch):
    import ui.research_ops as ro

    def _run(cmd, capture_output=True, text=True, timeout=None):
        return _R(0)

    monkeypatch.setattr(ro.subprocess, "run", _run)
    ok, status = run_research_cycle()
    assert ok is True
    assert status.get("ok") is True
