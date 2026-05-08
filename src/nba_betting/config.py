import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .league import LEAGUE


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _repo_data_root(repo_root: Path) -> Path:
    return repo_root / "data"


def _data_root(repo_root: Path) -> Path:
    """Root directory for data artifacts.

    Defaults to <repo>/data.
    Can be overridden (e.g., on Render) via WNBA_BETTING_DATA_ROOT to point at a
    persistent disk mount.
    """
    env = (
        os.environ.get(LEAGUE.data_root_env)
        or os.environ.get(LEAGUE.legacy_data_root_env)
        or ""
    ).strip()
    if env:
        try:
            return Path(env)
        except Exception:
            # Fall back to repo-local data dir
            return repo_root / "data"
    return repo_root / "data"


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _falsy_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"0", "false", "no", "n", "off"}


@dataclass(frozen=True)
class Paths:
    root: Path = field(default_factory=_repo_root)
    repo_data_root: Path = field(default_factory=lambda: _repo_data_root(_repo_root()))
    data_root: Path = field(default_factory=lambda: _data_root(_repo_root()))
    data_raw: Path = field(init=False)
    data_processed: Path = field(init=False)
    models: Path = field(init=False)
    repo_data_raw: Path = field(init=False)
    repo_data_processed: Path = field(init=False)
    repo_data_overrides: Path = field(init=False)
    data_overrides: Path = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_raw", self.data_root / "raw")
        object.__setattr__(self, "data_processed", self.data_root / "processed")
        object.__setattr__(self, "data_overrides", self.data_root / "overrides")
        object.__setattr__(self, "models", self.root / "models")

        object.__setattr__(self, "repo_data_raw", self.repo_data_root / "raw")
        object.__setattr__(self, "repo_data_processed", self.repo_data_root / "processed")
        object.__setattr__(self, "repo_data_overrides", self.repo_data_root / "overrides")


paths = Paths()


def reconcile_repo_data_to_active() -> dict[str, object]:
    """Reconcile repo-committed data into the active data root.

    Why: On Render we often set WNBA_BETTING_DATA_ROOT to a persistent disk.
    That means code reads/writes under that mount, but deploys bring updated
    repo-committed artifacts under <repo>/data. Without reconciliation, the
    persistent disk can lag behind the repo after a deploy.

    Behavior:
    - No-op when active data_root == repo_data_root.
    - Copies missing files and overwrites only when the repo copy is newer.

    Controls:
    - WNBA_BETTING_RECONCILE_REPO_DATA=0 disables.
    - Defaults to enabled when WNBA_BETTING_DATA_ROOT is set.
    """
    try:
        override_set = bool(
            (
                os.environ.get(LEAGUE.data_root_env)
                or os.environ.get(LEAGUE.legacy_data_root_env)
                or ""
            ).strip()
        )
        if _falsy_env("WNBA_BETTING_RECONCILE_REPO_DATA", default=False) or _falsy_env("NBA_BETTING_RECONCILE_REPO_DATA", default=False):
            return {"ok": True, "skipped": True, "reason": "disabled"}
        if not override_set and not (
            _truthy_env("WNBA_BETTING_RECONCILE_REPO_DATA", default=False)
            or _truthy_env("NBA_BETTING_RECONCILE_REPO_DATA", default=False)
        ):
            return {"ok": True, "skipped": True, "reason": "no override"}

        src_root = paths.repo_data_root
        dst_root = paths.data_root
        try:
            if src_root.resolve() == dst_root.resolve():
                return {"ok": True, "skipped": True, "reason": "same root"}
        except Exception:
            if str(src_root) == str(dst_root):
                return {"ok": True, "skipped": True, "reason": "same root"}

        copied = 0
        considered = 0
        errors: list[str] = []

        def _copy_tree(src_dir: Path, dst_dir: Path) -> None:
            nonlocal copied, considered
            if not src_dir.exists() or not src_dir.is_dir():
                return
            for src in src_dir.rglob("*"):
                try:
                    if not src.is_file():
                        continue
                    if src.name.startswith("."):
                        continue
                    rel = src.relative_to(src_dir)
                    dst = dst_dir / rel
                    considered += 1
                    if dst.exists():
                        try:
                            if float(src.stat().st_mtime) <= float(dst.stat().st_mtime):
                                continue
                        except Exception:
                            # If we can't compare mtimes, fall back to size compare
                            try:
                                if int(src.stat().st_size) == int(dst.stat().st_size):
                                    continue
                            except Exception:
                                continue
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    copied += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{src}: {e}")

        # Seed the persistent disk with repo-committed artifacts.
        _copy_tree(paths.repo_data_raw, paths.data_raw)
        _copy_tree(paths.repo_data_processed, paths.data_processed)
        _copy_tree(paths.repo_data_overrides, paths.data_overrides)

        return {
            "ok": True,
            "skipped": False,
            "repo_data_root": str(src_root),
            "active_data_root": str(dst_root),
            "files_considered": int(considered),
            "files_copied": int(copied),
            "errors": errors[:10],
        }
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}
