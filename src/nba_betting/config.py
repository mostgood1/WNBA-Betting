import os
from dataclasses import dataclass, field
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _data_root(repo_root: Path) -> Path:
    """Root directory for data artifacts.

    Defaults to <repo>/data.
    Can be overridden (e.g., on Render) via NBA_BETTING_DATA_ROOT to point at a
    persistent disk mount.
    """
    env = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
    if env:
        try:
            return Path(env)
        except Exception:
            # Fall back to repo-local data dir
            return repo_root / "data"
    return repo_root / "data"


@dataclass(frozen=True)
class Paths:
    root: Path = field(default_factory=_repo_root)
    data_root: Path = field(default_factory=lambda: _data_root(_repo_root()))
    data_raw: Path = field(init=False)
    data_processed: Path = field(init=False)
    models: Path = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_raw", self.data_root / "raw")
        object.__setattr__(self, "data_processed", self.data_root / "processed")
        object.__setattr__(self, "models", self.root / "models")


paths = Paths()
