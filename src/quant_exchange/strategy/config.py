"""Strategy configuration, versioning, and parameter persistence services.

Implements:
- Strategy version management (ST-04)
- Parameter set persistence (ST-03)
- YAML/TOML configuration drive (ST-03)
- Run ID and experiment record (ST-04, ST-06)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class StrategyVersion:
    """Immutable strategy version record."""

    version: str
    strategy_id: str
    created_at: datetime
    params_hash: str
    description: str = ""
    created_by: str = "system"


@dataclass(slots=True)
class StrategyParameterSet:
    """Named parameter set that can be frozen and versioned."""

    strategy_id: str
    name: str
    params: dict[str, Any]
    version: str = "1.0"
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    is_frozen: bool = False

    def freeze(self) -> None:
        """Freeze the parameter set to prevent further modifications."""
        self.is_frozen = True
        self.updated_at = utc_now()

    def clone_with_overrides(self, overrides: dict[str, Any]) -> StrategyParameterSet:
        """Create a new parameter set with overridden values."""
        if self.is_frozen:
            raise ValueError("cannot_modify_frozen_parameter_set")
        new_params = {**self.params, **overrides}
        return StrategyParameterSet(
            strategy_id=self.strategy_id,
            name=self.name + "_override",
            params=new_params,
            version=self.version,
        )


@dataclass(slots=True)
class StrategyRun:
    """Record of a single strategy execution/run."""

    run_id: str
    strategy_id: str
    version: str
    params: dict[str, Any]
    started_at: datetime
    ended_at: datetime | None = None
    instrument_ids: tuple[str, ...] = ()
    initial_cash: float = 100_000.0
    final_equity: float | None = None
    total_return: float | None = None
    sharpe: float | None = None
    max_drawdown: float | None = None
    notes: str = ""
    tags: tuple[str, ...] = ()

    @property
    def duration_seconds(self) -> float | None:
        """Return the run duration in seconds."""
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()


class StrategyConfigLoader:
    """Load strategy parameters from YAML or TOML configuration files."""

    def __init__(self) -> None:
        self._cache: dict[str, dict[str, Any]] = {}

    def load_from_yaml(self, path: str | Path) -> dict[str, Any]:
        """Load strategy configuration from a YAML file."""
        try:
            import yaml
        except ImportError:
            raise ImportError("pyyaml is required for YAML configuration loading")

        path = Path(path)
        if path in self._cache:
            return self._cache[path]

        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        self._cache[path] = config
        return config

    def load_from_toml(self, path: str | Path) -> dict[str, Any]:
        """Load strategy configuration from a TOML file."""
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        path = Path(path)
        if path in self._cache:
            return self._cache[path]

        with open(path, "rb") as f:
            config = tomllib.load(f)

        self._cache[path] = config
        return config

    def load(self, path: str | Path) -> dict[str, Any]:
        """Auto-detect format and load strategy configuration."""
        path = Path(path)
        suffix = path.suffix.lower()
        if suffix in (".yaml", ".yml"):
            return self.load_from_yaml(path)
        elif suffix == ".toml":
            return self.load_from_toml(path)
        else:
            raise ValueError(f"unsupported_config_format:{suffix}")

    def extract_params(self, config: dict[str, Any]) -> dict[str, Any]:
        """Extract the 'params' section from a configuration dict."""
        if "params" in config:
            return config["params"]
        return config

    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._cache.clear()


class StrategyVersionManager:
    """Manage strategy versions and parameter set history."""

    def __init__(self) -> None:
        self._versions: dict[tuple[str, str], StrategyVersion] = {}
        self._version_history: dict[str, list[StrategyVersion]] = {}

    def register_version(
        self,
        strategy_id: str,
        params: dict[str, Any],
        version: str | None = None,
        description: str = "",
        created_by: str = "system",
    ) -> StrategyVersion:
        """Register a new version of a strategy with its parameter set."""
        import hashlib

        if version is None:
            existing = self._version_history.get(strategy_id, [])
            if existing:
                last_ver = existing[-1].version
                parts = last_ver.split(".")
                version = f"{parts[0]}.{int(parts[1]) + 1}"
            else:
                version = "1.0"

        params_str = json.dumps(params, sort_keys=True)
        params_hash = hashlib.md5(params_str.encode()).hexdigest()

        ver = StrategyVersion(
            version=version,
            strategy_id=strategy_id,
            created_at=utc_now(),
            params_hash=params_hash,
            description=description,
            created_by=created_by,
        )

        self._versions[(strategy_id, version)] = ver
        if strategy_id not in self._version_history:
            self._version_history[strategy_id] = []
        self._version_history[strategy_id].append(ver)

        return ver

    def get_version(self, strategy_id: str, version: str) -> StrategyVersion | None:
        """Retrieve a specific version of a strategy."""
        return self._versions.get((strategy_id, version))

    def list_versions(self, strategy_id: str) -> list[StrategyVersion]:
        """List all versions of a strategy in chronological order."""
        return list(self._version_history.get(strategy_id, []))


class StrategyRunRecorder:
    """Record and persist strategy run metadata and results."""

    def __init__(self, storage_path: str | Path | None = None) -> None:
        self._storage_path = Path(storage_path) if storage_path else None
        self._runs: dict[str, StrategyRun] = {}
        self._runs_by_strategy: dict[str, list[str]] = {}

    def start_run(
        self,
        strategy_id: str,
        version: str,
        params: dict[str, Any],
        instrument_ids: tuple[str, ...] = (),
        initial_cash: float = 100_000.0,
        notes: str = "",
        tags: tuple[str, ...] = (),
    ) -> StrategyRun:
        """Start a new strategy run and return the run record."""
        run_id = str(uuid.uuid4())
        run = StrategyRun(
            run_id=run_id,
            strategy_id=strategy_id,
            version=version,
            params=dict(params),
            started_at=utc_now(),
            instrument_ids=instrument_ids,
            initial_cash=initial_cash,
            notes=notes,
            tags=tags,
        )
        self._runs[run_id] = run
        if strategy_id not in self._runs_by_strategy:
            self._runs_by_strategy[strategy_id] = []
        self._runs_by_strategy[strategy_id].append(run_id)
        return run

    def complete_run(
        self,
        run_id: str,
        *,
        ended_at: datetime | None = None,
        final_equity: float | None = None,
        total_return: float | None = None,
        sharpe: float | None = None,
        max_drawdown: float | None = None,
    ) -> StrategyRun | None:
        """Finalize a strategy run with its results."""
        run = self._runs.get(run_id)
        if run is None:
            return None

        object.__setattr__(run, "ended_at", ended_at or utc_now())
        object.__setattr__(run, "final_equity", final_equity)
        object.__setattr__(run, "total_return", total_return)
        object.__setattr__(run, "sharpe", sharpe)
        object.__setattr__(run, "max_drawdown", max_drawdown)

        if self._storage_path:
            self._persist_run(run)

        return run

    def get_run(self, run_id: str) -> StrategyRun | None:
        """Retrieve a strategy run by its ID."""
        return self._runs.get(run_id)

    def list_runs(
        self,
        strategy_id: str | None = None,
        limit: int = 100,
    ) -> list[StrategyRun]:
        """List strategy runs, optionally filtered by strategy ID."""
        if strategy_id is None:
            runs = list(self._runs.values())
        else:
            run_ids = self._runs_by_strategy.get(strategy_id, [])
            runs = [self._runs[rid] for rid in run_ids]

        runs.sort(key=lambda r: r.started_at, reverse=True)
        return runs[:limit]

    def _persist_run(self, run: StrategyRun) -> None:
        """Persist a run record to disk."""
        if self._storage_path is None:
            return

        self._storage_path.mkdir(parents=True, exist_ok=True)
        run_file = self._storage_path / f"{run.run_id}.json"
        data = {
            "run_id": run.run_id,
            "strategy_id": run.strategy_id,
            "version": run.version,
            "params": run.params,
            "started_at": run.started_at.isoformat(),
            "ended_at": run.ended_at.isoformat() if run.ended_at else None,
            "instrument_ids": list(run.instrument_ids),
            "initial_cash": run.initial_cash,
            "final_equity": run.final_equity,
            "total_return": run.total_return,
            "sharpe": run.sharpe,
            "max_drawdown": run.max_drawdown,
            "notes": run.notes,
            "tags": list(run.tags),
        }
        with open(run_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


class StrategyParameterStore:
    """Persist and retrieve strategy parameter sets."""

    def __init__(self, storage_path: str | Path | None = None) -> None:
        self._storage_path = Path(storage_path) if storage_path else None
        self._param_sets: dict[tuple[str, str], StrategyParameterSet] = {}

    def save(self, param_set: StrategyParameterSet) -> None:
        """Persist a parameter set."""
        key = (param_set.strategy_id, param_set.name)
        self._param_sets[key] = param_set

        if self._storage_path:
            self._persist_param_set(param_set)

    def load(self, strategy_id: str, name: str) -> StrategyParameterSet | None:
        """Load a parameter set by strategy ID and name."""
        key = (strategy_id, name)

        if key in self._param_sets:
            return self._param_sets[key]

        if self._storage_path:
            return self._load_param_set(strategy_id, name)

        return None

    def list_param_sets(self, strategy_id: str) -> list[StrategyParameterSet]:
        """List all parameter sets for a strategy."""
        return [
            ps for (sid, _), ps in self._param_sets.items() if sid == strategy_id
        ]

    def delete(self, strategy_id: str, name: str) -> bool:
        """Delete a parameter set."""
        key = (strategy_id, name)
        if key in self._param_sets:
            del self._param_sets[key]
            if self._storage_path:
                self._delete_param_set(strategy_id, name)
            return True
        return False

    def _persist_param_set(self, param_set: StrategyParameterSet) -> None:
        """Persist a parameter set to disk."""
        if self._storage_path is None:
            return

        self._storage_path.mkdir(parents=True, exist_ok=True)
        safe_name = param_set.name.replace("/", "_").replace("\\", "_")
        ps_file = self._storage_path / f"{param_set.strategy_id}_{safe_name}.json"
        data = {
            "strategy_id": param_set.strategy_id,
            "name": param_set.name,
            "params": param_set.params,
            "version": param_set.version,
            "created_at": param_set.created_at.isoformat(),
            "updated_at": param_set.updated_at.isoformat(),
            "is_frozen": param_set.is_frozen,
        }
        with open(ps_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _load_param_set(self, strategy_id: str, name: str) -> StrategyParameterSet | None:
        """Load a parameter set from disk."""
        if self._storage_path is None:
            return None

        safe_name = name.replace("/", "_").replace("\\", "_")
        ps_file = self._storage_path / f"{strategy_id}_{safe_name}.json"
        if not ps_file.exists():
            return None

        with open(ps_file, encoding="utf-8") as f:
            data = json.load(f)

        return StrategyParameterSet(
            strategy_id=data["strategy_id"],
            name=data["name"],
            params=data["params"],
            version=data.get("version", "1.0"),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            is_frozen=data.get("is_frozen", False),
        )

    def _delete_param_set(self, strategy_id: str, name: str) -> None:
        """Delete a parameter set file from disk."""
        if self._storage_path is None:
            return

        safe_name = name.replace("/", "_").replace("\\", "_")
        ps_file = self._storage_path / f"{strategy_id}_{safe_name}.json"
        if ps_file.exists():
            ps_file.unlink()