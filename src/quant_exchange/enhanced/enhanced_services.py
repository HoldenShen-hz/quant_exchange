"""Enhanced platform services with feature store persistence, model training pipeline, and execution orchestration.

Implements:
- Scalable Feature Store with backfill support
- Research Lab environment with notebook management
- Model training pipeline and drift monitoring
- Replay/Snapshot/Shadow orchestration
- Execution State Machine (EMS/SOR)
- Options/MM/DEX business state machine and accounting
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import sys
import threading
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable
from abc import ABC, abstractmethod


def _now() -> str:
    """Return an ISO-8601 timestamp string."""
    return datetime.now(timezone.utc).isoformat()


class FeatureStoreState(str, Enum):
    """Feature store processing state."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ModelState(str, Enum):
    """Model lifecycle state."""

    REGISTERED = "registered"
    TRAINING = "training"
    EVALUATED = "evaluated"
    DEPLOYED = "deployed"
    ARCHIVED = "archived"
    DRIFTED = "drifted"


class ExecutionState(str, Enum):
    """Execution state machine states."""

    PENDING = "pending"
    ROUTING = "routing"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass
class FeatureBackfillJob:
    """Feature backfill job configuration."""

    job_id: str
    feature_code: str
    instrument_ids: list[str]
    start_time: datetime
    end_time: datetime
    interval: str = "1d"
    state: FeatureStoreState = FeatureStoreState.PENDING
    progress: float = 0.0
    records_processed: int = 0
    error_message: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class FeatureDefinition:
    """Feature definition with versioning and metadata."""

    feature_code: str
    feature_name: str
    description: str
    expression: str
    version: str
    owner: str = ""
    tags: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    is_active: bool = True


@dataclass
class FeatureValue:
    """A computed feature value with metadata."""

    value_key: str
    feature_code: str
    instrument_id: str
    event_time: datetime
    value: float
    version: str
    computed_at: datetime = field(default_factory=datetime.now)


class ScalableFeatureStore:
    """Scalable feature store with backfill support and distributed computation."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._definitions: dict[str, FeatureDefinition] = {}
        self._values: dict[str, list[FeatureValue]] = defaultdict(list)
        self._backfill_jobs: dict[str, FeatureBackfillJob] = {}
        self._compute_handlers: dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._backfill_queue: asyncio.Queue = asyncio.Queue()

    def register_feature(
        self,
        feature_code: str,
        feature_name: str,
        expression: str,
        description: str = "",
        owner: str = "",
        tags: list[str] | None = None,
    ) -> FeatureDefinition:
        """Register a new feature definition."""
        version = "v1"
        definition = FeatureDefinition(
            feature_code=feature_code,
            feature_name=feature_name,
            description=description,
            expression=expression,
            version=version,
            owner=owner,
            tags=tags or [],
        )

        with self._lock:
            self._definitions[feature_code] = definition

        if self.persistence:
            self.persistence.upsert_record(
                "feature_definitions",
                "feature_code",
                feature_code,
                asdict(definition),
                extra_columns={"feature_name": feature_name},
            )

        return definition

    def publish_version(self, feature_code: str) -> str:
        """Publish a new version of a feature definition."""
        with self._lock:
            if feature_code not in self._definitions:
                raise ValueError(f"Feature {feature_code} not found")

            defn = self._definitions[feature_code]
            parts = defn.version.split("v")
            major = int(parts[1]) if len(parts) > 1 else 0
            new_version = f"v{major + 1}"

            defn.version = new_version
            defn.updated_at = datetime.now()

        return defn.version

    def register_compute_handler(self, feature_code: str, handler: Callable[[list], float]) -> None:
        """Register a custom compute handler for a feature."""
        self._compute_handlers[feature_code] = handler

    def compute_feature(
        self,
        feature_code: str,
        instrument_id: str,
        data: list,
        event_time: datetime,
    ) -> FeatureValue:
        """Compute a feature value for given data."""
        if feature_code not in self._definitions:
            raise ValueError(f"Feature {feature_code} not found")

        definition = self._definitions[feature_code]

        if feature_code in self._compute_handlers:
            value = self._compute_handlers[feature_code](data)
        else:
            value = self._evaluate_expression(definition.expression, data)

        value_key = f"{feature_code}:{instrument_id}:{event_time.isoformat()}"
        feature_value = FeatureValue(
            value_key=value_key,
            feature_code=feature_code,
            instrument_id=instrument_id,
            event_time=event_time,
            value=value,
            version=definition.version,
        )

        with self._lock:
            self._values[feature_code].append(feature_value)

        if self.persistence:
            self.persistence.upsert_record(
                "feature_values",
                "value_key",
                value_key,
                asdict(feature_value),
                extra_columns={
                    "feature_code": feature_code,
                    "instrument_id": instrument_id,
                    "event_time": event_time.isoformat(),
                },
            )

        return feature_value

    def start_backfill(
        self,
        feature_code: str,
        instrument_ids: list[str],
        start_time: datetime,
        end_time: datetime,
        interval: str = "1d",
    ) -> FeatureBackfillJob:
        """Start a backfill job for a feature."""
        job_id = f"backfill:{feature_code}:{uuid.uuid4().hex[:8]}"
        job = FeatureBackfillJob(
            job_id=job_id,
            feature_code=feature_code,
            instrument_ids=instrument_ids,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )

        with self._lock:
            self._backfill_jobs[job_id] = job

        return job

    def get_backfill_status(self, job_id: str) -> FeatureBackfillJob | None:
        """Get the status of a backfill job."""
        return self._backfill_jobs.get(job_id)

    def _evaluate_expression(self, expression: str, data: list) -> float:
        """Evaluate a feature expression."""
        parts = expression.split(":")
        if len(parts) < 2:
            return 0.0

        kind, window = parts[0], int(parts[1])
        values = data[-window:] if len(data) >= window else data

        if not values:
            return 0.0

        if kind == "sma":
            return sum(values) / len(values)
        elif kind == "momentum":
            return values[-1] - values[0] if len(values) > 1 else 0.0
        elif kind == "rsi":
            gains = []
            losses = []
            for i in range(1, len(values)):
                diff = values[i] - values[i - 1]
                if diff > 0:
                    gains.append(diff)
                else:
                    losses.append(abs(diff))
            avg_gain = sum(gains) / len(gains) if gains else 0.0
            avg_loss = sum(losses) / len(losses) if losses else 0.0
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return 100.0 - (100.0 / (1.0 + rs))
        elif kind == "std":
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            return variance ** 0.5

        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Research Kernel — Real Python Code Execution for Notebooks
# ─────────────────────────────────────────────────────────────────────────────

class ResearchKernel:
    """In-process Python execution kernel for research notebook cells.

    Provides a safe(ish) isolated namespace for executing Python code
    submitted from notebook cells, with stdout/stderr capture and
    pre-bound platform libraries.
    """

    BUILTIN_IMPORTS = (
        "import math, random, datetime, time, json, re, hashlib, itertools, "
        "functools, operator, collections, pathlib, types, traceback, sys, io"
    )

    def __init__(self) -> None:
        self._namespace: dict[str, Any] = {}
        self._stdout = io.StringIO()
        self._stderr = io.StringIO()
        self._exec(self.BUILTIN_IMPORTS)
        # Pre-bind quant platform libraries
        self._bind_platform_libraries()

    def _bind_platform_libraries(self) -> None:
        """Bind commonly-used platform libraries into the kernel namespace."""
        try:
            from quant_exchange.strategy import factors
            from quant_exchange.portfolio import service as portfolio_service
            from quant_exchange.risk import service as risk_service
            from quant_exchange.backtest import engine as backtest_engine
            from quant_exchange.stocks import service as stocks_service
            from quant_exchange.intelligence import service as intelligence_service
            self._namespace["factors"] = factors
            self._namespace["portfolio"] = portfolio_service
            self._namespace["risk"] = risk_service
            self._namespace["backtest"] = backtest_engine
            self._namespace["stocks"] = stocks_service
            self._namespace["intelligence"] = intelligence_service
        except Exception:
            pass  # Platform libraries not available in all contexts

    def execute(self, code: str, timeout_seconds: float = 30.0) -> dict[str, Any]:
        """Execute Python code and return results with captured output."""
        self._stdout = io.StringIO()
        self._stderr = io.StringIO()
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        result = {
            "status": "success",
            "output": "",
            "error": "",
            "execution_time_ms": 0,
            "stdout": "",
            "stderr": "",
            "return_value": None,
            "variables": {},
        }
        start = time.monotonic()
        try:
            sys.stdout = self._stdout
            sys.stderr = self._stderr
            _code = code.strip()
            # Detect pure expressions (no assignment, no keyword, single line)
            is_pure_expr = (
                _code
                and not any(_code.startswith(kw) for kw in ("if ", "for ", "while ", "def ", "class ", "import ", "from ", "return ", "raise ", "with ", "assert ", "global ", " nonlocal ", "="))
                and "\n" not in _code
                and "=" not in _code
            )
            if is_pure_expr:
                compiled = compile(_code, "<cell>", "eval")
                exec_result = eval(compiled, {"__builtins__": __builtins__}, self._namespace)
                result["return_value"] = self._serialize(exec_result)
            else:
                exec(_code, {"__builtins__": __builtins__}, self._namespace)
        except SystemExit:
            result["status"] = "error"
            result["error"] = "Kernel shutdown requested"
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            result["status"] = "error"
            tb_lines = traceback.format_exception(exc_type, exc_val, exc_tb)
            result["error"] = "".join(tb_lines[-10:])  # Last 10 frames
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            result["execution_time_ms"] = round((time.monotonic() - start) * 1000, 2)
            result["stdout"] = self._stdout.getvalue()
            result["stderr"] = self._stderr.getvalue()
            result["output"] = result["stdout"]
            # Expose public variables
            result["variables"] = {
                k: self._serialize(v)
                for k, v in self._namespace.items()
                if not k.startswith("_") and not callable(v)
            }
        return result

    def _serialize(self, value: Any) -> Any:
        """Convert a Python object to a JSON-safe representation."""
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (list, tuple)):
            return [self._serialize(x) for x in value]
        if isinstance(value, dict):
            return {k: self._serialize(v) for k, v in list(value.items())[:50]}
        if isinstance(value, (set, frozenset)):
            return sorted(self._serialize(x) for x in value)[:50]
        if hasattr(value, "__dict__"):
            return str(type(value).__name__) + "(...)"
        try:
            return repr(value)
        except Exception:
            return str(type(value).__name__)

    def get_variable(self, name: str) -> Any:
        """Get a variable from the kernel namespace."""
        return self._namespace.get(name)

    def set_variable(self, name: str, value: Any) -> None:
        """Set a variable in the kernel namespace."""
        self._namespace[name] = value

    def list_variables(self) -> list[str]:
        """List all variable names in the kernel namespace."""
        return [k for k in self._namespace.keys() if not k.startswith("_")]

    def clear(self) -> None:
        """Clear the kernel namespace and reset state."""
        self._namespace.clear()
        self._exec(self.BUILTIN_IMPORTS)
        self._bind_platform_libraries()

    def _exec(self, code: str) -> None:
        """Execute code in the kernel namespace without result capture."""
        exec(code, {"__builtins__": __builtins__}, self._namespace)


class ResearchLabEnvironment:
    """Research Lab environment with notebook management and experiment tracking."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._projects: dict[str, dict] = {}
        self._notebooks: dict[str, dict] = {}
        self._experiments: dict[str, dict] = {}
        self._experiment_runs: dict[str, list[dict]] = defaultdict(list)
        self._datasets: dict[str, dict] = {}
        self._kernels: dict[str, ResearchKernel] = {}  # notebook_key -> kernel
        self._lock = threading.RLock()

    def create_project(
        self,
        project_code: str,
        project_name: str,
        description: str = "",
        owner: str = "",
    ) -> dict:
        """Create a new research project."""
        payload = {
            "project_code": project_code,
            "project_name": project_name,
            "description": description,
            "owner": owner,
            "status": "active",
            "created_at": _now(),
            "updated_at": _now(),
        }

        with self._lock:
            self._projects[project_code] = payload

        if self.persistence:
            self.persistence.upsert_record(
                "research_projects",
                "project_code",
                project_code,
                payload,
                extra_columns={"project_name": project_name},
            )

        return payload

    def create_notebook(
        self,
        project_code: str,
        notebook_name: str,
        notebook_type: str = "jupyter",
    ) -> dict:
        """Create a new notebook in a project."""
        notebook_key = f"{project_code}:{notebook_name}"
        payload = {
            "notebook_key": notebook_key,
            "project_code": project_code,
            "notebook_name": notebook_name,
            "notebook_type": notebook_type,
            "status": "active",
            "cells": [],
            "created_at": _now(),
            "updated_at": _now(),
        }

        with self._lock:
            self._notebooks[notebook_key] = payload

        if self.persistence:
            self.persistence.upsert_record(
                "research_notebooks",
                "notebook_key",
                notebook_key,
                payload,
                extra_columns={"project_code": project_code},
            )

        return payload

    def execute_notebook_cell(
        self,
        notebook_key: str,
        cell_code: str,
        cell_type: str = "code",
    ) -> dict:
        """Execute a notebook cell using the ResearchKernel and store the result."""
        cell_id = f"{notebook_key}:{uuid.uuid4().hex[:8]}"

        # Get or create a kernel for this notebook
        with self._lock:
            if notebook_key not in self._kernels:
                self._kernels[notebook_key] = ResearchKernel()
            kernel = self._kernels[notebook_key]

        # Execute based on cell type
        if cell_type == "markdown":
            result = {"status": "skipped", "output": cell_code, "execution_time_ms": 0}
        else:
            result = kernel.execute(cell_code)

        payload = {
            "cell_key": cell_id,
            "cell_type": cell_type,
            "code": cell_code,
            "result": result,
            "executed_at": _now(),
        }

        with self._lock:
            if notebook_key in self._notebooks:
                self._notebooks[notebook_key]["cells"].append(payload)
                self._notebooks[notebook_key]["updated_at"] = _now()

        return result

    def restart_kernel(self, notebook_key: str) -> bool:
        """Restart the kernel for a notebook (clear all variables)."""
        with self._lock:
            if notebook_key in self._kernels:
                self._kernels[notebook_key].clear()
                return True
            return False

    def get_kernel_variables(self, notebook_key: str) -> dict[str, Any]:
        """Get all variables from a notebook kernel."""
        with self._lock:
            kernel = self._kernels.get(notebook_key)
        if kernel is None:
            return {}
        return kernel.list_variables()

    # ── Experiment Tracking ─────────────────────────────────────────────────

    def create_experiment(
        self,
        experiment_name: str,
        project_code: str,
        description: str = "",
        owner: str = "",
    ) -> dict:
        """Create a new ML experiment within a project."""
        exp_id = f"exp_{uuid.uuid4().hex[:12]}"
        payload = {
            "experiment_id": exp_id,
            "experiment_name": experiment_name,
            "project_code": project_code,
            "description": description,
            "owner": owner,
            "status": "active",
            "tags": [],
            "created_at": _now(),
            "updated_at": _now(),
        }
        with self._lock:
            self._experiments[exp_id] = payload
        if self.persistence:
            self.persistence.upsert_record("research_experiments", "experiment_id", exp_id, payload)
        return payload

    def log_experiment_run(
        self,
        experiment_id: str,
        params: dict,
        metrics: dict,
        artifacts: dict | None = None,
        status: str = "completed",
    ) -> dict:
        """Log a single run of an experiment with parameters and metrics."""
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run_payload = {
            "run_id": run_id,
            "experiment_id": experiment_id,
            "params": params,
            "metrics": metrics,
            "artifacts": artifacts or {},
            "status": status,
            "logged_at": _now(),
        }
        with self._lock:
            self._experiment_runs[experiment_id].append(run_payload)
        if self.persistence:
            self.persistence.upsert_record(
                "research_experiment_runs", "run_id", run_id, run_payload
            )
        return run_payload

    def get_experiment_runs(self, experiment_id: str) -> list[dict]:
        """Get all runs for an experiment, sorted by creation time."""
        with self._lock:
            runs = self._experiment_runs.get(experiment_id, [])
        return sorted(runs, key=lambda r: r.get("logged_at", ""), reverse=True)

    def compare_runs(self, experiment_id: str, metric_keys: list[str]) -> dict:
        """Compare all runs of an experiment on specified metrics."""
        runs = self.get_experiment_runs(experiment_id)
        if not runs:
            return {"runs": [], "best_runs": {}}
        best_runs = {}
        for key in metric_keys:
            best = max(runs, key=lambda r: r.get("metrics", {}).get(key, float("-inf")))
            best_runs[key] = {"run_id": best["run_id"], "value": best.get("metrics", {}).get(key)}
        return {"runs": runs, "best_runs": best_runs}

    # ── Dataset Management ──────────────────────────────────────────────────

    def register_dataset(
        self,
        dataset_name: str,
        dataset_type: str,  # csv, parquet, dataframe, api
        source_path: str = "",
        schema: dict | None = None,
        owner: str = "",
        description: str = "",
    ) -> dict:
        """Register a dataset for use in research projects."""
        dataset_id = f"ds_{uuid.uuid4().hex[:12]}"
        payload = {
            "dataset_id": dataset_id,
            "dataset_name": dataset_name,
            "dataset_type": dataset_type,
            "source_path": source_path,
            "schema": schema or {},
            "owner": owner,
            "description": description,
            "row_count": 0,
            "size_bytes": 0,
            "created_at": _now(),
            "updated_at": _now(),
        }
        with self._lock:
            self._datasets[dataset_id] = payload
        if self.persistence:
            self.persistence.upsert_record("research_datasets", "dataset_id", dataset_id, payload)
        return payload

    def get_dataset(self, dataset_id: str) -> dict | None:
        """Retrieve a dataset by ID."""
        with self._lock:
            return self._datasets.get(dataset_id)

    def list_datasets(self, owner: str | None = None) -> list[dict]:
        """List all registered datasets, optionally filtered by owner."""
        with self._lock:
            results = list(self._datasets.values())
        if owner:
            results = [d for d in results if d.get("owner") == owner]
        return results


class ModelTrainingPipeline:
    """Model training pipeline with experiment tracking and drift monitoring."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._models: dict[str, dict] = {}
        self._model_versions: dict[str, list[dict]] = defaultdict(list)
        self._deployments: dict[str, dict] = {}
        self._drift_metrics: dict[str, list[dict]] = defaultdict(list)
        self._training_jobs: dict[str, dict] = {}
        self._lock = threading.RLock()

    def register_model(
        self,
        model_code: str,
        model_name: str,
        model_type: str,
        description: str = "",
        owner: str = "",
    ) -> dict:
        """Register a new model family."""
        payload = {
            "model_code": model_code,
            "model_name": model_name,
            "model_type": model_type,
            "description": description,
            "owner": owner,
            "state": ModelState.REGISTERED.value,
            "created_at": _now(),
            "updated_at": _now(),
        }

        with self._lock:
            self._models[model_code] = payload

        if self.persistence:
            self.persistence.upsert_record(
                "ml_models",
                "model_code",
                model_code,
                payload,
                extra_columns={"model_name": model_name},
            )

        return payload

    def start_training(
        self,
        model_code: str,
        training_config: dict,
        training_data_id: str,
    ) -> dict:
        """Start a model training job."""
        job_id = f"train:{model_code}:{uuid.uuid4().hex[:8]}"

        with self._lock:
            self._models[model_code]["state"] = ModelState.TRAINING.value
            self._training_jobs[job_id] = {
                "job_id": job_id,
                "model_code": model_code,
                "training_config": training_config,
                "training_data_id": training_data_id,
                "status": "running",
                "started_at": _now(),
            }

        return self._training_jobs[job_id]

    def complete_training(
        self,
        job_id: str,
        metrics: dict,
        artifacts: dict | None = None,
    ) -> dict:
        """Complete a training job and create a new model version."""
        job = self._training_jobs.get(job_id)
        if not job:
            raise ValueError(f"Training job {job_id} not found")

        model_code = job["model_code"]
        version_no = str(len(self._model_versions[model_code]) + 1)

        with self._lock:
            version_code = f"{model_code}:{version_no}"
            version_payload = {
                "version_code": version_code,
                "model_code": model_code,
                "version_no": version_no,
                "metrics": metrics,
                "artifacts": artifacts or {},
                "training_config": job["training_config"],
                "training_data_id": job["training_data_id"],
                "status": "completed",
                "completed_at": _now(),
            }
            self._model_versions[model_code].append(version_payload)
            self._models[model_code]["state"] = ModelState.EVALUATED.value
            self._training_jobs[job_id]["status"] = "completed"

        if self.persistence:
            self.persistence.upsert_record(
                "ml_model_versions",
                "version_code",
                version_code,
                version_payload,
                extra_columns={"model_code": model_code},
            )

        return version_payload

    def deploy_model(
        self,
        model_code: str,
        version_no: str,
        target_environment: str,
        config: dict | None = None,
    ) -> dict:
        """Deploy a model version to an environment."""
        version_code = f"{model_code}:{version_no}"
        deployment_code = f"{version_code}:{target_environment}"

        payload = {
            "deployment_code": deployment_code,
            "model_code": model_code,
            "version_no": version_no,
            "target_environment": target_environment,
            "config": config or {},
            "status": "active",
            "deployed_at": _now(),
        }

        with self._lock:
            self._deployments[deployment_code] = payload
            self._models[model_code]["state"] = ModelState.DEPLOYED.value

        if self.persistence:
            self.persistence.upsert_record(
                "ml_model_deployments",
                "deployment_code",
                deployment_code,
                payload,
                extra_columns={"model_code": model_code},
            )

        return payload

    def record_drift(
        self,
        model_code: str,
        drift_score: float,
        feature_scores: dict | None = None,
    ) -> dict:
        """Record model drift observation."""
        metric_key = f"{model_code}:{uuid.uuid4().hex[:8]}"

        payload = {
            "metric_key": metric_key,
            "model_code": model_code,
            "drift_score": drift_score,
            "feature_scores": feature_scores or {},
            "is_drifted": drift_score > 0.7,
            "recorded_at": _now(),
        }

        with self._lock:
            self._drift_metrics[model_code].append(payload)
            if payload["is_drifted"]:
                self._models[model_code]["state"] = ModelState.DRIFTED.value

        if self.persistence:
            self.persistence.upsert_record(
                "ml_model_drift_metrics",
                "metric_key",
                metric_key,
                payload,
                extra_columns={"model_code": model_code},
            )

        return payload

    def get_drift_status(self, model_code: str) -> dict:
        """Get current drift status for a model."""
        metrics = self._drift_metrics.get(model_code, [])
        if not metrics:
            return {"has_drift": False, "current_score": 0.0}

        latest = metrics[-1]
        return {
            "has_drift": latest["is_drifted"],
            "current_score": latest["drift_score"],
            "recorded_at": latest["recorded_at"],
        }


class ExecutionStateMachine:
    """Execution state machine for order lifecycle management."""

    def __init__(self) -> None:
        self._orders: dict[str, dict] = {}
        self._state_transitions: dict[str, list[dict]] = defaultdict(list)
        self._lock = threading.RLock()

    def create_order(
        self,
        order_id: str,
        instrument_id: str,
        side: str,
        quantity: float,
        order_type: str = "market",
        limit_price: float | None = None,
    ) -> dict:
        """Create a new order in PENDING state."""
        order = {
            "order_id": order_id,
            "instrument_id": instrument_id,
            "side": side,
            "quantity": quantity,
            "filled_quantity": 0.0,
            "order_type": order_type,
            "limit_price": limit_price,
            "state": ExecutionState.PENDING.value,
            "created_at": _now(),
            "updated_at": _now(),
        }

        with self._lock:
            self._orders[order_id] = order
            self._record_transition(order_id, ExecutionState.PENDING.value)

        return order

    def transition(
        self,
        order_id: str,
        new_state: ExecutionState,
        filled_quantity: float | None = None,
        reason: str | None = None,
    ) -> bool:
        """Transition order to a new state."""
        with self._lock:
            if order_id not in self._orders:
                return False

            order = self._orders[order_id]
            old_state = order["state"]

            valid_transitions = {
                ExecutionState.PENDING.value: [
                    ExecutionState.ROUTING.value,
                    ExecutionState.SUBMITTED.value,
                    ExecutionState.CANCELLED.value,
                    ExecutionState.REJECTED.value,
                ],
                ExecutionState.ROUTING.value: [
                    ExecutionState.SUBMITTED.value,
                    ExecutionState.REJECTED.value,
                ],
                ExecutionState.SUBMITTED.value: [
                    ExecutionState.PARTIALLY_FILLED.value,
                    ExecutionState.FILLED.value,
                    ExecutionState.CANCELLED.value,
                    ExecutionState.REJECTED.value,
                ],
                ExecutionState.PARTIALLY_FILLED.value: [
                    ExecutionState.PARTIALLY_FILLED.value,
                    ExecutionState.FILLED.value,
                    ExecutionState.CANCELLED.value,
                ],
            }

            if new_state.value not in valid_transitions.get(old_state, []):
                return False

            order["state"] = new_state.value
            order["updated_at"] = _now()

            if filled_quantity is not None:
                order["filled_quantity"] = filled_quantity

            self._record_transition(order_id, new_state.value, reason)

            return True

    def get_order(self, order_id: str) -> dict | None:
        """Get order by ID."""
        return self._orders.get(order_id)

    def get_order_history(self, order_id: str) -> list[dict]:
        """Get state transition history for an order."""
        return self._state_transitions.get(order_id, [])

    def _record_transition(
        self,
        order_id: str,
        new_state: str,
        reason: str | None = None,
    ) -> None:
        """Record a state transition."""
        transition = {
            "order_id": order_id,
            "to_state": new_state,
            "reason": reason,
            "transitioned_at": _now(),
        }
        self._state_transitions[order_id].append(transition)


class SmartOrderRouter:
    """Smart Order Router with venue selection and optimization."""

    def __init__(self) -> None:
        self._policies: dict[str, dict] = {}
        self._decisions: list[dict] = []
        self._venue_latencies: dict[str, dict] = {}
        self._lock = threading.RLock()

    def register_policy(
        self,
        policy_code: str,
        venues: list[str],
        selection_logic: str = "best_price",
        max_venue_fill_rate: float = 0.95,
    ) -> dict:
        """Register a smart routing policy."""
        payload = {
            "policy_code": policy_code,
            "venues": venues,
            "selection_logic": selection_logic,
            "max_venue_fill_rate": max_venue_fill_rate,
            "created_at": _now(),
        }

        with self._lock:
            self._policies[policy_code] = payload

        return payload

    def select_venue(
        self,
        policy_code: str,
        instrument_id: str,
        side: str,
        quantity: float,
    ) -> dict:
        """Select the best venue for an order based on policy."""
        policy = self._policies.get(policy_code)
        if not policy:
            raise ValueError(f"Policy {policy_code} not found")

        venues = policy["venues"]

        selected_venue = venues[0]
        best_score = float("inf")

        for venue in venues:
            latency = self._venue_latencies.get(venue, {}).get("avg_latency_ms", 100)
            fill_rate = self._venue_latencies.get(venue, {}).get("fill_rate", 0.9)

            score = latency * (1.0 / fill_rate)

            if score < best_score:
                best_score = score
                selected_venue = venue

        decision = {
            "decision_id": f"dec:{uuid.uuid4().hex[:8]}",
            "policy_code": policy_code,
            "instrument_id": instrument_id,
            "side": side,
            "quantity": quantity,
            "selected_venue": selected_venue,
            "score": best_score,
            "decided_at": _now(),
        }

        with self._lock:
            self._decisions.append(decision)

        return decision

    def record_venue_performance(
        self,
        venue: str,
        latency_ms: float,
        fill_rate: float,
    ) -> None:
        """Record venue performance metrics."""
        with self._lock:
            if venue not in self._venue_latencies:
                self._venue_latencies[venue] = {
                    "avg_latency_ms": latency_ms,
                    "fill_rate": fill_rate,
                    "sample_count": 1,
                }
            else:
                current = self._venue_latencies[venue]
                count = current["sample_count"]
                current["avg_latency_ms"] = (current["avg_latency_ms"] * count + latency_ms) / (count + 1)
                current["fill_rate"] = (current["fill_rate"] * count + fill_rate) / (count + 1)
                current["sample_count"] = count + 1


class OptionsStateMachine:
    """Options business state machine with Greeks tracking and exercise logic."""

    def __init__(self) -> None:
        self._positions: dict[str, dict] = {}
        self._chains: dict[str, dict] = {}
        self._exercise_events: list[dict] = []
        self._lock = threading.RLock()

    def register_option_chain(
        self,
        underlying: str,
        expiry: str,
        strikes: list[float],
        option_type: str = "call",
    ) -> dict:
        """Register an option chain for an underlying."""
        chain_code = f"{underlying}:{expiry}:{option_type}"

        payload = {
            "chain_code": chain_code,
            "underlying": underlying,
            "expiry": expiry,
            "option_type": option_type,
            "strikes": strikes,
            "created_at": _now(),
        }

        with self._lock:
            self._chains[chain_code] = payload

        return payload

    def open_position(
        self,
        position_id: str,
        underlying: str,
        expiry: str,
        strike: float,
        option_type: str,
        side: str,
        quantity: float,
        premium: float,
    ) -> dict:
        """Open an options position."""
        payload = {
            "position_id": position_id,
            "underlying": underlying,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "side": side,
            "quantity": quantity,
            "premium": premium,
            "current_price": premium,
            "delta": 0.0,
            "gamma": 0.0,
            "theta": 0.0,
            "vega": 0.0,
            "rho": 0.0,
            "state": "open",
            "opened_at": _now(),
        }

        with self._lock:
            self._positions[position_id] = payload

        return payload

    def update_greeks(
        self,
        position_id: str,
        delta: float,
        gamma: float,
        theta: float,
        vega: float,
        rho: float,
        current_price: float,
    ) -> None:
        """Update Greeks for an options position."""
        with self._lock:
            if position_id in self._positions:
                pos = self._positions[position_id]
                pos["delta"] = delta
                pos["gamma"] = gamma
                pos["theta"] = theta
                pos["vega"] = vega
                pos["rho"] = rho
                pos["current_price"] = current_price

    def exercise_option(
        self,
        position_id: str,
        spot_price: float,
    ) -> dict:
        """Exercise an options position."""
        with self._lock:
            if position_id not in self._positions:
                raise ValueError(f"Position {position_id} not found")

            pos = self._positions[position_id]
            pos["state"] = "exercised"

            event = {
                "event_id": f"exercise:{uuid.uuid4().hex[:8]}",
                "position_id": position_id,
                "spot_price": spot_price,
                "strike": pos["strike"],
                "option_type": pos["option_type"],
                "quantity": pos["quantity"],
                "exercised_at": _now(),
            }

            self._exercise_events.append(event)
            return event


class MarketMakingService:
    """Market making service with spread management and inventory tracking."""

    def __init__(self) -> None:
        self._configs: dict[str, dict] = {}
        self._inventories: dict[str, dict] = {}
        self._orders: dict[str, dict] = {}
        self._lock = threading.RLock()

    def create_config(
        self,
        symbol: str,
        base_spread_bps: float,
        max_inventory: float,
        inventory_skew_factor: float = 0.5,
    ) -> dict:
        """Create a market making configuration."""
        config_code = f"mm:{symbol}"

        payload = {
            "config_code": config_code,
            "symbol": symbol,
            "base_spread_bps": base_spread_bps,
            "max_inventory": max_inventory,
            "inventory_skew_factor": inventory_skew_factor,
            "status": "active",
            "created_at": _now(),
        }

        with self._lock:
            self._configs[config_code] = payload

        return payload

    def update_inventory(
        self,
        symbol: str,
        position: float,
        avg_cost: float,
    ) -> None:
        """Update inventory for a symbol."""
        with self._lock:
            self._inventories[symbol] = {
                "symbol": symbol,
                "position": position,
                "avg_cost": avg_cost,
                "unrealized_pnl": 0.0,
                "updated_at": _now(),
            }

    def calculate_bid_ask(
        self,
        symbol: str,
        mid_price: float,
    ) -> tuple[float, float]:
        """Calculate bid and ask prices based on configuration."""
        config = self._configs.get(f"mm:{symbol}")
        if not config:
            spread = 0.001
            return mid_price * (1 - spread / 2), mid_price * (1 + spread / 2)

        inventory = self._inventories.get(symbol, {}).get("position", 0)
        max_inv = config["max_inventory"]
        skew = config["inventory_skew_factor"]

        inventory_skew = (inventory / max_inv) * skew if max_inv > 0 else 0

        base_spread = config["base_spread_bps"] / 10000.0
        bid_price = mid_price * (1 - base_spread / 2 - inventory_skew)
        ask_price = mid_price * (1 + base_spread / 2 + inventory_skew)

        return bid_price, ask_price


class DEXLiquidityService:
    """DEX liquidity service with pool tracking and LP position management."""

    def __init__(self) -> None:
        self._pools: dict[str, dict] = {}
        self._positions: dict[str, dict] = {}
        self._transactions: list[dict] = []
        self._lock = threading.RLock()

    def register_pool(
        self,
        pool_code: str,
        token0: str,
        token1: str,
        fee_tier: str = "0.30",
        total_liquidity: float = 0.0,
    ) -> dict:
        """Register a DEX pool."""
        payload = {
            "pool_code": pool_code,
            "token0": token0,
            "token1": token1,
            "fee_tier": fee_tier,
            "total_liquidity": total_liquidity,
            "created_at": _now(),
        }

        with self._lock:
            self._pools[pool_code] = payload

        return payload

    def add_liquidity(
        self,
        position_id: str,
        pool_code: str,
        amount0: float,
        amount1: float,
        share_ratio: float,
    ) -> dict:
        """Add liquidity to a pool."""
        payload = {
            "position_id": position_id,
            "pool_code": pool_code,
            "amount0": amount0,
            "amount1": amount1,
            "share_ratio": share_ratio,
            "fees_earned": 0.0,
            "state": "active",
            "created_at": _now(),
        }

        with self._lock:
            self._positions[position_id] = payload

            if pool_code in self._pools:
                self._pools[pool_code]["total_liquidity"] += amount0 + amount1

        return payload

    def remove_liquidity(
        self,
        position_id: str,
    ) -> dict:
        """Remove liquidity from a pool."""
        with self._lock:
            if position_id not in self._positions:
                raise ValueError(f"Position {position_id} not found")

            pos = self._positions[position_id]
            pos["state"] = "removed"

            pool_code = pos["pool_code"]
            if pool_code in self._pools:
                self._pools[pool_code]["total_liquidity"] -= pos["amount0"] + pos["amount1"]

            return pos

    def record_swap(
        self,
        pool_code: str,
        token_in: str,
        token_out: str,
        amount_in: float,
        amount_out: float,
        fee: float,
    ) -> dict:
        """Record a swap transaction."""
        payload = {
            "swap_id": f"swap:{uuid.uuid4().hex[:8]}",
            "pool_code": pool_code,
            "token_in": token_in,
            "token_out": token_out,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "fee": fee,
            "swapped_at": _now(),
        }

        with self._lock:
            self._transactions.append(payload)

        return payload
