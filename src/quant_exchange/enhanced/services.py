"""First-pass implementations for enhanced platform capabilities."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

from quant_exchange.core.models import Instrument, Kline
from quant_exchange.strategy.factors import momentum, rsi, sma


def _now() -> str:
    """Return an ISO-8601 timestamp string."""

    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class UniverseDefinition:
    """Metadata for a dynamic or static tradeable universe."""

    universe_code: str
    universe_name: str
    asset_class: str
    scope_type: str


class UniverseService:
    """Manage universes, screener rules, and rebuildable snapshots."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.universes: dict[str, UniverseDefinition] = {}
        self.rules: dict[str, list[dict]] = {}
        self.snapshots: dict[str, dict] = {}

    def create_universe(self, universe_code: str, universe_name: str, asset_class: str, scope_type: str = "DYNAMIC") -> dict:
        """Create and persist a new universe definition."""

        universe = UniverseDefinition(universe_code, universe_name, asset_class, scope_type)
        self.universes[universe_code] = universe
        self.rules.setdefault(universe_code, [])
        payload = asdict(universe)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "universe_universes",
                "universe_code",
                universe_code,
                payload,
                extra_columns={"universe_name": universe_name},
            )
        return payload

    def add_rule(self, universe_code: str, field_name: str, operator: str, value) -> dict:
        """Append a screener rule to a universe."""

        rule = {
            "rule_key": f"{universe_code}:{len(self.rules.get(universe_code, []))}",
            "field_name": field_name,
            "operator": operator,
            "value": value,
        }
        self.rules.setdefault(universe_code, []).append(rule)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "universe_rules",
                "rule_key",
                rule["rule_key"],
                rule,
                extra_columns={"universe_code": universe_code},
            )
        return rule

    def rebuild_snapshot(self, universe_code: str, instruments: list[Instrument]) -> dict:
        """Evaluate all rules and produce a deterministic universe snapshot."""

        selected: list[str] = []
        for instrument in instruments:
            if self._matches(instrument, self.rules.get(universe_code, [])):
                selected.append(instrument.instrument_id)
        snapshot = {
            "snapshot_key": f"{universe_code}:{_now()}",
            "universe_code": universe_code,
            "instrument_ids": selected,
            "created_at": _now(),
        }
        self.snapshots[universe_code] = snapshot
        if self.persistence is not None:
            self.persistence.upsert_record(
                "universe_snapshots",
                "snapshot_key",
                snapshot["snapshot_key"],
                snapshot,
                extra_columns={"universe_code": universe_code},
            )
        return snapshot

    def _matches(self, instrument: Instrument, rules: list[dict]) -> bool:
        for rule in rules:
            candidate = getattr(instrument, rule["field_name"], None)
            if hasattr(candidate, "value"):
                candidate = candidate.value
            operator = rule["operator"]
            expected = rule["value"]
            if operator == "eq" and candidate != expected:
                return False
            if operator == "in" and candidate not in expected:
                return False
        return True


class FeatureStoreService:
    """Manage feature definitions, versions, and computed values."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.definitions: dict[str, dict] = {}
        self.versions: dict[str, list[dict]] = {}
        self.values: list[dict] = []

    def create_feature(self, feature_code: str, feature_name: str, expression: str) -> dict:
        """Register a feature definition."""

        payload = {
            "feature_code": feature_code,
            "feature_name": feature_name,
            "expression": expression,
            "created_at": _now(),
        }
        self.definitions[feature_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record(
                "feature_definitions",
                "feature_code",
                feature_code,
                payload,
                extra_columns={"feature_name": feature_name},
            )
        return payload

    def publish_version(self, feature_code: str, version_no: str) -> dict:
        """Publish a feature version for later reproducibility."""

        payload = {
            "version_key": f"{feature_code}:{version_no}",
            "feature_code": feature_code,
            "version_no": version_no,
            "expression": self.definitions[feature_code]["expression"],
            "created_at": _now(),
        }
        self.versions.setdefault(feature_code, []).append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "feature_versions",
                "version_key",
                payload["version_key"],
                payload,
                extra_columns={"feature_code": feature_code},
            )
        return payload

    def compute_and_store(self, feature_code: str, instrument_id: str, bars: list[Kline]) -> dict:
        """Compute a feature value from bars and persist the result."""

        expression = self.definitions[feature_code]["expression"]
        closes = [bar.close for bar in bars]
        value = self._evaluate_expression(expression, closes)
        payload = {
            "value_key": f"{feature_code}:{instrument_id}:{bars[-1].close_time.isoformat()}",
            "feature_code": feature_code,
            "instrument_id": instrument_id,
            "event_time": bars[-1].close_time.isoformat(),
            "value": value,
            "created_at": _now(),
        }
        self.values.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "feature_values",
                "value_key",
                payload["value_key"],
                payload,
                extra_columns={
                    "feature_code": feature_code,
                    "instrument_id": instrument_id,
                    "event_time": payload["event_time"],
                },
            )
        return payload

    def _evaluate_expression(self, expression: str, closes: list[float]) -> float:
        kind, _, raw_window = expression.partition(":")
        window = int(raw_window or "1")
        if kind == "sma":
            return sma(closes, window)
        if kind == "momentum":
            return momentum(closes, window)
        if kind == "rsi":
            return rsi(closes, window)
        raise ValueError(f"Unsupported feature expression: {expression}")


class ResearchMlService:
    """Manage research projects, experiments, and model registry metadata."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.projects: dict[str, dict] = {}
        self.notebooks: list[dict] = []
        self.datasets: dict[str, dict] = {}
        self.experiments: dict[str, dict] = {}
        self.experiment_runs: list[dict] = []
        self.models: dict[str, dict] = {}
        self.model_versions: list[dict] = []
        self.model_deployments: list[dict] = []
        self.drift_metrics: list[dict] = []

    def create_project(self, project_code: str, project_name: str) -> dict:
        """Create a research project."""

        payload = {"project_code": project_code, "project_name": project_name, "created_at": _now()}
        self.projects[project_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record(
                "research_projects",
                "project_code",
                project_code,
                payload,
                extra_columns={"project_name": project_name},
            )
        return payload

    def register_notebook(self, project_code: str, notebook_name: str) -> dict:
        """Attach a notebook record to a research project."""

        payload = {
            "notebook_key": f"{project_code}:{notebook_name}",
            "project_code": project_code,
            "notebook_name": notebook_name,
            "created_at": _now(),
        }
        self.notebooks.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "research_notebooks",
                "notebook_key",
                payload["notebook_key"],
                payload,
                extra_columns={"project_code": project_code},
            )
        return payload

    def register_dataset(self, dataset_code: str, description: str) -> dict:
        """Register a research dataset."""

        payload = {"dataset_code": dataset_code, "description": description, "created_at": _now()}
        self.datasets[dataset_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("research_datasets", "dataset_code", dataset_code, payload)
        return payload

    def create_experiment(self, experiment_code: str, experiment_name: str) -> dict:
        """Create an experiment container."""

        payload = {
            "experiment_code": experiment_code,
            "experiment_name": experiment_name,
            "created_at": _now(),
        }
        self.experiments[experiment_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_experiments",
                "experiment_code",
                experiment_code,
                payload,
                extra_columns={"experiment_name": experiment_name},
            )
        return payload

    def create_experiment_run(self, experiment_code: str, metrics: dict) -> dict:
        """Persist one experiment run."""

        payload = {
            "run_code": f"{experiment_code}:{uuid4().hex[:8]}",
            "experiment_code": experiment_code,
            "metrics": metrics,
            "created_at": _now(),
        }
        self.experiment_runs.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_experiment_runs",
                "run_code",
                payload["run_code"],
                payload,
                extra_columns={"experiment_code": experiment_code},
            )
        return payload

    def register_model(self, model_code: str, model_name: str) -> dict:
        """Register a model family."""

        payload = {"model_code": model_code, "model_name": model_name, "created_at": _now()}
        self.models[model_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_models",
                "model_code",
                model_code,
                payload,
                extra_columns={"model_name": model_name},
            )
        return payload

    def publish_model_version(self, model_code: str, version_no: str) -> dict:
        """Publish a model version."""

        payload = {
            "version_code": f"{model_code}:{version_no}",
            "model_code": model_code,
            "version_no": version_no,
            "created_at": _now(),
        }
        self.model_versions.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_model_versions",
                "version_code",
                payload["version_code"],
                payload,
                extra_columns={"model_code": model_code},
            )
        return payload

    def deploy_model(self, model_code: str, target: str) -> dict:
        """Create a model deployment record."""

        payload = {
            "deployment_code": f"{model_code}:{target}",
            "model_code": model_code,
            "target": target,
            "status": "ACTIVE",
            "created_at": _now(),
        }
        self.model_deployments.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_model_deployments",
                "deployment_code",
                payload["deployment_code"],
                payload,
                extra_columns={"model_code": model_code},
            )
        return payload

    def record_drift(self, model_code: str, score: float) -> dict:
        """Store one model drift observation."""

        payload = {
            "metric_key": f"{model_code}:{uuid4().hex[:8]}",
            "model_code": model_code,
            "score": score,
            "created_at": _now(),
        }
        self.drift_metrics.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "ml_model_drift_metrics",
                "metric_key",
                payload["metric_key"],
                payload,
                extra_columns={"model_code": model_code},
            )
        return payload


class BiasAuditService:
    """Run simple bias checks and persist their outputs."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.jobs: dict[str, dict] = {}
        self.results: list[dict] = []

    def create_job(self, audit_type: str, target_type: str, target_ref: str) -> dict:
        """Register an audit job."""

        payload = {
            "audit_job_code": f"audit:{uuid4().hex[:8]}",
            "audit_type": audit_type,
            "target_type": target_type,
            "target_ref": target_ref,
            "created_at": _now(),
        }
        self.jobs[payload["audit_job_code"]] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("audit_jobs", "audit_job_code", payload["audit_job_code"], payload)
        return payload

    def run_lookahead_audit(self, audit_job_code: str, timestamps: list[datetime]) -> dict:
        """Check whether timestamps are strictly non-decreasing."""

        has_issue = any(timestamps[idx - 1] > timestamps[idx] for idx in range(1, len(timestamps)))
        payload = {
            "result_key": f"{audit_job_code}:result",
            "audit_job_code": audit_job_code,
            "status": "FAILED" if has_issue else "PASSED",
            "issues": ["lookahead_bias_detected"] if has_issue else [],
            "created_at": _now(),
        }
        self.results.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "audit_results",
                "result_key",
                payload["result_key"],
                payload,
                extra_columns={"audit_job_code": audit_job_code},
            )
        return payload


class ReplayService:
    """Store replayable event logs, snapshots, and shadow deployment summaries."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.events: list[dict] = []
        self.snapshots: list[dict] = []
        self.jobs: dict[str, dict] = {}
        self.shadow_deployments: dict[str, dict] = {}

    def append_event(self, event_type: str, payload: dict) -> dict:
        """Persist one replayable event."""

        record = {"event_code": f"evt:{uuid4().hex[:8]}", "event_type": event_type, "payload": payload, "created_at": _now()}
        self.events.append(record)
        if self.persistence is not None:
            self.persistence.upsert_record("replay_event_logs", "event_code", record["event_code"], record)
        return record

    def create_snapshot(self, state_name: str, state: dict) -> dict:
        """Persist one state snapshot."""

        record = {"snapshot_code": f"snap:{uuid4().hex[:8]}", "state_name": state_name, "state": state, "created_at": _now()}
        self.snapshots.append(record)
        if self.persistence is not None:
            self.persistence.upsert_record("replay_state_snapshots", "snapshot_code", record["snapshot_code"], record)
        return record

    def create_replay_job(self, source: str) -> dict:
        """Create a replay job descriptor."""

        record = {"replay_job_code": f"replay:{uuid4().hex[:8]}", "source": source, "status": "CREATED", "created_at": _now()}
        self.jobs[record["replay_job_code"]] = record
        if self.persistence is not None:
            self.persistence.upsert_record("replay_jobs", "replay_job_code", record["replay_job_code"], record)
        return record

    def create_shadow_deployment(self, baseline: str, candidate: str, diff_score: float) -> dict:
        """Persist a shadow deployment comparison."""

        record = {
            "shadow_code": f"shadow:{uuid4().hex[:8]}",
            "baseline": baseline,
            "candidate": candidate,
            "diff_score": diff_score,
            "created_at": _now(),
        }
        self.shadow_deployments[record["shadow_code"]] = record
        if self.persistence is not None:
            self.persistence.upsert_record("replay_shadow_deployments", "shadow_code", record["shadow_code"], record)
        return record


class LedgerService:
    """Manage virtual accounts and internal ledger transfers."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.virtual_accounts: dict[str, dict] = {}
        self.entries: list[dict] = []
        self.transfers: list[dict] = []

    def create_virtual_account(self, account_code: str, currency: str, balance: float = 0.0) -> dict:
        """Create an internal virtual account."""

        payload = {"account_code": account_code, "currency": currency, "balance": balance, "created_at": _now()}
        self.virtual_accounts[account_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("ledger_virtual_accounts", "account_code", account_code, payload)
        return payload

    def add_entry(self, account_code: str, amount: float, entry_type: str) -> dict:
        """Add a ledger entry and update the virtual account balance."""

        self.virtual_accounts[account_code]["balance"] += amount
        payload = {
            "entry_code": f"entry:{uuid4().hex[:8]}",
            "account_code": account_code,
            "amount": amount,
            "entry_type": entry_type,
            "created_at": _now(),
        }
        self.entries.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record("ledger_entries", "entry_code", payload["entry_code"], payload)
            self.persistence.upsert_record("ledger_virtual_accounts", "account_code", account_code, self.virtual_accounts[account_code])
        return payload

    def transfer(self, from_account: str, to_account: str, amount: float) -> dict:
        """Transfer funds between virtual accounts."""

        self.add_entry(from_account, -amount, "TRANSFER_OUT")
        self.add_entry(to_account, amount, "TRANSFER_IN")
        payload = {
            "transfer_code": f"transfer:{uuid4().hex[:8]}",
            "from_account": from_account,
            "to_account": to_account,
            "amount": amount,
            "created_at": _now(),
        }
        self.transfers.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record("ledger_transfers", "transfer_code", payload["transfer_code"], payload)
        return payload


class AlternativeDataService:
    """Store alternative data metadata and records."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.sources: dict[str, dict] = {}
        self.datasets: dict[str, dict] = {}
        self.records: list[dict] = []

    def create_source(self, source_code: str, description: str) -> dict:
        """Register an alternative data source."""

        payload = {"source_code": source_code, "description": description, "created_at": _now()}
        self.sources[source_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("alt_data_sources", "source_code", source_code, payload)
        return payload

    def create_dataset(self, dataset_code: str, source_code: str) -> dict:
        """Register an alternative dataset."""

        payload = {"dataset_code": dataset_code, "source_code": source_code, "created_at": _now()}
        self.datasets[dataset_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("alt_datasets", "dataset_code", dataset_code, payload)
        return payload

    def add_record(self, dataset_code: str, record: dict) -> dict:
        """Persist one alternative data record."""

        payload = {
            "record_key": f"{dataset_code}:{uuid4().hex[:8]}",
            "dataset_code": dataset_code,
            "record": record,
            "created_at": _now(),
        }
        self.records.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "alt_dataset_records",
                "record_key",
                payload["record_key"],
                payload,
                extra_columns={"dataset_code": dataset_code},
            )
        return payload


class AdvancedExecutionService:
    """Store execution algorithms, order baskets, and router metadata."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.algorithms: dict[str, dict] = {}
        self.baskets: dict[str, dict] = {}
        self.router_policies: dict[str, dict] = {}
        self.router_decisions: list[dict] = []

    def register_algorithm(self, algorithm_code: str, config: dict) -> dict:
        """Register an execution algorithm."""

        payload = {"algorithm_code": algorithm_code, "config": config, "created_at": _now()}
        self.algorithms[algorithm_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("ems_execution_algorithms", "algorithm_code", algorithm_code, payload)
        return payload

    def create_router_policy(self, policy_code: str, venues: list[str]) -> dict:
        """Create a smart routing policy."""

        payload = {"policy_code": policy_code, "venues": venues, "created_at": _now()}
        self.router_policies[policy_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("ems_router_policies", "policy_code", policy_code, payload)
        return payload

    def create_order_basket(self, basket_code: str, orders: list[dict]) -> dict:
        """Create a parent order basket."""

        payload = {"basket_code": basket_code, "orders": orders, "created_at": _now()}
        self.baskets[basket_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("ems_order_baskets", "basket_code", basket_code, payload)
        return payload

    def record_router_decision(self, policy_code: str, decision: dict) -> dict:
        """Store one router decision."""

        payload = {"decision_code": f"route:{uuid4().hex[:8]}", "policy_code": policy_code, "decision": decision, "created_at": _now()}
        self.router_decisions.append(payload)
        if self.persistence is not None:
            self.persistence.upsert_record("ems_router_decisions", "decision_code", payload["decision_code"], payload)
        return payload


class DerivativesDexService:
    """Store option, market-making, and DEX liquidity metadata."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self.option_chains: dict[str, dict] = {}
        self.mm_configs: dict[str, dict] = {}
        self.dex_positions: dict[str, dict] = {}

    def register_option_chain(self, chain_code: str, symbol: str, expiries: list[str]) -> dict:
        """Register an option chain summary."""

        payload = {"chain_code": chain_code, "symbol": symbol, "expiries": expiries, "created_at": _now()}
        self.option_chains[chain_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("opt_option_chains", "chain_code", chain_code, payload)
        return payload

    def create_market_making_config(self, config_code: str, symbol: str, spread_bps: float) -> dict:
        """Register a market-making configuration."""

        payload = {"config_code": config_code, "symbol": symbol, "spread_bps": spread_bps, "created_at": _now()}
        self.mm_configs[config_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("mm_market_making_configs", "config_code", config_code, payload)
        return payload

    def upsert_dex_position(self, position_code: str, pool: str, liquidity: float) -> dict:
        """Store a DEX LP position snapshot."""

        payload = {"position_code": position_code, "pool": pool, "liquidity": liquidity, "created_at": _now()}
        self.dex_positions[position_code] = payload
        if self.persistence is not None:
            self.persistence.upsert_record("dex_liquidity_positions", "position_code", position_code, payload)
        return payload
