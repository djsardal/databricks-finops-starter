"""YAML control plane: loader, resolver and validator.

All ingestion behavior is driven by the ``conf/`` tree. Loading an effective
configuration for one environment/region/workspace resolves every reference
(schedules, contracts, quality rules, allocation policy) and validates:
structure/enums, unique IDs, references, watermarks and keys against the
declared source contracts, region/residency compatibility, target-table
collisions, destructive-schema-change guards, and secret leakage.

Onboarding a new workspace, region or source is configuration-only.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

log = logging.getLogger("finops.config")

ALLOWED_ENVIRONMENTS = {"dev", "test", "prod"}
ALLOWED_DEPLOYMENT_MODES = {
    "single_workspace",
    "multi_workspace_single_region",
    "multi_region_hub_spoke",
    "regional_federated",
}
ALLOWED_SOURCE_TYPES = {"databricks_system_table", "derived_system_table", "workspace_api"}
ALLOWED_INGESTION_MODES = {"incremental", "append", "merge", "watermark", "lookback_merge", "snapshot", "reconcile"}
ALLOWED_DRIFT_ADDITIVE = {"capture_and_review", "fail_source"}
ALLOWED_DRIFT_TYPE_CHANGE = {"quarantine_and_alert", "fail_source"}
ALLOWED_MISSING_COLUMNS = {"fail_source", "skip_source"}
ALLOWED_QA_ACTIONS = {"quarantine", "warn", "fail_source"}
ALLOWED_WRITE_STRATEGIES = {"merge", "append"}

# Hard-failure secret leakage patterns (tokens / private keys / assigned values).
SECRET_ERROR_PATTERNS = {
    "databricks_token": re.compile(r"dapi-[0-9a-f]{20,}"),
    "private_key_block": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "assigned_credential_value": re.compile(
        r"(?im)^\s*#?\s*(password|passwd|client_secret|api_key|access_token|secret_key)"
        r"\s*[:=]\s*[^\s{][^\n]{7,}\s*$"
    ),
}
# Warning-level: keys that reference credentials but hold references/placeholders.
SECRET_WARNING_PATTERNS = {
    "credential_reference_key": re.compile(
        r"(?im)^\s*#?\s*(secret_scope|credential_reference|token_reference)\s*:\s*[^\n]*$"
    ),
}

IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class ConfigValidationError(ValueError):
    """Raised when configuration fails validation with blocking errors."""


@dataclass(frozen=True)
class ValidationIssue:
    level: str  # ERROR | WARNING
    code: str
    message: str
    file: str = ""

    def __str__(self) -> str:  # pragma: no cover - logging sugar
        return f"[{self.level}] {self.code}: {self.message} ({self.file})"


@dataclass
class SourceConfig:
    """One ingestable source, resolved from conf/sources/*.yml."""

    source_id: str
    enabled: bool
    required: bool
    source_type: str
    source_object: str
    target_schema: str
    target_table: str
    mode: str
    watermark_column: Optional[str]
    watermark_type: Optional[str]
    lookback_hours: int
    primary_key_columns: list[str]
    merge_columns: list[str]
    write_strategy: str
    batch_size: int
    schedule_ref: str
    source_record_id_column: Optional[str]
    source_record_id_expression: Optional[str]
    event_time_column: Optional[str]
    updated_time_column: Optional[str]
    contract_ref: str
    additive_columns: str
    unexpected_type_change: str
    missing_required_columns: str
    quality_rules: list[str]
    bronze_days: int
    silver_days: int
    derived_from: Optional[str] = None
    derived_predicate: Optional[str] = None
    file: str = ""

    @property
    def is_derived(self) -> bool:
        return self.source_type == "derived_system_table"

    @classmethod
    def from_dict(cls, d: dict, file: str) -> "SourceConfig":
        ing = d.get("ingestion") or {}
        keys = d.get("derived_keys") or {}
        sch = d.get("schema") or {}
        tgt = d.get("target") or {}
        ret = d.get("retention") or {}
        return cls(
            source_id=d["source_id"],
            enabled=bool(d.get("enabled", True)),
            required=bool(d.get("required", False)),
            source_type=d["source_type"],
            source_object=d["source_object"],
            target_schema=tgt.get("schema", "bronze"),
            target_table=tgt["table"],
            mode=ing.get("mode", "incremental"),
            watermark_column=ing.get("watermark_column"),
            watermark_type=ing.get("watermark_type"),
            lookback_hours=int(ing.get("lookback_hours", 72)),
            primary_key_columns=list(ing.get("primary_key_columns", [])),
            merge_columns=list(ing.get("merge_columns", [])),
            write_strategy=ing.get("write_strategy", "merge"),
            batch_size=int(ing.get("batch_size", 100000)),
            schedule_ref=ing.get("schedule_ref", "standard_hourly"),
            source_record_id_column=keys.get("source_record_id_column"),
            source_record_id_expression=keys.get("source_record_id_expression"),
            event_time_column=keys.get("event_time_column"),
            updated_time_column=keys.get("updated_time_column"),
            contract_ref=sch.get("contract_ref", ""),
            additive_columns=sch.get("additive_columns", "capture_and_review"),
            unexpected_type_change=sch.get("unexpected_type_change", "quarantine_and_alert"),
            missing_required_columns=sch.get("missing_required_columns", "fail_source"),
            quality_rules=list(d.get("quality_rules", [])),
            bronze_days=int(ret.get("bronze_days", 400)),
            silver_days=int(ret.get("silver_days", 400)),
            derived_from=d.get("derived_from"),
            derived_predicate=d.get("derived_predicate"),
            file=file,
        )


@dataclass
class EffectiveConfig:
    """Fully resolved configuration for one environment/region/workspace run."""

    environment: str
    deployment_mode: str
    config_version: str
    platform: dict
    ingestion: dict
    security: dict
    recovery: dict
    freshness_slo: dict
    reliability: dict
    region: dict
    workspace: dict
    sources: dict[str, SourceConfig]
    schedules: dict
    contracts: dict
    quality_rules: dict
    allocation: dict
    regional_catalog: str
    central_catalog: str
    raw_texts: dict[str, str] = field(repr=False, default_factory=dict)

    @property
    def enabled_sources(self) -> list[SourceConfig]:
        return [s for s in self.sources.values() if s.enabled]

    def to_dict(self) -> dict:
        """Canonical dict (stable ordering) used for the effective-config hash."""
        return {
            "environment": self.environment,
            "deployment_mode": self.deployment_mode,
            "config_version": self.config_version,
            "platform": self.platform,
            "ingestion": self.ingestion,
            "recovery": self.recovery,
            "reliability": self.reliability,
            "region": {k: v for k, v in self.region.items() if k != "file"},
            "workspace": {k: v for k, v in self.workspace.items() if k != "file"},
            "sources": {
                sid: {k: v for k, v in s.__dict__.items() if k != "file"}
                for sid, s in sorted(self.sources.items())
            },
            "schedules": self.schedules,
            "contracts": self.contracts,
            "quality_rules": self.quality_rules,
        }

    @property
    def effective_hash(self) -> str:
        """SHA-256 over the canonical effective configuration."""
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()

    # ------------------------------------------------------------------ loader
    @classmethod
    def load(
        cls,
        conf_root: str | Path,
        environment: str,
        region_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
    ) -> "EffectiveConfig":
        root = Path(conf_root)
        raw_texts: dict[str, str] = {}

        def _read(rel: str) -> dict:
            path = root / rel
            text = path.read_text(encoding="utf-8")
            raw_texts[rel] = text
            return yaml.safe_load(text) or {}

        env = _read(f"environments/{environment}.yml")
        platform = env.get("platform") or {}

        # Workspace registry entry: default to the single enabled workspace file.
        ws_dir = root / "workspaces"
        ws_files = sorted(p for p in ws_dir.glob("*.yml"))
        if workspace_id is None and len(ws_files) == 1:
            workspace = _read(f"workspaces/{ws_files[0].name}")
        elif workspace_id is not None:
            workspace = None
            for p in ws_files:
                candidate = _read(f"workspaces/{p.name}")
                if str(candidate.get("workspace_id")) == str(workspace_id):
                    workspace = candidate
                    break
            if workspace is None:
                raise ConfigValidationError(f"workspace_id {workspace_id} not found in conf/workspaces")
        else:
            raise ConfigValidationError("multiple workspace files: pass workspace_id explicitly")

        region_id = region_id or workspace.get("region_id")
        region = _read(f"regions/{region_id}.yml")

        schedules = _read("schedules/ingestion_schedule.yml").get("schedules") or {}
        contracts = _read("schemas/source_contracts.yml").get("contracts") or {}
        quality_rules = _read("quality/quality_rules.yml").get("rules") or {}
        allocation = _read("mappings/cost_allocation.yml")

        sources: dict[str, SourceConfig] = {}
        for p in sorted((root / "sources").glob("*.yml")):
            d = _read(f"sources/{p.name}")
            src = SourceConfig.from_dict(d, file=f"sources/{p.name}")
            if src.source_id in sources:
                raise ConfigValidationError(f"duplicate source_id {src.source_id} in {p}")
            sources[src.source_id] = src

        # Derived sources: resolve predicate/origin from the contract when the
        # source file defers to it (single source of truth stays the contract).
        for src in sources.values():
            if src.is_derived and not src.derived_predicate:
                contract = contracts.get(src.contract_ref) or {}
                src.derived_predicate = contract.get("derived_predicate")

        central_pattern = platform.get(
            "central_catalog_pattern", "finops_{environment}_global"
        )
        cfg = cls(
            environment=env.get("environment", environment),
            deployment_mode=env.get("deployment_mode", "single_workspace"),
            config_version=str(env.get("config_version", "0.0.0")),
            platform=platform,
            ingestion=env.get("ingestion") or {},
            security=env.get("security") or {},
            recovery=env.get("recovery") or {},
            freshness_slo=env.get("freshness_slo") or {},
            reliability=env.get("reliability") or {},
            region=region,
            workspace=workspace,
            sources=sources,
            schedules=schedules,
            contracts=contracts,
            quality_rules=quality_rules,
            allocation=allocation,
            regional_catalog=region.get("regional_catalog")
            or f"finops_{environment}_{region_id}",
            central_catalog=central_pattern.format(environment=environment),
            raw_texts=raw_texts,
        )
        issues = validate_config(cfg)
        errors = [i for i in issues if i.level == "ERROR"]
        if errors:
            raise ConfigValidationError("; ".join(str(e) for e in errors))
        for warn in (i for i in issues if i.level == "WARNING"):
            log.warning("config warning: %s", warn)
        return cfg


def _norm_type(t: str) -> str:
    """Normalize a type string for drift comparison."""
    t = str(t).strip().lower()
    if t.startswith("struct"):
        return "struct"
    if t.startswith("array"):
        return "array"
    return t


def validate_config(cfg: EffectiveConfig) -> list[ValidationIssue]:
    """Validate a resolved configuration; returns ERROR/WARNING issues."""
    issues: list[ValidationIssue] = []

    def err(code: str, message: str, file: str = ""):
        issues.append(ValidationIssue("ERROR", code, message, file))

    def warn(code: str, message: str, file: str = ""):
        issues.append(ValidationIssue("WARNING", code, message, file))

    # --- environment-level --------------------------------------------------
    if cfg.environment not in ALLOWED_ENVIRONMENTS:
        err("environment_invalid", f"environment must be one of {sorted(ALLOWED_ENVIRONMENTS)}")
    if cfg.deployment_mode not in ALLOWED_DEPLOYMENT_MODES:
        err("deployment_mode_invalid", f"unknown deployment_mode {cfg.deployment_mode}")
    if not re.match(r"^\d+\.\d+\.\d+$", cfg.config_version):
        err("config_version_invalid", "config_version must be semver-like X.Y.Z")
    if cfg.platform.get("cloud") not in {"aws", "azure", "gcp"}:
        err("cloud_invalid", "platform.cloud must be aws|azure|gcp")
    if not IDENTIFIER_RE.match(cfg.regional_catalog) or not IDENTIFIER_RE.match(cfg.central_catalog):
        err("catalog_name_invalid", f"catalog names must match {IDENTIFIER_RE.pattern}")
    ing = cfg.ingestion
    if ing.get("allow_destructive_schema_change", False):
        err("destructive_schema_change_forbidden", "allow_destructive_schema_change must be false")

    # --- region / workspace / residency ------------------------------------
    region_id = cfg.region.get("region_id")
    if cfg.workspace.get("region_id") != region_id:
        err(
            "region_mismatch",
            f"workspace region_id {cfg.workspace.get('region_id')} != region file {region_id}",
        )
    if cfg.region.get("cloud") != cfg.platform.get("cloud"):
        err("cloud_mismatch", "region cloud differs from platform cloud")
    if cfg.region.get("central_transfer", {}).get("enabled") and cfg.deployment_mode == "single_workspace":
        warn("transfer_enabled_single_workspace", "central transfer enabled in single_workspace mode")

    # --- sources ------------------------------------------------------------
    workspace_sources = set(cfg.workspace.get("sources") or [])
    for sid in workspace_sources - set(cfg.sources):
        err("source_reference_unknown", f"workspace references unknown source {sid}")

    targets: dict[str, str] = {}
    for src in cfg.sources.values():
        f = src.file
        if src.source_type not in ALLOWED_SOURCE_TYPES:
            err("source_type_invalid", f"{src.source_id}: bad source_type", f)
        if src.mode not in ALLOWED_INGESTION_MODES:
            err("mode_invalid", f"{src.source_id}: bad ingestion.mode {src.mode}", f)
        if src.write_strategy not in ALLOWED_WRITE_STRATEGIES:
            err("write_strategy_invalid", f"{src.source_id}: bad write_strategy", f)
        if src.schedule_ref not in cfg.schedules:
            err("schedule_ref_unknown", f"{src.source_id}: unknown schedule_ref {src.schedule_ref}", f)
        if src.contract_ref not in cfg.contracts:
            err("contract_ref_unknown", f"{src.source_id}: unknown contract_ref {src.contract_ref}", f)
        if src.additive_columns not in ALLOWED_DRIFT_ADDITIVE:
            err("additive_policy_invalid", f"{src.source_id}: bad additive_columns policy", f)
        if src.unexpected_type_change not in ALLOWED_DRIFT_TYPE_CHANGE:
            err("type_change_policy_invalid", f"{src.source_id}: bad unexpected_type_change policy", f)
        if src.missing_required_columns not in ALLOWED_MISSING_COLUMNS:
            err("missing_columns_policy_invalid", f"{src.source_id}: bad missing_required_columns policy", f)
        if src.write_strategy == "merge" and not src.merge_columns:
            err("merge_columns_required", f"{src.source_id}: merge strategy needs merge_columns", f)
        if not src.primary_key_columns:
            err("primary_key_required", f"{src.source_id}: primary_key_columns required", f)
        if not (src.source_record_id_column or src.source_record_id_expression):
            err("source_record_id_required",
                f"{src.source_id}: derived_keys need a source key column or expression", f)
        if not src.event_time_column:
            err("event_time_required", f"{src.source_id}: derived_keys.event_time_column required", f)
        if src.is_derived:
            if not src.derived_from or src.derived_from not in cfg.sources:
                err("derived_from_unknown", f"{src.source_id}: derived_from must reference a source", f)
            if not src.derived_predicate:
                err("derived_predicate_required", f"{src.source_id}: derived sources need a predicate", f)
        else:
            if not src.source_object.startswith("system."):
                err("source_object_invalid", f"{src.source_id}: system-table sources must be system.*", f)

        # keys/watermarks against the declared contract
        contract = cfg.contracts.get(src.contract_ref) or {}
        cols = contract.get("columns") or {}
        if cols:
            if src.mode == "incremental":
                if not src.watermark_column:
                    err("watermark_required", f"{src.source_id}: incremental mode needs watermark_column", f)
                elif src.watermark_column not in cols:
                    err("watermark_not_in_contract",
                        f"{src.source_id}: watermark {src.watermark_column} not in contract columns", f)
                elif src.watermark_type and not _norm_type(cols[src.watermark_column]["type"]).startswith(
                    _norm_type(src.watermark_type)
                ):
                    err("watermark_type_mismatch",
                        f"{src.source_id}: watermark_type {src.watermark_type} vs contract "
                        f"{cols[src.watermark_column]['type']}", f)
            for role, col in (("primary_key", c) for c in src.primary_key_columns):
                if col not in cols:
                    err(f"{role}_not_in_contract", f"{src.source_id}: key column {col} not in contract", f)
            for col in src.merge_columns:
                if col not in cols:
                    err("merge_col_not_in_contract", f"{src.source_id}: merge column {col} not in contract", f)
            if src.event_time_column and src.event_time_column not in cols:
                err("event_time_not_in_contract",
                    f"{src.source_id}: event time {src.event_time_column} not in contract", f)
            if src.source_record_id_column and src.source_record_id_column not in cols:
                err("record_id_col_not_in_contract",
                    f"{src.source_id}: record id column {src.source_record_id_column} not in contract", f)

        # target collision
        full_target = f"{src.target_schema}.{src.target_table}"
        if full_target in targets:
            err("target_collision", f"{src.source_id} and {targets[full_target]} share target {full_target}", f)
        targets[full_target] = src.source_id

        for rule_id in src.quality_rules:
            if rule_id not in cfg.quality_rules:
                err("quality_rule_unknown", f"{src.source_id}: unknown quality rule {rule_id}", f)

    # --- quality rules -----------------------------------------------------
    for rule_id, rule in cfg.quality_rules.items():
        if not rule.get("expression"):
            err("quality_expression_missing", f"quality rule {rule_id} needs an expression")
        if rule.get("action") not in ALLOWED_QA_ACTIONS:
            err("quality_action_invalid", f"quality rule {rule_id}: bad action")

    # --- secret leakage -----------------------------------------------------
    for rel, text in cfg.raw_texts.items():
        for name, pattern in SECRET_ERROR_PATTERNS.items():
            m = pattern.search(text)
            if m:
                err("secret_leak", f"possible {name} in {rel} (never store credentials in YAML)")
        for name, pattern in SECRET_WARNING_PATTERNS.items():
            if pattern.search(text):
                warn("credential_reference", f"credential-like key in {rel}: verify it is a reference, not a value")

    return issues
