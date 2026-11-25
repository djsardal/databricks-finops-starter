"""Data-quality rule engine and quarantine framework.

Rules are declared in ``conf/quality/quality_rules.yml`` (SQL boolean
expressions against Silver tables). A row VIOLATES a rule when NOT(expression).
Invalid rows are routed idempotently to ``quarantine.<table>`` keyed on
(record_id, rule_id) with a review payload; ``record_status`` is flipped to
'quarantined' in Silver; outcomes land in ``ops.data_quality_result``.

Quarantine is never silent: rates above the configured tolerance raise
``DataQualityError`` so the pipeline records a failure for that source.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger("finops.quality")


class DataQualityError(RuntimeError):
    """Raised when the quarantine rate exceeds the configured tolerance."""


@dataclass
class RuleResult:
    rule_id: str
    target_table: str
    evaluated: int
    failed: int
    action: str
    status: str  # PASSED | WARNED | QUARANTINED | FAILED_SOURCE


class QualityEngine:
    """Evaluates configured rules against one Silver table per run."""

    def __init__(self, spark, catalog: str, rules: dict, max_quarantine_pct: float = 1.0):
        self.spark = spark
        self.catalog = catalog
        self.rules = rules
        self.max_quarantine_pct = max_quarantine_pct

    def ensure_tables(self) -> None:
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {self.catalog}.ops.data_quality_result (
                  run_id STRING, source_id STRING, rule_id STRING, target_table STRING,
                  evaluated_count BIGINT, failed_count BIGINT, action STRING, status STRING,
                  evaluated_at TIMESTAMP
                )"""
        )

    def _ensure_quarantine_table(self, silver_table: str) -> str:
        q = f"{self.catalog}.quarantine.{silver_table}"
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {q} (
                  record_id STRING, rule_id STRING, rule_expression STRING,
                  payload_json STRING, source_id STRING, run_id STRING,
                  quarantined_at TIMESTAMP
                )"""
        )
        return q

    def evaluate(self, source_id: str, silver_table: str, run_id: str) -> list[RuleResult]:
        """Run every rule targeting *silver_table*; returns per-rule results."""
        table = f"{self.catalog}.silver.{silver_table}"
        results: list[RuleResult] = []
        total_failed = 0
        for rule_id, rule in self.rules.items():
            if rule.get("target_table") != silver_table:
                continue
            expression = rule["expression"]
            action = rule.get("action", "warn")

            counts = self.spark.sql(
                f"""SELECT count(*) AS evaluated,
                          sum(CASE WHEN NOT ({expression}) THEN 1 ELSE 0 END) AS failed
                    FROM {table}"""
            ).collect()
            evaluated = int(counts[0].evaluated) if counts else 0
            failed = int(counts[0].failed or 0) if counts else 0

            status = "PASSED" if failed == 0 else (
                "QUARANTINED" if action == "quarantine" else
                "FAILED_SOURCE" if action == "fail_source" else "WARNED"
            )
            if failed > 0 and action == "quarantine":
                q = self._ensure_quarantine_table(silver_table)
                self.spark.sql(
                    f"""MERGE INTO {q} AS t
                        USING (
                          SELECT record_id,
                                 '{rule_id}' AS rule_id,
                                 '{expression.replace(chr(39), chr(39)*2)}' AS rule_expression,
                                 to_json(struct(*)) AS payload_json,
                                 '{source_id}' AS source_id,
                                 '{run_id}' AS run_id,
                                 current_timestamp() AS quarantined_at
                          FROM {table} WHERE NOT ({expression})
                        ) AS s
                        ON t.record_id = s.record_id AND t.rule_id = s.rule_id
                        WHEN NOT MATCHED THEN INSERT *
                        WHEN MATCHED AND t.run_id <> s.run_id THEN UPDATE SET *"""
                )
                self.spark.sql(
                    f"""UPDATE {table} SET record_status = 'quarantined'
                        WHERE record_id IN (SELECT record_id FROM {q} WHERE rule_id = '{rule_id}')"""
                )
            if failed > 0 and action == "fail_source":
                status = "FAILED_SOURCE"

            self.spark.sql(
                f"""INSERT INTO {self.catalog}.ops.data_quality_result
                    VALUES ('{run_id}', '{source_id}', '{rule_id}', '{silver_table}',
                            {evaluated}, {failed}, '{action}', '{status}', current_timestamp())"""
            )
            total_failed += failed if action == "quarantine" else 0
            results.append(RuleResult(rule_id, silver_table, evaluated, failed, action, status))

        # tolerance enforcement on quarantine-rate
        if results:
            evaluated = sum(r.evaluated for r in results)
            rate = (total_failed / evaluated * 100.0) if evaluated else 0.0
            if rate > self.max_quarantine_pct:
                raise DataQualityError(
                    f"quarantine rate {rate:.3f}% exceeds tolerance {self.max_quarantine_pct}% "
                    f"for {silver_table}"
                )
        return results
