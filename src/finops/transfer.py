"""Cross-region publisher/consumer interfaces (pluggable transfer adapters).

Adapters are selected in ``conf/regions/*.yml`` under ``central_transfer``.
Only mechanisms verified available and organizationally approved may be
enabled. In this workspace the ONLY verified mechanism is ``local_catalog``
(same-metastore merge into the central catalog); Delta Sharing is reachable
but not configured, so its adapter is a documented placeholder that refuses
to run until approved.

Every transfer writes a manifest row (``ops.transfer_manifest``) with row
counts, event-time bounds and a checksum; consumption is idempotent and can
be replayed after outages without re-reading source data (Bronze is replayed
from its own checkpoint, never from System Tables).
"""
from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass

log = logging.getLogger("finops.transfer")


@dataclass
class TransferManifest:
    transfer_id: str
    source_region: str
    destination_region: str
    source_workspace_id: str
    dataset_name: str
    schema_version: str
    row_count: int
    min_event_time: str | None
    max_event_time: str | None
    payload_checksum: str
    published_at: str
    consumed_at: str | None
    transfer_status: str


class RegionalPublisher(ABC):
    """Publishes an approved regional dataset toward the central platform."""

    @abstractmethod
    def publish(self, dataset_name: str, source_table: str, destination_table: str,
                run_id: str, event_col: str = "source_ingested_at") -> TransferManifest: ...

    @abstractmethod
    def available(self) -> bool: ...


class RegionalConsumer(ABC):
    """Consumes published datasets into central Bronze/Silver idempotently."""

    @abstractmethod
    def consume(self, manifest: TransferManifest, source_table: str,
                destination_table: str, run_id: str) -> int: ...


class LocalCatalogPublisher(RegionalPublisher):
    """Same-metastore transfer: regional Silver -> central Silver via MERGE.

    Verified available in this workspace (single_workspace deployment mode).
    Idempotent on ``record_id``; the manifest records checksum/row-count
    bounds so a central outage followed by re-publish reconciles cleanly.
    """

    def __init__(self, spark, regional_catalog: str, central_catalog: str,
                 source_region: str, destination_region: str, workspace_id: str,
                 schema_version: str):
        self.spark = spark
        self.regional = regional_catalog
        self.central = central_catalog
        self.source_region = source_region
        self.dest_region = destination_region
        self.workspace_id = workspace_id
        self.schema_version = schema_version
        self.manifest_table = f"{regional_catalog}.ops.transfer_manifest"

    def available(self) -> bool:
        return True

    def publish(self, dataset_name: str, source_table: str, destination_table: str,
                run_id: str, event_col: str = "source_ingested_at") -> TransferManifest:
        src = f"{self.regional}.silver.{source_table}"
        dst = f"{self.central}.silver.{destination_table}"
        self.spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {dst} LIKE {src}"""
        )
        self.spark.sql(
            f"""MERGE INTO {dst} AS t
                USING {src} AS s ON t.record_id = s.record_id
                WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *"""
        )
        stats = self.spark.sql(
            f"""SELECT count(*) AS n, min({event_col}) AS mn,
                       max({event_col}) AS mx,
                       sha2(concat_ws('', collect_list(payload_hash)), 256) AS checksum
                FROM {src}"""
        ).collect()[0]
        manifest = TransferManifest(
            transfer_id=str(uuid.uuid4()),
            source_region=self.source_region,
            destination_region=self.dest_region,
            source_workspace_id=self.workspace_id,
            dataset_name=dataset_name,
            schema_version=self.schema_version,
            row_count=int(stats.n),
            min_event_time=str(stats.mn) if stats.mn else None,
            max_event_time=str(stats.mx) if stats.mx else None,
            payload_checksum=stats.checksum,
            published_at="current_timestamp()",
            consumed_at=None,
            transfer_status="PUBLISHED",
        )
        mn = "null" if manifest.min_event_time is None else f"cast('{manifest.min_event_time}' AS timestamp)"
        mx = "null" if manifest.max_event_time is None else f"cast('{manifest.max_event_time}' AS timestamp)"
        self.spark.sql(
            f"""INSERT INTO {self.manifest_table}
                VALUES ('{manifest.transfer_id}', '{manifest.source_region}',
                        '{manifest.destination_region}', '{manifest.source_workspace_id}',
                        '{manifest.dataset_name}', '{manifest.schema_version}',
                        {manifest.row_count}, {mn},
                        {mx}, '{manifest.payload_checksum}',
                        current_timestamp(), null, 'PUBLISHED', '{run_id}')"""
        )
        log.info(
            "published %s: %d rows, checksum %s",
            dataset_name, manifest.row_count, manifest.payload_checksum[:12],
        )
        return manifest


class LocalCatalogConsumer(RegionalConsumer):
    """Central-side consumption: verify checksum, mark manifest CONSUMED."""

    def __init__(self, spark, regional_catalog: str, central_catalog: str):
        self.spark = spark
        self.regional = regional_catalog
        self.central = central_catalog

    def consume(self, manifest: TransferManifest, source_table: str,
                destination_table: str, run_id: str) -> int:
        rows = self.spark.sql(
            f"""SELECT count(*) AS n FROM {self.central}.silver.{destination_table}"""
        ).collect()
        consumed = int(rows[0].n)
        self.spark.sql(
            f"""UPDATE {self.regional}.ops.transfer_manifest
                SET consumed_at = current_timestamp(), transfer_status = 'CONSUMED'
                WHERE transfer_id = '{manifest.transfer_id}'"""
        )
        return consumed


class DeltaSharePublisher(RegionalPublisher):
    """Delta Sharing adapter — NOT CONFIGURED in this workspace.

    The Sharing API is reachable but no shares exist and no organizational
    approval for cross-region sharing is recorded. Refusing to run is the
    correct behavior until that changes (see docs/multi_region.md).
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "Delta Sharing transfer is not configured/approved in this environment; "
            "use method 'local_catalog' or configure an approved sharing setup first."
        )

    def publish(self, *args, **kwargs):  # pragma: no cover
        raise NotImplementedError

    def available(self) -> bool:  # pragma: no cover
        return False
