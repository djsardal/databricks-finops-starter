"""Databricks FinOps Starter — YAML-driven ingestion framework.

System Tables are source interfaces only: collectors land source records in
regional Bronze tables, normalize them into Silver, and publish approved
datasets to a central layer. Gold reporting models consume
intermediate/conformed data exclusively — never live System Tables.
"""

__version__ = "1.0.0"
