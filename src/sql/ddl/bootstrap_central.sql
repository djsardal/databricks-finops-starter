-- Bootstrap the CENTRAL FinOps catalog (hub). Idempotent.
-- Central Silver fact tables are created by the transfer publisher
-- (CREATE TABLE LIKE <regional silver>); Gold tables/views by gold_model.sql.

CREATE CATALOG IF NOT EXISTS {catalog};

CREATE SCHEMA IF NOT EXISTS {catalog}.config;
CREATE SCHEMA IF NOT EXISTS {catalog}.ops;
CREATE SCHEMA IF NOT EXISTS {catalog}.bronze;
CREATE SCHEMA IF NOT EXISTS {catalog}.silver;
CREATE SCHEMA IF NOT EXISTS {catalog}.quarantine;
CREATE SCHEMA IF NOT EXISTS {catalog}.gold;
