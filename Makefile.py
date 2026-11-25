# NOTE: workspace tooling created this file as "Makefile.py" (a Makefile has no
# extension). This is a documentation copy of the intended Make targets; the same
# commands are documented in README.md. When cloned locally as "Makefile", it works
# with GNU make unchanged.

# --- make targets (reference) ---------------------------------------------
# validate : validate YAML configuration for the dev environment
#   PYTHONPATH=src python scripts/validate_config.py --conf-root conf --environment dev
#
# discover : inspect source system tables and print the capability matrix
#   PYTHONPATH=src python scripts/discover_sources.py --conf-root conf --environment dev
#
# bootstrap: create catalogs/schemas/control tables (dev only; needs approval for prod)
#   PYTHONPATH=src python scripts/bootstrap_catalogs.py --conf-root conf --environment dev
#
# ingest   : run the regional ingestion + normalization + publish pipeline
#   PYTHONPATH=src python src/notebooks/02_run_regional_ingestion.py --conf-root conf --environment dev
#
# test     : run unit and integration tests
#   PYTHONPATH=src:tests python -m pytest tests -q
#
# bundle-validate : validate the Declarative Automation Bundle without deploying
#   databricks bundle validate
#
# bundle-deploy : deploy the dev bundle (requires explicit approval)
#   databricks bundle deploy --target dev
