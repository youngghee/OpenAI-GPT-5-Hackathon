# Environment Configuration

Add one YAML file per runtime environment (`dev`, `staging`, `prod`). Each file
should declare `model_id`, `codex_id`, per-agent token budgets, relevant rate
limits, asset paths (for example `paths.scrapes_dir`, `paths.schema_escalations_dir`,
or `paths.migrations_dir`), and any environment variable indirections needed by
integrations (`data_sources.csv.path_env`, `search.api_key_env`, etc.).
