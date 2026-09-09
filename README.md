# adam-dagster-shared

## Alerting and error reporting

The centralized error-reporting pipeline (Dagster instance log handler, service/worker
bridges, ER group renderer, daily digest, hung-run watchdog) lives in
`adam_dagster_shared.alerting` and `adam_dagster_shared.er_logging`.
See [docs/ALERTING.md](docs/ALERTING.md) for wiring, env flags, and delivery semantics.
