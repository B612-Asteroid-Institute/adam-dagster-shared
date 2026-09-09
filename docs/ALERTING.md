# Error reporting and Slack alerting

One pipeline, centralized here so services plug in automatically:

```
error → structured stdout entry → Cloud Logging → Google Error Reporting (grouping,
counts, dedup, history) → group renderer sensor → rich Slack card (adam-alerts bot)
```

Google Error Reporting is the system of record for grouping and novelty; Slack shows one
rich card per NEW error class; known classes accumulate counts silently. A daily digest
(16:00 UTC) summarizes Dagster failures plus a cross-surface ER overview.

## How errors get in

| Surface | Wiring | Coverage |
|---|---|---|
| Every Dagster code location | `alerting/er_log_handler.py` attached instance-wide via the Helm chart's `pythonLogs.dagsterHandlerConfig` — no per-location, per-job, or per-asset registration | step/run failures with full cause chains; grouping = (exception class, step) |
| Django/ninja services (adam-api) | root logging handler via `er_logging.ErrorReportingHandler` (settings LOGGING) | unhandled endpoint exceptions, `django.request`/`django.security` middleware errors; grouping = (exception class, METHOD /route/{id}) |
| Workers / entrypoints (adam-jobs) | `er_logging.maybe_install()` at process start (adam-jobs runs it in its package `__init__`, before fragile imports) | `logger.exception` paths AND uncaught exceptions via chained `sys.excepthook` (reported CRITICAL); grouping = (exception class, logger name) |

Not covered: SIGKILL/OOM terminations (no Python runs), non-Python containers, silently
missing schedules (keep independent monitoring for those).

## The Slack surface (registered by exactly ONE host location)

```python
from adam_dagster_shared.alerting import alerting_definitions
```
merges the ER group renderer, daily digest job+schedule, hung-run watchdog, and the
audit-only failure classifier into that location's Definitions. adam-etl is the host.

### Renderer semantics (the important guarantees)

- **Initialization**: the first COMPLETE discovery snapshot (paginated to the end, empty
  results included) adopts every existing group silently and sets `initialized` in the
  cursor. A failed or page-capped fetch defers initialization. Enabling the renderer never
  floods the channel, and an empty project does not swallow its first real error.
- **Delivery**: a group is `announced` only after Slack accepts the post; failures of any
  kind leave it `pending` and it retries on later ticks. At most 10 deliveries per tick;
  overflow stays pending.
- **Scoping**: only groups with a service in this namespace announce; the sample event is
  fetched with `serviceFilter.service` so a group shared across environments never shows
  another environment's traceback.
- Cards carry: quoted cause line, Service/Step/Request/User/Occurrences/Build fields, up to
  1800 chars of stack (head+tail), and URL buttons (mrkdwn links unfurl; buttons never do).
  CRITICAL severity (from the `-- severity: critical` footer) renders 🔥.

## Environment flags

| Env | Where | Meaning |
|---|---|---|
| `ALERTING_ENABLED` | host location | sensors/schedule default RUNNING when truthy (production only in the values) |
| `ALERTING_SLACK_TOKEN_SECRET` | host location | Secret Manager resource of the bot token; unset ⇒ dry-run (payloads logged, nothing posted) |
| `ALERTING_SLACK_CHANNEL` / `ALERTING_DRY_RUN` | host location | channel override / force dry-run |
| `ALERTING_SENSOR_POSTING` | host location | legacy shared posting gate; the failure classifier no longer posts at all |
| `ALERTING_WATCHDOG_POSTING` | host location | hung-run watchdog posting (defaults to the gate above); verdicts always log |
| `ER_LOGGING_ENABLED` | every bridged service | the non-Dagster bridge is a no-op without it |
| `ER_SERVICE_NAME` / `ER_SERVICE_VERSION` | every bridged service | ER `serviceContext` (`<namespace>/<name>`, build tag); resolved at emit time |
| `DAGSTER_LOCATION_NAME` / `DAGSTER_IMAGE_VERSION` | code locations | same, for the Dagster handler |

## Grouping contract (why messages look the way they do)

ER groups stackless entries by the first 3 message tokens + `reportLocation.functionName`,
and switches to frame-based grouping when it sees `Traceback (most recent call last):`.
Frame grouping merges unrelated errors through shared framework frames (measured), so every
emitted traceback has ALL headers rewritten to `Stack trace (most recent call last):` and
the handlers own the key via the headline (`Cls in step` / `Cls at METHOD /route/{id}` /
`Cls in logger.name`). Request/run identifiers live in a trailing `-- ` footer, invisible
to both grouping modes. Treat these formats as a tested protocol: the renderer parses them.

## Operations

- Mute/resolve a noisy group in the ER console; resolved-group recurrence is only covered
  by Google's native notification channel if that stays enabled.
- Kill switch for posting: revoke the bot token (or unset the token secret env). The
  digest run FAILS on undelivered posts (visible, retryable) rather than succeeding
  silently.
- The `alerting_failure_reporter` sensor logs one `ALERTING_VERDICT` JSON line per failed
  run (class, `user_facing`, signature) as an audit trail while ER and the legacy metric
  alert run side by side. It never posts, and the digest re-derives its own counts from the
  runs table, so the sensor can be deleted once the dual-run period ends.

Rollout/validation history: `cloud_errors_2/WORKLOG.md` in the operator's workspace; the
review that shaped the delivery semantics: 2026-09-08 independent review (R1–R12).
