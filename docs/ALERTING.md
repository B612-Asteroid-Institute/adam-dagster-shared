# Error reporting and Slack alerting

```
error → structured stdout entry → Cloud Logging → Google Error Reporting (grouping,
counts, history) → group renderer sensor → Slack card (adam-alerts bot)
```

Google Error Reporting (ER) is the system of record; Slack shows one card per NEW error
group, known groups accumulate silently, and a daily digest (16:00 UTC) summarizes Dagster
failures plus a cross-surface ER overview.

## How errors get in

| Surface | Wiring | Grouping key |
|---|---|---|
| Every Dagster code location | `alerting/er_log_handler.py`, attached instance-wide via the Helm chart's `pythonLogs.dagsterHandlerConfig`; module loggers listed in `pythonLogs.managedPythonLoggers` flow through it too | (exception class, step) |
| Django services (adam-api) | `er_logging.ErrorReportingHandler` on the root logger via settings LOGGING | (exception class, `METHOD /route/{id}`) |
| Workers (adam-jobs) | `er_logging.ErrorReportingHandler` in `logging.basicConfig(handlers=[...])` | (exception class, logger name) |

Constructing `er_logging.ErrorReportingHandler` also chains `sys.excepthook`, so an uncaught
exception in a service or worker reports as CRITICAL.

Deliberately not reported as cards (the daily digest counts them by class): spot
preemption / node-drain interrupts (`DagsterExecutionInterruptedError`), step pods that
vanished (`failed health check`: evicted, preempted or OOM-killed — Dagster cannot tell
which), run cancels, and steps abandoned because an upstream step failed.

Uncaught exceptions in worker threads report too (`threading.excepthook` is chained).
Entries are bounded to 60k chars (head + tail) so Cloud Logging never drops them.

Not covered: non-Python containers, schedules that are turned off, and the code
server / daemon being down (see the dead-man's switch in the rollout notes).

## The Slack surface

Exactly one code location (adam-etl) merges `alerting_definitions()` into its Definitions:
the ER group renderer, the daily digest job+schedule, the hung-run watchdog and the tick
watch. A second registrant would double-post.

The tick watch (`alerting_tick_failure_watch`, 10 min) covers what happens in the daemon
rather than in a run pod: a sensor or schedule whose evaluation raises (one card per
instigator and exception class, re-alerted daily) and runs that hit the run-start timeout
(≥3 in an hour → one card per day). The digest classifies the latter as `start-timeout`.

Renderer guarantees:

- **Initialization**: the first COMPLETE snapshot (paginated to the end, empty included)
  adopts every existing group silently; a failed or page-capped fetch defers. Enabling the
  renderer never floods the channel, and an empty project never swallows its first error.
- **Delivery**: a group is `announced` only once Slack accepts the post; anything else
  leaves it `pending` for later ticks. At most 10 posts per tick.
- **Scoping**: only groups with a service in this namespace announce, and the sample event
  is fetched with `serviceFilter.service`, so a group shared across environments never
  shows another environment's traceback.
- Cards: quoted cause line, Service/Step/Request/User/Occurrences/Build fields, ≤1800 chars
  of stack (head+tail), URL buttons only (mrkdwn links unfurl). `-- severity: critical` → 🔥.

## Environment flags

| Env | Where | Meaning |
|---|---|---|
| `ALERTING_ENABLED` | host location | sensors/schedule default RUNNING when truthy |
| `ALERTING_SLACK_TOKEN_SECRET` | host location | Secret Manager resource of the bot token; unset ⇒ dry-run (payloads logged) |
| `ALERTING_SLACK_CHANNEL` / `ALERTING_DRY_RUN` | host location | channel override / force dry-run |
| `ALERTING_WATCHDOG_POSTING` | host location | hung-run watchdog posting (default true) |
| `ER_LOGGING_ENABLED` | every bridged service | the non-Dagster handler is a no-op without it |
| `ER_SERVICE_NAME` / `ER_SERVICE_VERSION` | every bridged service | ER `serviceContext` (`<namespace>/<name>`, build tag) |
| `DAGSTER_LOCATION_NAME` / `DAGSTER_IMAGE_VERSION` | code locations | same, for the Dagster handler |

## Grouping contract

ER groups stackless entries by the first 3 message tokens + `reportLocation.functionName`,
and switches to frame-based grouping when it sees `Traceback (most recent call last):`
(which merges unrelated errors through shared framework frames). So every traceback header
is rewritten to `Stack trace (most recent call last):`, the headline owns the key
(`Cls in step` / `Cls at METHOD /route/{id}` / `Cls in logger.name`), and varying context
lives in a trailing `-- ` footer. The renderer parses these formats; they are tested.

## Operations

- Mute/resolve a noisy group in the ER console.
- Kill switch: revoke the bot token or unset the token secret env. The digest run FAILS on
  an undelivered post rather than succeeding silently.
