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

Uncaught exceptions in worker threads report too (`threading.excepthook` is chained); a
log-then-raise in either hook keeps the logger's group. Entries are bounded to 60k chars
(head + tail) so Cloud Logging never drops them. Every copy of a request path (headline,
footer, `httpRequest.url`, message text) is normalized: ids become `{id}` and sharing
capability tokens (`/share/<slug>/<token>/`, or any token-shaped segment) become `{token}`,
so a credential never reaches Error Reporting or a card. Workers install the bridge before
importing their dependencies (`python -m adam_jobs.cutout_worker` re-exports lazily), so a
broken image reports its ImportError instead of crash-looping on stderr only; batch Jobs
launched with `python -m <package>.entrypoint` still import their package first, and a
failure there is reported by the orchestrating op instead.

Not covered: non-Python containers, schedules that are turned off, and the code
server / daemon being down (see the dead-man's switch in the rollout notes).

## The Slack surface

Exactly one code location (adam-etl) merges `alerting_definitions()` into its Definitions:
the ER group renderer, the daily digest job+schedule, the hung-run watchdog and the tick
watch. A second registrant would double-post.

The tick watch (`alerting_tick_failure_watch`, 10 min) covers what happens in the daemon
rather than in a run pod: a sensor or schedule whose evaluation raises (one card per
instigator and exception class, re-alerted daily), runs that hit the run-start timeout
(≥3 in an hour → one card per day; `start-timeout` in the digest), and every other run the
daemon marks failed without a log handler ever seeing it (run worker lost after its resume
attempts, max runtime exceeded, launch failure): one card per scan listing the runs. It
finds them with one `fetch_run_status_changes` query over the last hour (RUN_FAILURE events
that carry neither an exception nor the run worker's "Steps failed" summary). Suppression
is recorded only once Slack accepted the post — the watchdog does the same — and a
rejected post keeps the scan window open, so an outage delays a card instead of eating it.

Renderer guarantees:

- **Initialization**: the first COMPLETE snapshot (paginated to the end, empty included)
  adopts every existing group silently; a failed or page-capped fetch defers. Enabling the
  renderer never floods the channel, and an empty project never swallows its first error.
- **Delivery**: a group is `announced` only once Slack accepts the post; anything else
  leaves it `pending` for later ticks. At most 10 posts per tick.
- **Memory**: `announced` is pruned by scope (a group gone from the weekly window is
  forgotten; its recurrence is news), never by count while a group is active; a 2000-id
  safety cap applies only to page-capped scans and forgets absent ids first.
- **Outages are visible**: three consecutive failed ER fetches, or three ticks on which
  Slack rejected every post, make the tick itself fail. The tick watch posts the failing
  sensor (when Slack is up) and the daemon stops logging the heartbeat the dead-man's
  switch watches (when it is not). A single hiccup stays a warning.
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
