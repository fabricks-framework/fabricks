# Debugging

New entries follow this template: a `##` heading naming the failure, then
**Symptom**, **Cause**, **Workaround**.

## DBR 18 `foreachBatch` Isolated Worker Startup

**Symptom:** A streaming query fails from `.start()` before its batch callback
logs, with `ISOLATION_STARTUP_FAILURE.GENERIC` and
`SandboxClientTimeoutTracker(Some(60000 milliseconds))`.

**Cause:** On `USER_ISOLATION` compute, Databricks SafeSpark times out while
starting the isolated Python worker. This platform timeout is unrelated to the
framework query timeout.

**Workaround:** `write_stream()` retries this startup failure with backoff. A
streaming callback must serialize only job identity and configuration, then
reconstruct the job inside the worker instead of capturing the driver-side
`Processor`.
