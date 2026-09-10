# Durability contract

## At-least-once execution

A successful enqueue means the job was durably submitted. It does not mean the handler completed. A worker may execute a handler again after a crash, lost acknowledgement, or expired lease.

Handlers that affect external systems must therefore be idempotent or use a fencing mechanism accepted by that external system.

## Idempotency

```csharp
await basalt.EnqueueAsync(job, key: "invoice:123");
```

The stable business key resolves repeated submissions to the same durable receipt. If `BasaltUnknownCommitException` is thrown, retry the identical operation with the same idempotency key; do not generate a new key.

## Retries

Retry policy is persisted with the execution. `MaxAttempts` includes the initial attempt. Fixed, linear, exponential, and exponential-with-jitter strategies are available. Retry is distinct from recurring scheduling.

## Leases and fencing

A worker owns claimed work for a bounded lease and renews it while handling the job. If ownership expires, another worker can recover the execution. A fencing token changes when ownership changes; use `JobContext.FencingToken` when an external resource supports fencing stale workers.

## Crash recovery

Durable state transitions are atomic in the selected provider. After process or machine failure, Basalt recovers persisted state, reclaims expired work, and continues eligible executions. This recovery can repeat handler invocation, which is why the public contract remains at least once.

Embedded recovery uses the BasaltDB WAL and durable files. SQL Server recovery uses transactional rows, leases, compare-and-swap revisions, and fencing in the configured Basalt schema.
