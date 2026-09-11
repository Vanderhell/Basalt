# Basalt.NET architecture

```text
Application
    ↓
Managed Basalt API
    ↓
Basalt Core
    ↓
Storage SPI
   ↙       ↘
Embedded   SQL Server
```

- The managed API provides typed registration, enqueue, scheduling, workflows, management, and lifecycle.
- Basalt Core owns worker coordination and durable state transitions without depending on a particular store.
- The storage SPI supplies the atomic records, compare-and-swap transitions, clock, transactions, and allocation required by Core.
- Embedded uses BasaltDB files for one local durable store. SQL Server uses a dedicated schema in an existing database and supports shared workers.

Workers claim eligible executions under leases and fencing tokens. The scheduler creates executions from durable schedule definitions. Workflows are static DAGs whose nodes become eligible as dependencies finish. On restart, persisted state and expired leases allow unfinished work to be recovered and executed again under the at-least-once contract.

The managed API does not expose native handles or provider storage handles to normal applications.
