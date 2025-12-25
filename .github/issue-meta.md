## Summary

This meta-issue tracks the overall implementation progress of the Distributed Task Scheduler project. Each phase is tracked as a separate issue, and this issue will be updated as phases are completed.

## Project Overview

A fault-tolerant job orchestration system demonstrating distributed systems fundamentals, actor-based concurrency, and production-grade failure handling.

## Implementation Phases

| Phase | Description | Status | Issue |
|-------|-------------|--------|-------|
| 1 | Core Domain Model + Persistence | Not Started | #2 |
| 2 | DAG Parser + Single-Worker Executor | Not Started | #3 |
| 3 | REST API + Worker Pool | Not Started | #4 |
| 4 | Distributed Coordination with etcd | Not Started | #5 |
| 5 | Production Hardening + Dead Letter Queue | Not Started | #6 |

## Progress Tracking

### Phase 1: Core Domain Model + Persistence
- [ ] Project scaffolding with `build.sbt`
- [ ] Domain case classes with Circe codecs
- [ ] Flyway migrations for all tables
- [ ] Doobie Transactor configuration
- [ ] Repository implementations
- [ ] Integration tests with Testcontainers

### Phase 2: DAG Parser + Single-Worker Executor
- [ ] YAML parser with custom decoders
- [ ] Cycle detection algorithm
- [ ] Topological sort implementation
- [ ] ShellTaskExecutor
- [ ] HttpTaskExecutor
- [ ] SimpleScheduler for sequential execution

### Phase 3: REST API + Worker Pool
- [ ] Tapir endpoint definitions
- [ ] API route handlers
- [ ] OpenAPI documentation
- [ ] Worker implementation
- [ ] WorkerPool with configurable size
- [ ] TaskRouter with load balancing

### Phase 4: Distributed Coordination with etcd
- [ ] etcd client configuration
- [ ] Leader election
- [ ] Distributed locks
- [ ] Worker registration with heartbeats
- [ ] DistributedScheduler
- [ ] Work stealing implementation

### Phase 5: Production Hardening + Dead Letter Queue
- [ ] DLQ schema and repository
- [ ] DeadLetterQueue service
- [ ] Prometheus metrics
- [ ] Health check endpoints
- [ ] Graceful shutdown
- [ ] Signal handling

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Gateway                                     │
│                         (http4s + Tapir)                                    │
└─────────┬───────────────────┬────────────────────────┬──────────────────────┘
          │                   │                        │
          ▼                   ▼                        ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐
│   DAG Parser    │  │    Scheduler    │  │   Coordination Layer (etcd)     │
└────────┬────────┘  └────────┬────────┘  └─────────────────────────────────┘
         │                    │
         │           ┌────────┴────────┐
         │           │  Task Router    │
         │           └────────┬────────┘
         │    ┌───────────────┼───────────────┬───────────────┐
         │    ▼               ▼               ▼               ▼
         │ ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
         │ │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker N │
         │ └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
         └──────┴────────────┴────────────┴────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────────────┐  ┌────────────────────────────┐
│      State Store            │  │    Dead Letter Queue       │
│      (PostgreSQL)           │  │    (PostgreSQL)            │
└─────────────────────────────┘  └────────────────────────────┘
```

## Technology Stack

- **Language**: Scala 3.3
- **Effect System**: ZIO 2.x
- **HTTP**: http4s + Tapir
- **Database**: PostgreSQL + Doobie
- **Coordination**: etcd
- **Build**: sbt

## Related Documentation

- [Implementation Plan](docs/IMPLEMENTATION_PLAN.md)
- [README](README.md)
