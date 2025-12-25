# Distributed Task Scheduler

A fault-tolerant job orchestration system demonstrating distributed systems fundamentals, actor-based concurrency, and production-grade failure handling. Inspired by Apache Airflow's DAG-based execution model.

## Overview

This project implements a simplified but production-quality task scheduler that supports:

- **DAG-based Task Orchestration**: Define workflows as directed acyclic graphs with dependencies
- **Distributed Execution**: Multiple workers with work-stealing load balancing
- **Fault Tolerance**: Leader election, distributed locks, and automatic failover
- **Dead Letter Queue**: Capture and manage failed tasks for manual intervention
- **Multiple Task Types**: Shell commands, HTTP requests, and Python scripts

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │            API Gateway              │
                         │          (http4s + Tapir)           │
                         └─────────────────┬───────────────────┘
                                           │
              ┌────────────────────────────┼────────────────────────────┐
              │                            │                            │
              ▼                            ▼                            ▼
    ┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
    │   DAG Parser    │          │    Scheduler    │          │  Coordination   │
    │                 │          │                 │          │     (etcd)      │
    │  YAML → Graph   │          │  Task Routing   │          │                 │
    │  Cycle Check    │          │  Ready Queue    │          │ Leader Election │
    └─────────────────┘          └────────┬────────┘          └─────────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              │                           │                           │
              ▼                           ▼                           ▼
    ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
    │    Worker 1     │         │    Worker 2     │         │    Worker N     │
    │   Task Exec     │         │   Task Exec     │         │   Task Exec     │
    │   Heartbeat     │         │   Heartbeat     │         │   Heartbeat     │
    └─────────────────┘         └─────────────────┘         └─────────────────┘
              │                           │                           │
              └───────────────────────────┼───────────────────────────┘
                                          ▼
                              ┌─────────────────────┐
                              │     PostgreSQL      │
                              │   State + DLQ       │
                              └─────────────────────┘
```

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| Effect System | ZIO 2.x | Resource safety, structured concurrency, testability |
| HTTP | http4s + Tapir | Type-safe APIs, automatic OpenAPI generation |
| Database | PostgreSQL + Doobie | Strong consistency, ZIO integration |
| Coordination | etcd | Leader election, distributed locks |
| JSON | Circe | Derivation-based codecs |
| Build | sbt | Standard Scala tooling |
| Testing | ZIO Test + Testcontainers | Real database/etcd testing |

## Project Structure

```
distributed-task-scheduler/
├── src/
│   ├── main/scala/scheduler/
│   │   ├── domain/          # Core domain models
│   │   ├── parser/          # DAG parsing and validation
│   │   ├── executor/        # Task execution engines
│   │   ├── scheduler/       # Scheduling logic
│   │   ├── coordination/    # etcd integration
│   │   ├── api/             # HTTP endpoints
│   │   └── persistence/     # Database repositories
│   └── test/scala/scheduler/
├── docs/
│   └── IMPLEMENTATION_PLAN.md
├── migrations/              # Flyway database migrations
└── build.sbt
```

## Implementation Phases

1. **Core Domain Model + Persistence** - Foundational data model and database layer
2. **DAG Parser + Single-Worker Executor** - YAML parsing, validation, sequential execution
3. **REST API + Worker Pool** - HTTP endpoints, parallel worker execution
4. **Distributed Coordination** - Leader election, distributed locks, worker registry
5. **Production Hardening** - DLQ, metrics, health checks, graceful shutdown

## Getting Started

### Prerequisites

- JDK 17+
- Scala 3.3+
- sbt 1.9+
- Docker (for Testcontainers)
- PostgreSQL 15+
- etcd 3.5+

### Running Locally

```bash
# Start dependencies
docker-compose up -d postgres etcd

# Run database migrations
sbt flywayMigrate

# Start the scheduler
sbt run

# Run tests
sbt test
```

### Example DAG Definition

```yaml
name: data-pipeline
tasks:
  - id: extract
    type: shell
    command: "curl -o /tmp/data.json https://api.example.com/data"
    timeout: 5m

  - id: transform
    type: python
    script: transform.py
    dependencies: [extract]
    retry:
      maxAttempts: 3
      backoff: exponential

  - id: load
    type: http
    method: POST
    url: "https://warehouse.example.com/ingest"
    dependencies: [transform]
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/dags` | Create a new DAG |
| GET | `/api/v1/dags/{id}` | Get DAG details |
| POST | `/api/v1/dags/{id}/runs` | Trigger a DAG run |
| GET | `/api/v1/runs/{id}` | Get run status |
| GET | `/api/v1/dlq` | List dead letter queue items |
| POST | `/api/v1/dlq/{id}/retry` | Retry a failed task |
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

## Key Features

### Fault Tolerance

- Automatic leader election with etcd
- Worker heartbeats with failure detection
- Task retry with exponential backoff
- Dead letter queue for permanently failed tasks

### Observability

- Prometheus metrics for all components
- Structured logging with correlation IDs
- Health check endpoints for each component
- Distributed tracing support

### Scalability

- Horizontal worker scaling
- Work-stealing load balancing
- Connection pooling for database
- Backpressure handling in task queues

## License

MIT

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
