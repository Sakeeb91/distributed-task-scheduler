# Distributed Task Scheduler - Implementation Plan

**Author**: Senior Distributed Systems Architect
**Version**: 1.0
**Last Updated**: December 2024

---

## Executive Summary

This document outlines the implementation strategy for building a fault-tolerant distributed task scheduler. The system demonstrates core distributed systems concepts including consensus, leader election, work distribution, and failure recovery while remaining practical and implementable within a 12-week timeline.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Technology Decisions](#2-technology-decisions)
3. [Phase 1: Core Domain Model + Persistence](#3-phase-1-core-domain-model--persistence)
4. [Phase 2: DAG Parser + Single-Worker Executor](#4-phase-2-dag-parser--single-worker-executor)
5. [Phase 3: REST API + Worker Pool](#5-phase-3-rest-api--worker-pool)
6. [Phase 4: Distributed Coordination with etcd](#6-phase-4-distributed-coordination-with-etcd)
7. [Phase 5: Production Hardening + Dead Letter Queue](#7-phase-5-production-hardening--dead-letter-queue)
8. [Risk Assessment](#8-risk-assessment)
9. [Testing Strategy](#9-testing-strategy)

---

## 1. Architecture Overview

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Gateway                                     │
│                         (http4s + Tapir)                                    │
│         DAG Submit │ Task Status │ Worker Management │ Metrics              │
└─────────┬───────────────────┬────────────────────────┬──────────────────────┘
          │                   │                        │
          ▼                   ▼                        ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐
│   DAG Parser    │  │    Scheduler    │  │   Coordination Layer (etcd)     │
│                 │  │                 │  │                                 │
│ YAML/JSON → DAG │  │ Topological     │  │  Leader Election                │
│ Validation      │  │ Sort + Ready    │  │  Distributed Locks              │
│ Cycle Detection │  │ Queue           │  │  Worker Registry                │
└────────┬────────┘  └────────┬────────┘  └─────────────────────────────────┘
         │                    │                        │
         │                    ▼                        │
         │           ┌─────────────────┐               │
         │           │  Task Router    │◄──────────────┘
         │           │                 │
         │           │ Work Stealing   │
         │           │ Load Balancing  │
         │           └────────┬────────┘
         │                    │
         │    ┌───────────────┼───────────────┬───────────────┐
         │    ▼               ▼               ▼               ▼
         │ ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
         │ │Worker 1 │  │Worker 2 │  │Worker 3 │  │Worker N │
         │ │         │  │         │  │         │  │         │
         │ │Task Exec│  │Task Exec│  │Task Exec│  │Task Exec│
         │ │Heartbeat│  │Heartbeat│  │Heartbeat│  │Heartbeat│
         │ └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
         │      │            │            │            │
         └──────┴────────────┴────────────┴────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────────────┐  ┌────────────────────────────┐
│      State Store            │  │    Dead Letter Queue       │
│      (PostgreSQL)           │  │    (PostgreSQL)            │
│                             │  │                            │
│  DAG Definitions            │  │  Failed Tasks              │
│  Task Instances             │  │  Error Context             │
│  Execution History          │  │  Manual Review Queue       │
│  Worker State               │  │                            │
└─────────────────────────────┘  └────────────────────────────┘
```

### Core Components

| Component | Responsibility | Key Interfaces |
|-----------|---------------|----------------|
| **API Gateway** | REST endpoints for DAG submission, status queries, worker management | `DagController`, `RunController`, `WorkerController` |
| **DAG Parser** | Parse YAML/JSON definitions, validate structure, detect cycles | `DagParser`, `DagValidator` |
| **Scheduler** | Determine task execution order, manage ready queue | `Scheduler`, `TaskRouter` |
| **Workers** | Execute tasks, report status, handle retries | `Worker`, `TaskExecutor` |
| **Coordination Layer** | Leader election, distributed locks, worker discovery | `DistributedCoordinator`, `LeaderElection` |
| **State Store** | Persist all state, enable crash recovery | `DagRepository`, `TaskInstanceRepository` |
| **Dead Letter Queue** | Capture permanently failed tasks for manual intervention | `DeadLetterQueue`, `DlqRepository` |

---

## 2. Technology Decisions

### Technology Stack

| Layer | Choice | Rationale | Trade-offs |
|-------|--------|-----------|------------|
| **Language** | Scala 3.3 | Strong type system, excellent for DSLs, ZIO ecosystem | Smaller talent pool than Java/Kotlin |
| **Effect System** | ZIO 2.x | Superior resource safety, structured concurrency, excellent testing | Steeper learning curve than Cats Effect |
| **HTTP** | http4s + Tapir | Type-safe API definitions, automatic OpenAPI generation | More boilerplate than Akka HTTP initially |
| **Database** | PostgreSQL + Doobie | Mature, excellent ZIO integration, JSONB for flexible schemas | Doobie requires understanding ConnectionIO |
| **Coordination** | etcd (jetcd) | Simpler than ZooKeeper, excellent documentation, gRPC-based | Less JVM ecosystem support than ZK |
| **JSON** | Circe | Derivation-based codecs, excellent error messages | Compile times with many case classes |
| **Build** | sbt | Standard Scala tooling, good IDE support | Slower than Mill |
| **Testing** | ZIO Test + Testcontainers | First-class ZIO support, real infrastructure testing | Testcontainers adds CI complexity |

### Why ZIO over Akka?

1. **Effect systems are the future**: More transferable concepts across the industry
2. **Better testability**: ZIO Test provides superior testing patterns
3. **Resource safety**: ZIO's Scope eliminates resource leaks
4. **Simpler concurrency**: ZIO's Fiber model is more intuitive than Actor supervision
5. **Actor concepts still present**: ZIO Hub and Queue provide similar patterns

---

## 3. Phase 1: Core Domain Model + Persistence

**Duration**: Weeks 1-2
**Estimated Hours**: 15-20

### Objectives

- Establish foundational data model
- Implement database schema with Flyway migrations
- Create repository layer with full CRUD operations
- Set up ZIO + Doobie integration

### Domain Model

```scala
// File: src/main/scala/scheduler/domain/Task.scala

package scheduler.domain

import java.util.UUID
import java.time.{Duration, Instant}
import io.circe.{Json, Encoder, Decoder}
import io.circe.generic.semiauto._

// === Identifiers ===

case class DagId(value: UUID) extends AnyVal
object DagId {
  def generate: DagId = DagId(UUID.randomUUID())

  implicit val encoder: Encoder[DagId] = Encoder[UUID].contramap(_.value)
  implicit val decoder: Decoder[DagId] = Decoder[UUID].map(DagId(_))
}

case class TaskId(value: String) extends AnyVal
object TaskId {
  implicit val encoder: Encoder[TaskId] = Encoder[String].contramap(_.value)
  implicit val decoder: Decoder[TaskId] = Decoder[String].map(TaskId(_))
}

case class RunId(value: UUID) extends AnyVal
object RunId {
  def generate: RunId = RunId(UUID.randomUUID())

  implicit val encoder: Encoder[RunId] = Encoder[UUID].contramap(_.value)
  implicit val decoder: Decoder[RunId] = Decoder[UUID].map(RunId(_))
}

case class WorkerId(value: UUID) extends AnyVal
object WorkerId {
  def generate: WorkerId = WorkerId(UUID.randomUUID())
}

// === Task Types ===

sealed trait TaskType
object TaskType {
  case class Shell(
    command: String,
    env: Map[String, String] = Map.empty
  ) extends TaskType

  case class Http(
    method: String,
    url: String,
    headers: Map[String, String] = Map.empty,
    body: Option[Json] = None
  ) extends TaskType

  case class Python(
    script: String,
    args: List[String] = Nil,
    virtualEnv: Option[String] = None
  ) extends TaskType

  implicit val encoder: Encoder[TaskType] = deriveEncoder
  implicit val decoder: Decoder[TaskType] = deriveDecoder
}

// === Retry Policy ===

case class RetryPolicy(
  maxAttempts: Int = 3,
  initialDelay: Duration = Duration.ofSeconds(1),
  maxDelay: Duration = Duration.ofMinutes(5),
  backoffMultiplier: Double = 2.0
)

object RetryPolicy {
  val default: RetryPolicy = RetryPolicy()

  implicit val encoder: Encoder[RetryPolicy] = deriveEncoder
  implicit val decoder: Decoder[RetryPolicy] = deriveDecoder
}

// === Task Definition ===

case class TaskDefinition(
  id: TaskId,
  name: String,
  taskType: TaskType,
  timeout: Duration = Duration.ofMinutes(30),
  retryPolicy: RetryPolicy = RetryPolicy.default,
  dependencies: Set[TaskId] = Set.empty
)

object TaskDefinition {
  implicit val encoder: Encoder[TaskDefinition] = deriveEncoder
  implicit val decoder: Decoder[TaskDefinition] = deriveDecoder
}

// === DAG Definition ===

case class DagDefinition(
  id: DagId,
  name: String,
  tasks: List[TaskDefinition],
  schedule: Option[String] = None,
  version: Int = 1,
  createdAt: Instant = Instant.now()
) {
  require(tasks.nonEmpty, "DAG must have at least one task")
  require(tasks.map(_.id).toSet.size == tasks.size, "Task IDs must be unique")
}

// === Task State ===

sealed trait TaskState {
  def isTerminal: Boolean = this match {
    case TaskState.Succeeded(_) | TaskState.Failed(_, _) | TaskState.Skipped => true
    case _ => false
  }
}

object TaskState {
  case object Pending extends TaskState
  case object Queued extends TaskState
  case object Running extends TaskState
  case class Succeeded(completedAt: Instant) extends TaskState
  case class Failed(error: String, attempt: Int) extends TaskState
  case object Skipped extends TaskState
}

// === Task Instance ===

case class TaskInstance(
  id: UUID,
  taskId: TaskId,
  runId: RunId,
  dagId: DagId,
  taskName: String,
  state: TaskState,
  attempt: Int = 1,
  workerId: Option[WorkerId] = None,
  startedAt: Option[Instant] = None,
  completedAt: Option[Instant] = None,
  createdAt: Instant = Instant.now()
)

// === DAG Run ===

case class DagRun(
  id: RunId,
  dagId: DagId,
  state: TaskState,
  triggeredBy: String,
  startedAt: Option[Instant] = None,
  completedAt: Option[Instant] = None,
  createdAt: Instant = Instant.now()
)
```

### Database Schema

```sql
-- File: migrations/V1__initial_schema.sql

CREATE TYPE task_state AS ENUM (
  'pending', 'queued', 'running', 'succeeded', 'failed', 'skipped'
);

CREATE TYPE task_type AS ENUM ('shell', 'http', 'python');

CREATE TABLE dags (
  id UUID PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  definition JSONB NOT NULL,
  schedule VARCHAR(100),
  is_active BOOLEAN DEFAULT true,
  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE dag_runs (
  id UUID PRIMARY KEY,
  dag_id UUID NOT NULL REFERENCES dags(id),
  state task_state NOT NULL DEFAULT 'pending',
  triggered_by VARCHAR(50) NOT NULL,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE task_instances (
  id UUID PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES dag_runs(id),
  task_id VARCHAR(255) NOT NULL,
  task_name VARCHAR(255) NOT NULL,
  state task_state NOT NULL DEFAULT 'pending',
  attempt INT NOT NULL DEFAULT 1,
  worker_id UUID,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(run_id, task_id, attempt)
);

-- Performance indexes
CREATE INDEX idx_task_instances_state ON task_instances(state);
CREATE INDEX idx_task_instances_run ON task_instances(run_id);
CREATE INDEX idx_dag_runs_dag ON dag_runs(dag_id);
CREATE INDEX idx_dag_runs_state ON dag_runs(state);
```

### Repository Layer

```scala
// File: src/main/scala/scheduler/persistence/DagRepository.scala

package scheduler.persistence

import scheduler.domain._
import zio._

trait DagRepository {
  def create(dag: DagDefinition): Task[DagId]
  def get(id: DagId): Task[Option[DagDefinition]]
  def getByName(name: String): Task[Option[DagDefinition]]
  def list(limit: Int, offset: Int): Task[List[DagDefinition]]
  def update(dag: DagDefinition): Task[Unit]
  def delete(id: DagId): Task[Unit]
}

trait TaskInstanceRepository {
  def create(instance: TaskInstance): Task[UUID]
  def get(id: UUID): Task[Option[TaskInstance]]
  def getByRunId(runId: RunId): Task[List[TaskInstance]]
  def updateState(id: UUID, state: TaskState): Task[Unit]
  def findReady(limit: Int): Task[List[TaskInstance]]
  def claimTask(id: UUID, workerId: WorkerId): Task[Boolean]
}

trait DagRunRepository {
  def create(run: DagRun): Task[RunId]
  def get(id: RunId): Task[Option[DagRun]]
  def getByDagId(dagId: DagId): Task[List[DagRun]]
  def updateState(id: RunId, state: TaskState): Task[Unit]
  def findPending(limit: Int): Task[List[DagRun]]
}
```

### Deliverables Checklist

- [ ] Project scaffolding with `build.sbt`
- [ ] All domain case classes with Circe codecs
- [ ] Flyway migrations for all tables
- [ ] Doobie `Transactor` configuration with ZIO
- [ ] `DagRepository` implementation with CRUD
- [ ] `TaskInstanceRepository` implementation
- [ ] `DagRunRepository` implementation
- [ ] Integration tests with Testcontainers PostgreSQL

---

## 4. Phase 2: DAG Parser + Single-Worker Executor

**Duration**: Weeks 3-4
**Estimated Hours**: 20-25

### Objectives

- Parse YAML DAG definitions
- Validate DAG structure with cycle detection
- Implement task executors (Shell, HTTP)
- Build single-worker sequential executor

### DAG Parser

```scala
// File: src/main/scala/scheduler/parser/DagParser.scala

package scheduler.parser

import scheduler.domain._
import io.circe.yaml.parser
import cats.syntax.all._

sealed trait DagParseError
object DagParseError {
  case class InvalidYaml(message: String) extends DagParseError
  case class MissingField(field: String) extends DagParseError
  case class InvalidTaskType(given: String) extends DagParseError
  case class InvalidDuration(value: String) extends DagParseError
}

sealed trait DagValidationError
object DagValidationError {
  case class CycleDetected(cycle: List[TaskId]) extends DagValidationError
  case class MissingDependency(task: TaskId, missing: TaskId) extends DagValidationError
  case class DuplicateTaskId(id: TaskId) extends DagValidationError
  case class EmptyDag(name: String) extends DagValidationError
}

trait DagParser {
  def parse(yaml: String): Either[DagParseError, DagDefinition]
  def validate(dag: DagDefinition): Either[DagValidationError, DagDefinition]
}

class DagParserImpl extends DagParser {

  def parse(yaml: String): Either[DagParseError, DagDefinition] = {
    parser.parse(yaml)
      .leftMap(e => DagParseError.InvalidYaml(e.getMessage))
      .flatMap(json => json.as[DagDefinition]
        .leftMap(e => DagParseError.MissingField(e.getMessage)))
  }

  def validate(dag: DagDefinition): Either[DagValidationError, DagDefinition] = {
    for {
      _ <- validateNonEmpty(dag)
      _ <- validateUniqueIds(dag)
      _ <- validateDependencies(dag)
      _ <- validateNoCycles(dag)
    } yield dag
  }

  private def validateNoCycles(dag: DagDefinition): Either[DagValidationError, Unit] = {
    DagValidator.detectCycle(dag.tasks) match {
      case Some(cycle) => Left(DagValidationError.CycleDetected(cycle))
      case None => Right(())
    }
  }

  // ... other validation methods
}
```

### Cycle Detection (Kahn's Algorithm)

```scala
// File: src/main/scala/scheduler/parser/DagValidator.scala

package scheduler.parser

import scheduler.domain._
import scala.collection.mutable

object DagValidator {

  def detectCycle(tasks: List[TaskDefinition]): Option[List[TaskId]] = {
    val graph = tasks.map(t => t.id -> t.dependencies).toMap
    val inDegree = mutable.Map[TaskId, Int]().withDefaultValue(0)

    // Calculate in-degrees
    tasks.foreach { task =>
      task.dependencies.foreach { dep =>
        inDegree(dep) = inDegree(dep) + 1
      }
    }

    val queue = mutable.Queue[TaskId]()
    val sorted = mutable.ListBuffer[TaskId]()

    // Start with nodes having no incoming edges
    tasks.map(_.id).filter(id => !inDegree.contains(id) || inDegree(id) == 0)
      .foreach(queue.enqueue(_))

    while (queue.nonEmpty) {
      val node = queue.dequeue()
      sorted += node
      graph.getOrElse(node, Set.empty).foreach { neighbor =>
        inDegree(neighbor) = inDegree(neighbor) - 1
        if (inDegree(neighbor) == 0) {
          queue.enqueue(neighbor)
        }
      }
    }

    if (sorted.size != tasks.size) {
      Some(findCycle(graph, tasks.map(_.id).toSet))
    } else {
      None
    }
  }

  def topologicalSort(tasks: List[TaskDefinition]): Either[DagValidationError, List[TaskDefinition]] = {
    detectCycle(tasks) match {
      case Some(cycle) => Left(DagValidationError.CycleDetected(cycle))
      case None =>
        val taskMap = tasks.map(t => t.id -> t).toMap
        val sorted = kahnSort(tasks)
        Right(sorted.flatMap(id => taskMap.get(id)))
    }
  }

  private def kahnSort(tasks: List[TaskDefinition]): List[TaskId] = {
    // Implementation returns topologically sorted task IDs
    ???
  }

  private def findCycle(
    graph: Map[TaskId, Set[TaskId]],
    nodes: Set[TaskId]
  ): List[TaskId] = {
    // DFS-based cycle finding for error reporting
    ???
  }
}
```

### Task Executor

```scala
// File: src/main/scala/scheduler/executor/TaskExecutor.scala

package scheduler.executor

import scheduler.domain._
import zio._
import java.time.Duration

case class ExecutionContext(
  runId: RunId,
  dagId: DagId,
  attempt: Int,
  startTime: java.time.Instant
) {
  def elapsed: Duration = Duration.between(startTime, java.time.Instant.now())
}

case class TaskResult(
  success: Boolean,
  output: Option[String],
  error: Option[String],
  duration: Duration
)

trait TaskExecutor {
  def execute(task: TaskDefinition, context: ExecutionContext): Task[TaskResult]
}

class ShellTaskExecutor extends TaskExecutor {

  def execute(task: TaskDefinition, ctx: ExecutionContext): Task[TaskResult] = {
    task.taskType match {
      case TaskType.Shell(command, env) =>
        executeShell(command, env, task.timeout, ctx)
      case other =>
        ZIO.fail(new IllegalArgumentException(s"ShellTaskExecutor cannot handle $other"))
    }
  }

  private def executeShell(
    command: String,
    env: Map[String, String],
    timeout: Duration,
    ctx: ExecutionContext
  ): Task[TaskResult] = {
    ZIO.scoped {
      for {
        startTime <- Clock.instant
        process <- ZIO.attemptBlocking {
          val pb = new ProcessBuilder("sh", "-c", command)
          env.foreach { case (k, v) => pb.environment().put(k, v) }
          pb.redirectErrorStream(true)
          pb.start()
        }
        output <- ZIO.attemptBlocking {
          scala.io.Source.fromInputStream(process.getInputStream).mkString
        }.fork
        exitCode <- ZIO.attemptBlocking(process.waitFor())
          .timeout(zio.Duration.fromJava(timeout))
          .someOrFail(TaskTimeoutError(ctx.runId))
        outputStr <- output.join
        endTime <- Clock.instant
      } yield TaskResult(
        success = exitCode == 0,
        output = Some(outputStr.take(10000)), // Limit output size
        error = if (exitCode != 0) Some(s"Exit code: $exitCode") else None,
        duration = Duration.between(startTime, endTime)
      )
    }
  }
}

class HttpTaskExecutor extends TaskExecutor {

  def execute(task: TaskDefinition, ctx: ExecutionContext): Task[TaskResult] = {
    task.taskType match {
      case TaskType.Http(method, url, headers, body) =>
        executeHttp(method, url, headers, body, task.timeout, ctx)
      case other =>
        ZIO.fail(new IllegalArgumentException(s"HttpTaskExecutor cannot handle $other"))
    }
  }

  private def executeHttp(
    method: String,
    url: String,
    headers: Map[String, String],
    body: Option[io.circe.Json],
    timeout: Duration,
    ctx: ExecutionContext
  ): Task[TaskResult] = {
    // Implementation using http4s client
    ???
  }
}

case class TaskTimeoutError(runId: RunId) extends Exception(s"Task timed out for run $runId")
```

### Simple Scheduler

```scala
// File: src/main/scala/scheduler/SimpleScheduler.scala

package scheduler

import scheduler.domain._
import scheduler.persistence._
import scheduler.executor._
import scheduler.parser._
import zio._

class SimpleScheduler(
  dagRepo: DagRepository,
  taskRepo: TaskInstanceRepository,
  runRepo: DagRunRepository,
  executor: TaskExecutor
) {

  def executeDag(dagId: DagId): Task[DagRun] = {
    for {
      dag <- dagRepo.get(dagId).someOrFail(DagNotFound(dagId))
      runId = RunId.generate
      run = DagRun(runId, dagId, TaskState.Running, "manual")
      _ <- runRepo.create(run)
      sorted <- ZIO.fromEither(DagValidator.topologicalSort(dag.tasks))
        .mapError(e => ValidationError(e.toString))
      _ <- createTaskInstances(sorted, runId, dagId)
      _ <- executeTasks(sorted, runId)
      completedRun <- runRepo.get(runId).someOrFail(RunNotFound(runId))
    } yield completedRun
  }

  private def createTaskInstances(
    tasks: List[TaskDefinition],
    runId: RunId,
    dagId: DagId
  ): Task[Unit] = {
    ZIO.foreach(tasks) { task =>
      val instance = TaskInstance(
        id = java.util.UUID.randomUUID(),
        taskId = task.id,
        runId = runId,
        dagId = dagId,
        taskName = task.name,
        state = TaskState.Pending
      )
      taskRepo.create(instance)
    }.unit
  }

  private def executeTasks(tasks: List[TaskDefinition], runId: RunId): Task[Unit] = {
    ZIO.foreach(tasks) { task =>
      executeWithRetry(task, runId)
    }.unit
  }

  private def executeWithRetry(task: TaskDefinition, runId: RunId): Task[TaskResult] = {
    val policy = task.retryPolicy
    val ctx = ExecutionContext(runId, DagId.generate, 1, java.time.Instant.now())

    executor.execute(task, ctx)
      .retry(
        Schedule.exponential(zio.Duration.fromJava(policy.initialDelay)) &&
        Schedule.recurs(policy.maxAttempts - 1)
      )
  }
}

case class DagNotFound(id: DagId) extends Exception(s"DAG not found: $id")
case class RunNotFound(id: RunId) extends Exception(s"Run not found: $id")
case class ValidationError(msg: String) extends Exception(msg)
```

### Deliverables Checklist

- [ ] YAML parser with custom decoders
- [ ] Cycle detection algorithm
- [ ] Topological sort implementation
- [ ] `ShellTaskExecutor` with process management
- [ ] `HttpTaskExecutor` with http4s client
- [ ] `SimpleScheduler` for sequential execution
- [ ] Unit tests for parser and validator
- [ ] Integration tests for executors

---

## 5. Phase 3: REST API + Worker Pool

**Duration**: Weeks 5-6
**Estimated Hours**: 25-30

### Objectives

- Expose scheduler via REST API with Tapir
- Implement concurrent worker pool
- Add work distribution and load balancing
- Generate OpenAPI documentation

### API Endpoints

```scala
// File: src/main/scala/scheduler/api/Endpoints.scala

package scheduler.api

import scheduler.domain._
import sttp.tapir._
import sttp.tapir.json.circe._
import sttp.tapir.generic.auto._
import java.util.UUID

object Endpoints {

  // === Request/Response Models ===

  case class CreateDagRequest(yaml: String)
  case class DagResponse(id: DagId, name: String, taskCount: Int, version: Int)
  case class TriggerRunRequest(triggeredBy: Option[String])
  case class RunResponse(runId: RunId, dagId: DagId, state: String)
  case class RunStatusResponse(
    runId: RunId,
    dagId: DagId,
    state: String,
    tasks: List[TaskStatusResponse],
    startedAt: Option[String],
    completedAt: Option[String]
  )
  case class TaskStatusResponse(
    taskId: String,
    name: String,
    state: String,
    attempt: Int,
    workerId: Option[String]
  )
  case class WorkerResponse(id: WorkerId, status: String, tasksRunning: Int)
  case class ApiError(code: String, message: String)

  // === Endpoints ===

  val createDag: Endpoint[Unit, CreateDagRequest, ApiError, DagResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "dags")
      .in(jsonBody[CreateDagRequest])
      .out(jsonBody[DagResponse])
      .errorOut(jsonBody[ApiError])
      .description("Create a new DAG from YAML definition")

  val getDag: Endpoint[Unit, UUID, ApiError, DagResponse, Any] =
    endpoint.get
      .in("api" / "v1" / "dags" / path[UUID]("dagId"))
      .out(jsonBody[DagResponse])
      .errorOut(jsonBody[ApiError])
      .description("Get DAG details by ID")

  val listDags: Endpoint[Unit, (Int, Int), ApiError, List[DagResponse], Any] =
    endpoint.get
      .in("api" / "v1" / "dags")
      .in(query[Int]("limit").default(20))
      .in(query[Int]("offset").default(0))
      .out(jsonBody[List[DagResponse]])
      .errorOut(jsonBody[ApiError])
      .description("List all DAGs with pagination")

  val triggerRun: Endpoint[Unit, UUID, ApiError, RunResponse, Any] =
    endpoint.post
      .in("api" / "v1" / "dags" / path[UUID]("dagId") / "runs")
      .out(jsonBody[RunResponse])
      .errorOut(jsonBody[ApiError])
      .description("Trigger a new run for a DAG")

  val getRunStatus: Endpoint[Unit, UUID, ApiError, RunStatusResponse, Any] =
    endpoint.get
      .in("api" / "v1" / "runs" / path[UUID]("runId"))
      .out(jsonBody[RunStatusResponse])
      .errorOut(jsonBody[ApiError])
      .description("Get detailed status of a DAG run")

  val listWorkers: Endpoint[Unit, Unit, ApiError, List[WorkerResponse], Any] =
    endpoint.get
      .in("api" / "v1" / "workers")
      .out(jsonBody[List[WorkerResponse]])
      .errorOut(jsonBody[ApiError])
      .description("List all active workers")

  val all = List(createDag, getDag, listDags, triggerRun, getRunStatus, listWorkers)
}
```

### Worker Pool

```scala
// File: src/main/scala/scheduler/worker/WorkerPool.scala

package scheduler.worker

import scheduler.domain._
import scheduler.persistence._
import scheduler.executor._
import zio._
import zio.stream._

sealed trait WorkerStatus
object WorkerStatus {
  case object Idle extends WorkerStatus
  case class Running(taskIds: Set[UUID]) extends WorkerStatus
  case object Draining extends WorkerStatus
  case object Stopped extends WorkerStatus
}

case class Worker(
  id: WorkerId,
  concurrency: Int,
  taskQueue: Queue[TaskAssignment],
  status: Ref[WorkerStatus]
)

case class TaskAssignment(
  instanceId: UUID,
  task: TaskDefinition,
  runId: RunId,
  dagId: DagId,
  attempt: Int
)

class WorkerPool(
  poolSize: Int,
  concurrencyPerWorker: Int,
  taskRepo: TaskInstanceRepository,
  executor: TaskExecutor
) {

  def start: ZIO[Scope, Nothing, Ref[Map[WorkerId, Worker]]] = {
    for {
      workersRef <- Ref.make(Map.empty[WorkerId, Worker])
      workers <- ZIO.foreach(1 to poolSize) { _ =>
        createWorker(concurrencyPerWorker)
      }
      _ <- workersRef.set(workers.map(w => w.id -> w).toMap)
      _ <- ZIO.foreach(workers)(runWorker).forkScoped
    } yield workersRef
  }

  private def createWorker(concurrency: Int): UIO[Worker] = {
    for {
      id <- ZIO.succeed(WorkerId.generate)
      queue <- Queue.bounded[TaskAssignment](100)
      status <- Ref.make[WorkerStatus](WorkerStatus.Idle)
    } yield Worker(id, concurrency, queue, status)
  }

  private def runWorker(worker: Worker): Task[Nothing] = {
    ZStream
      .fromQueue(worker.taskQueue)
      .mapZIOPar(worker.concurrency) { assignment =>
        executeTask(worker, assignment)
          .catchAll(e => handleTaskFailure(worker, assignment, e))
      }
      .runDrain
  }

  private def executeTask(worker: Worker, assignment: TaskAssignment): Task[Unit] = {
    for {
      _ <- updateWorkerStatus(worker, assignment.instanceId, running = true)
      _ <- taskRepo.updateState(assignment.instanceId, TaskState.Running)

      ctx = ExecutionContext(
        assignment.runId,
        assignment.dagId,
        assignment.attempt,
        java.time.Instant.now()
      )
      result <- executor.execute(assignment.task, ctx)
        .timeout(zio.Duration.fromJava(assignment.task.timeout))

      newState = result match {
        case Some(r) if r.success => TaskState.Succeeded(java.time.Instant.now())
        case Some(r) => TaskState.Failed(r.error.getOrElse("Unknown error"), assignment.attempt)
        case None => TaskState.Failed("Timeout", assignment.attempt)
      }
      _ <- taskRepo.updateState(assignment.instanceId, newState)
      _ <- updateWorkerStatus(worker, assignment.instanceId, running = false)
    } yield ()
  }

  private def updateWorkerStatus(
    worker: Worker,
    taskId: UUID,
    running: Boolean
  ): UIO[Unit] = {
    worker.status.update {
      case WorkerStatus.Running(tasks) if running =>
        WorkerStatus.Running(tasks + taskId)
      case WorkerStatus.Running(tasks) =>
        val remaining = tasks - taskId
        if (remaining.isEmpty) WorkerStatus.Idle else WorkerStatus.Running(remaining)
      case WorkerStatus.Idle if running =>
        WorkerStatus.Running(Set(taskId))
      case other => other
    }
  }

  private def handleTaskFailure(
    worker: Worker,
    assignment: TaskAssignment,
    error: Throwable
  ): UIO[Unit] = {
    for {
      _ <- taskRepo.updateState(
        assignment.instanceId,
        TaskState.Failed(error.getMessage, assignment.attempt)
      ).orDie
      _ <- updateWorkerStatus(worker, assignment.instanceId, running = false)
    } yield ()
  }
}
```

### Task Router

```scala
// File: src/main/scala/scheduler/scheduler/TaskRouter.scala

package scheduler.scheduler

import scheduler.domain._
import scheduler.persistence._
import scheduler.worker._
import zio._

class TaskRouter(
  taskRepo: TaskInstanceRepository,
  workers: Ref[Map[WorkerId, Worker]]
) {

  def start: Task[Nothing] = {
    pollForReadyTasks
      .repeat(Schedule.fixed(100.millis))
      .forever
  }

  private def pollForReadyTasks: Task[Unit] = {
    for {
      ready <- taskRepo.findReady(limit = 50)
      _ <- ZIO.foreachParDiscard(ready)(assignToWorker)
    } yield ()
  }

  private def assignToWorker(task: TaskInstance): Task[Unit] = {
    for {
      workerMap <- workers.get
      worker <- selectLeastLoadedWorker(workerMap.values.toList)
      claimed <- taskRepo.claimTask(task.id, worker.id)
      _ <- ZIO.when(claimed) {
        // Fetch full task definition and create assignment
        createAndEnqueueAssignment(worker, task)
      }
    } yield ()
  }

  private def selectLeastLoadedWorker(workers: List[Worker]): Task[Worker] = {
    ZIO.foreach(workers) { worker =>
      worker.taskQueue.size.map(size => (worker, size))
    }.map(_.minBy(_._2)._1)
  }

  private def createAndEnqueueAssignment(
    worker: Worker,
    instance: TaskInstance
  ): Task[Unit] = {
    // Create TaskAssignment and offer to worker queue
    ???
  }
}
```

### Deliverables Checklist

- [ ] All Tapir endpoint definitions
- [ ] API route handlers with ZIO
- [ ] OpenAPI documentation generation
- [ ] `Worker` implementation with concurrent execution
- [ ] `WorkerPool` with configurable size
- [ ] `TaskRouter` with polling and assignment
- [ ] Least-loaded worker selection
- [ ] API integration tests
- [ ] Load testing with multiple concurrent DAGs

---

## 6. Phase 4: Distributed Coordination with etcd

**Duration**: Weeks 7-9
**Estimated Hours**: 30-35

### Objectives

- Implement leader election with etcd
- Add distributed locks for task claiming
- Create worker registry with heartbeats
- Enable work stealing between workers

### Distributed Coordinator

```scala
// File: src/main/scala/scheduler/coordination/DistributedCoordinator.scala

package scheduler.coordination

import scheduler.domain._
import zio._
import zio.stream._

trait DistributedCoordinator {
  def acquireLeadership(candidateId: String): Task[LeadershipLease]
  def acquireLock(lockName: String, ttl: zio.Duration): Task[DistributedLock]
  def registerWorker(worker: WorkerInfo): Task[Unit]
  def deregisterWorker(workerId: WorkerId): Task[Unit]
  def watchWorkers: ZStream[Any, Throwable, WorkerEvent]
  def getClusterState: Task[ClusterState]
}

case class LeadershipLease(
  isLeader: Ref[Boolean],
  keepAlive: Fiber[Throwable, Unit],
  release: Task[Unit]
)

case class DistributedLock(
  key: String,
  leaseId: Long,
  release: Task[Unit]
)

case class WorkerInfo(
  id: WorkerId,
  host: String,
  port: Int,
  capacity: Int,
  registeredAt: java.time.Instant
)

sealed trait WorkerEvent
object WorkerEvent {
  case class Joined(worker: WorkerInfo) extends WorkerEvent
  case class Left(workerId: WorkerId) extends WorkerEvent
  case class Updated(worker: WorkerInfo) extends WorkerEvent
}

case class ClusterState(
  leaderId: Option[String],
  workers: List[WorkerInfo],
  activeRuns: Int
)
```

### etcd Implementation

```scala
// File: src/main/scala/scheduler/coordination/EtcdCoordinator.scala

package scheduler.coordination

import scheduler.domain._
import io.etcd.jetcd._
import io.etcd.jetcd.options._
import io.etcd.jetcd.watch._
import zio._
import zio.stream._
import scala.jdk.CollectionConverters._

class EtcdCoordinator(client: Client) extends DistributedCoordinator {

  private val leaderKey = "/scheduler/leader"
  private val workersPrefix = "/scheduler/workers/"
  private val locksPrefix = "/scheduler/locks/"

  def acquireLeadership(candidateId: String): Task[LeadershipLease] = {
    for {
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(30).get()
      )
      isLeader <- Ref.make(false)

      acquired <- attemptLeaderElection(candidateId, lease.getID)
      _ <- isLeader.set(acquired)

      keepAlive <- startLeaseKeepAlive(lease.getID).fork
      watchFiber <- watchLeadership(candidateId, isLeader).fork

    } yield LeadershipLease(
      isLeader,
      keepAlive,
      release = for {
        _ <- keepAlive.interrupt
        _ <- watchFiber.interrupt
        _ <- ZIO.attemptBlocking(client.getLeaseClient.revoke(lease.getID).get())
      } yield ()
    )
  }

  private def attemptLeaderElection(candidateId: String, leaseId: Long): Task[Boolean] = {
    ZIO.attemptBlocking {
      val txn = client.getKVClient.txn()
      val key = ByteSequence.from(leaderKey.getBytes)
      val value = ByteSequence.from(candidateId.getBytes)

      txn
        .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
        .Then(Op.put(key, value, PutOption.builder().withLeaseId(leaseId).build()))
        .commit()
        .get()
        .isSucceeded
    }
  }

  private def startLeaseKeepAlive(leaseId: Long): Task[Unit] = {
    ZIO.attemptBlocking {
      client.getLeaseClient.keepAlive(
        leaseId,
        new StreamObserver[LeaseKeepAliveResponse] {
          def onNext(value: LeaseKeepAliveResponse): Unit = ()
          def onError(t: Throwable): Unit = ()
          def onCompleted(): Unit = ()
        }
      )
    }.unit.repeat(Schedule.fixed(10.seconds)).unit
  }

  def acquireLock(lockName: String, ttl: zio.Duration): Task[DistributedLock] = {
    val lockKey = s"$locksPrefix$lockName"

    for {
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(ttl.toSeconds).get()
      )
      lock <- ZIO.attemptBlocking(
        client.getLockClient.lock(
          ByteSequence.from(lockKey.getBytes),
          lease.getID
        ).get()
      )
    } yield DistributedLock(
      key = new String(lock.getKey.getBytes),
      leaseId = lease.getID,
      release = ZIO.attemptBlocking(
        client.getLockClient.unlock(lock.getKey).get()
      ).unit
    )
  }

  def registerWorker(worker: WorkerInfo): Task[Unit] = {
    val key = s"$workersPrefix${worker.id.value}"

    for {
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(30).get()
      )
      _ <- ZIO.attemptBlocking {
        val keyBytes = ByteSequence.from(key.getBytes)
        val valueBytes = ByteSequence.from(workerToJson(worker).getBytes)
        client.getKVClient.put(
          keyBytes,
          valueBytes,
          PutOption.builder().withLeaseId(lease.getID).build()
        ).get()
      }
      _ <- startLeaseKeepAlive(lease.getID).fork
    } yield ()
  }

  def watchWorkers: ZStream[Any, Throwable, WorkerEvent] = {
    ZStream.asyncZIO[Any, Throwable, WorkerEvent] { callback =>
      ZIO.attemptBlocking {
        val keyBytes = ByteSequence.from(workersPrefix.getBytes)
        client.getWatchClient.watch(
          keyBytes,
          WatchOption.builder().withPrefix(keyBytes).build(),
          new Watch.Listener {
            override def onNext(response: WatchResponse): Unit = {
              response.getEvents.asScala.foreach { event =>
                val workerEvent = event.getEventType match {
                  case WatchEvent.EventType.PUT =>
                    WorkerEvent.Joined(parseWorker(event.getKeyValue))
                  case WatchEvent.EventType.DELETE =>
                    WorkerEvent.Left(extractWorkerId(event.getKeyValue))
                  case _ => return
                }
                callback(ZIO.succeed(Chunk(workerEvent)))
              }
            }
            override def onError(t: Throwable): Unit =
              callback(ZIO.fail(Some(t)))
            override def onCompleted(): Unit =
              callback(ZIO.fail(None))
          }
        )
      }.unit
    }
  }

  // Helper methods
  private def workerToJson(worker: WorkerInfo): String = ???
  private def parseWorker(kv: KeyValue): WorkerInfo = ???
  private def extractWorkerId(kv: KeyValue): WorkerId = ???
  private def watchLeadership(candidateId: String, isLeader: Ref[Boolean]): Task[Unit] = ???

  def deregisterWorker(workerId: WorkerId): Task[Unit] = ???
  def getClusterState: Task[ClusterState] = ???
}
```

### Distributed Scheduler

```scala
// File: src/main/scala/scheduler/scheduler/DistributedScheduler.scala

package scheduler.scheduler

import scheduler.domain._
import scheduler.coordination._
import scheduler.persistence._
import zio._

class DistributedScheduler(
  coordinator: DistributedCoordinator,
  taskRouter: TaskRouter,
  runRepo: DagRunRepository
) {

  def start: Task[Nothing] = {
    for {
      instanceId <- ZIO.succeed(java.util.UUID.randomUUID().toString)
      lease <- coordinator.acquireLeadership(instanceId)
      _ <- runSchedulerLoop(lease).forever
    } yield throw new IllegalStateException("Scheduler should never exit")
  }

  private def runSchedulerLoop(lease: LeadershipLease): Task[Unit] = {
    lease.isLeader.get.flatMap { isLeader =>
      if (isLeader) {
        for {
          _ <- ZIO.logInfo("Running as leader")
          pendingRuns <- runRepo.findPending(limit = 100)
          _ <- ZIO.foreachParDiscard(pendingRuns) { run =>
            scheduleRunWithLock(run)
          }
        } yield ()
      } else {
        ZIO.logDebug("Not leader, standing by...") *> ZIO.sleep(1.second)
      }
    }
  }

  private def scheduleRunWithLock(run: DagRun): Task[Unit] = {
    coordinator.acquireLock(s"run-${run.id.value}", 30.seconds).flatMap { lock =>
      scheduleRun(run).ensuring(lock.release)
    }
  }

  private def scheduleRun(run: DagRun): Task[Unit] = {
    // Schedule tasks for execution
    ???
  }
}
```

### Work Stealing

```scala
// File: src/main/scala/scheduler/worker/WorkStealing.scala

package scheduler.worker

import scheduler.domain._
import zio._

class WorkStealingRouter(
  workers: Ref[Map[WorkerId, Worker]]
) {

  private val stealThreshold = 5

  def startWorkStealing: Task[Nothing] = {
    checkAndSteal
      .repeat(Schedule.fixed(500.millis))
      .forever
  }

  private def checkAndSteal: Task[Unit] = {
    for {
      workerMap <- workers.get
      loads <- ZIO.foreach(workerMap.toList) { case (id, worker) =>
        worker.taskQueue.size.map(size => (id, worker, size))
      }
      _ <- rebalanceIfNeeded(loads)
    } yield ()
  }

  private def rebalanceIfNeeded(
    loads: List[(WorkerId, Worker, Int)]
  ): Task[Unit] = {
    val sorted = loads.sortBy(_._3)
    if (sorted.isEmpty) ZIO.unit
    else {
      val (_, leastLoaded, minLoad) = sorted.head
      val (_, mostLoaded, maxLoad) = sorted.last

      if (maxLoad - minLoad > stealThreshold) {
        stealTasks(
          from = mostLoaded,
          to = leastLoaded,
          count = (maxLoad - minLoad) / 2
        )
      } else ZIO.unit
    }
  }

  private def stealTasks(from: Worker, to: Worker, count: Int): Task[Unit] = {
    ZIO.replicateZIO(count) {
      from.taskQueue.poll.flatMap {
        case Some(task) => to.taskQueue.offer(task).unit
        case None => ZIO.unit
      }
    }.unit
  }
}
```

### Deliverables Checklist

- [ ] etcd client configuration
- [ ] Leader election implementation
- [ ] Distributed lock acquisition/release
- [ ] Worker registration with heartbeats
- [ ] Worker event streaming
- [ ] `DistributedScheduler` with leader-only execution
- [ ] Work stealing implementation
- [ ] Chaos testing for leader failure
- [ ] Multi-node integration tests

---

## 7. Phase 5: Production Hardening + Dead Letter Queue

**Duration**: Weeks 10-12
**Estimated Hours**: 25-30

### Objectives

- Implement Dead Letter Queue for failed tasks
- Add Prometheus metrics
- Create health check endpoints
- Implement graceful shutdown

### Dead Letter Queue Schema

```sql
-- File: migrations/V5__dead_letter_queue.sql

CREATE TABLE dead_letter_queue (
  id UUID PRIMARY KEY,
  task_instance_id UUID NOT NULL REFERENCES task_instances(id),
  run_id UUID NOT NULL REFERENCES dag_runs(id),
  dag_id UUID NOT NULL REFERENCES dags(id),
  task_name VARCHAR(255) NOT NULL,
  error_type VARCHAR(100) NOT NULL,
  error_message TEXT NOT NULL,
  stack_trace TEXT,
  last_attempt_at TIMESTAMPTZ NOT NULL,
  total_attempts INT NOT NULL,
  task_config JSONB NOT NULL,
  execution_context JSONB,
  resolution_status VARCHAR(50) DEFAULT 'pending',
  resolved_at TIMESTAMPTZ,
  resolved_by VARCHAR(255),
  resolution_notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_status ON dead_letter_queue(resolution_status);
CREATE INDEX idx_dlq_dag ON dead_letter_queue(dag_id);
CREATE INDEX idx_dlq_created ON dead_letter_queue(created_at);
```

### Dead Letter Queue Implementation

```scala
// File: src/main/scala/scheduler/dlq/DeadLetterQueue.scala

package scheduler.dlq

import scheduler.domain._
import zio._
import java.util.UUID

case class DeadLetter(
  id: UUID,
  taskInstanceId: UUID,
  runId: RunId,
  dagId: DagId,
  taskName: String,
  errorType: String,
  errorMessage: String,
  stackTrace: Option[String],
  totalAttempts: Int,
  resolutionStatus: String,
  createdAt: java.time.Instant
)

case class TaskFailure(
  taskInstance: TaskInstance,
  error: Throwable,
  isRetryExhausted: Boolean
)

trait DeadLetterQueue {
  def enqueue(failure: TaskFailure): Task[UUID]
  def listPending(limit: Int, offset: Int): Task[List[DeadLetter]]
  def getById(id: UUID): Task[Option[DeadLetter]]
  def retry(id: UUID): Task[RunId]
  def skip(id: UUID, reason: String): Task[Unit]
  def resolve(id: UUID, notes: String): Task[Unit]
}

class DeadLetterQueueImpl(
  repo: DlqRepository,
  scheduler: DistributedScheduler
) extends DeadLetterQueue {

  def enqueue(failure: TaskFailure): Task[UUID] = {
    val entry = DeadLetterEntry(
      id = UUID.randomUUID(),
      taskInstanceId = failure.taskInstance.id,
      runId = failure.taskInstance.runId,
      dagId = failure.taskInstance.dagId,
      taskName = failure.taskInstance.taskName,
      errorType = failure.error.getClass.getSimpleName,
      errorMessage = failure.error.getMessage,
      stackTrace = Some(failure.error.getStackTrace.mkString("\n")),
      lastAttemptAt = java.time.Instant.now(),
      totalAttempts = failure.taskInstance.attempt
    )

    for {
      id <- repo.insert(entry)
      _ <- ZIO.logWarning(
        s"Task ${entry.taskName} moved to DLQ after ${entry.totalAttempts} attempts: ${entry.errorMessage}"
      )
      _ <- Metrics.dlqEnqueued.increment
    } yield id
  }

  def retry(id: UUID): Task[RunId] = {
    for {
      entry <- repo.get(id).someOrFail(DlqEntryNotFound(id))
      runId <- scheduler.retryTask(entry.taskInstanceId)
      _ <- repo.updateStatus(id, "retried")
    } yield runId
  }

  def skip(id: UUID, reason: String): Task[Unit] = {
    repo.updateStatus(id, "skipped", Some(reason))
  }

  def resolve(id: UUID, notes: String): Task[Unit] = {
    repo.updateStatus(id, "resolved", Some(notes))
  }

  def listPending(limit: Int, offset: Int): Task[List[DeadLetter]] =
    repo.findByStatus("pending", limit, offset)

  def getById(id: UUID): Task[Option[DeadLetter]] =
    repo.get(id)
}

case class DlqEntryNotFound(id: UUID) extends Exception(s"DLQ entry not found: $id")
```

### Metrics

```scala
// File: src/main/scala/scheduler/metrics/Metrics.scala

package scheduler.metrics

import io.prometheus.client._
import zio._

object Metrics {

  val registry = new CollectorRegistry()

  val tasksProcessed: Counter = Counter.build()
    .name("scheduler_tasks_processed_total")
    .help("Total number of tasks processed")
    .labelNames("status", "task_type")
    .register(registry)

  val taskDuration: Histogram = Histogram.build()
    .name("scheduler_task_duration_seconds")
    .help("Task execution duration in seconds")
    .labelNames("task_type")
    .buckets(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0)
    .register(registry)

  val queueDepth: Gauge = Gauge.build()
    .name("scheduler_queue_depth")
    .help("Current number of tasks in queue")
    .labelNames("worker_id")
    .register(registry)

  val dlqSize: Gauge = Gauge.build()
    .name("scheduler_dlq_size")
    .help("Number of items in dead letter queue")
    .register(registry)

  val activeWorkers: Gauge = Gauge.build()
    .name("scheduler_active_workers")
    .help("Number of active workers")
    .register(registry)

  val leaderStatus: Gauge = Gauge.build()
    .name("scheduler_is_leader")
    .help("Whether this instance is the leader (1) or not (0)")
    .register(registry)

  val dlqEnqueued: Counter = Counter.build()
    .name("scheduler_dlq_enqueued_total")
    .help("Total items added to dead letter queue")
    .register(registry)
}
```

### Health Checks

```scala
// File: src/main/scala/scheduler/health/HealthChecker.scala

package scheduler.health

import scheduler.coordination._
import scheduler.worker._
import zio._
import doobie.util.transactor.Transactor

case class HealthStatus(
  status: String,
  components: Map[String, ComponentHealth]
)

case class ComponentHealth(
  status: String,
  latency: Option[Long],
  message: Option[String]
)

class HealthChecker(
  db: Transactor[Task],
  coordinator: DistributedCoordinator,
  workers: Ref[Map[WorkerId, Worker]]
) {

  def check: Task[HealthStatus] = {
    for {
      dbHealth <- checkDatabase
      etcdHealth <- checkEtcd
      workersHealth <- checkWorkers

      components = Map(
        "database" -> dbHealth,
        "etcd" -> etcdHealth,
        "workers" -> workersHealth
      )

      overallStatus = determineOverallStatus(components.values.toList)
    } yield HealthStatus(overallStatus, components)
  }

  private def checkDatabase: Task[ComponentHealth] = {
    val start = System.currentTimeMillis()

    // Simple health check query
    ZIO.unit // Replace with actual DB query
      .as {
        val latency = System.currentTimeMillis() - start
        ComponentHealth("healthy", Some(latency), None)
      }
      .timeout(5.seconds)
      .someOrElse(ComponentHealth("unhealthy", None, Some("Timeout")))
      .catchAll(e =>
        ZIO.succeed(ComponentHealth("unhealthy", None, Some(e.getMessage)))
      )
  }

  private def checkEtcd: Task[ComponentHealth] = {
    coordinator.getClusterState
      .as(ComponentHealth("healthy", None, None))
      .timeout(5.seconds)
      .someOrElse(ComponentHealth("unhealthy", None, Some("Timeout")))
      .catchAll(e =>
        ZIO.succeed(ComponentHealth("unhealthy", None, Some(e.getMessage)))
      )
  }

  private def checkWorkers: Task[ComponentHealth] = {
    workers.get.map { workerMap =>
      if (workerMap.nonEmpty) {
        ComponentHealth("healthy", None, Some(s"${workerMap.size} workers active"))
      } else {
        ComponentHealth("degraded", None, Some("No workers available"))
      }
    }
  }

  private def determineOverallStatus(components: List[ComponentHealth]): String = {
    if (components.forall(_.status == "healthy")) "healthy"
    else if (components.exists(_.status == "unhealthy")) "unhealthy"
    else "degraded"
  }
}
```

### Graceful Shutdown

```scala
// File: src/main/scala/scheduler/lifecycle/GracefulShutdown.scala

package scheduler.lifecycle

import scheduler.coordination._
import scheduler.worker._
import zio._

class GracefulShutdown(
  coordinator: DistributedCoordinator,
  workers: Ref[Map[WorkerId, Worker]],
  leaderLease: LeadershipLease
) {

  def shutdown: Task[Unit] = {
    for {
      _ <- ZIO.logInfo("Initiating graceful shutdown...")

      // Release leadership
      _ <- leaderLease.release
        .catchAll(e => ZIO.logError(s"Error releasing leadership: ${e.getMessage}"))

      // Drain all workers
      workerMap <- workers.get
      _ <- ZIO.foreachParDiscard(workerMap.values.toList) { worker =>
        drainWorker(worker, timeout = 60.seconds)
      }

      // Deregister workers
      _ <- ZIO.foreachDiscard(workerMap.keys) { workerId =>
        coordinator.deregisterWorker(workerId)
          .catchAll(e => ZIO.logError(s"Error deregistering worker: ${e.getMessage}"))
      }

      _ <- ZIO.logInfo("Graceful shutdown complete")
    } yield ()
  }

  private def drainWorker(worker: Worker, timeout: zio.Duration): Task[Unit] = {
    for {
      _ <- worker.status.set(WorkerStatus.Draining)
      _ <- ZIO.logInfo(s"Draining worker ${worker.id.value}")

      // Wait for in-flight tasks
      _ <- worker.status.get.repeatUntil {
        case WorkerStatus.Running(tasks) => tasks.isEmpty
        case _ => true
      }.timeout(timeout).someOrElse(())

      _ <- worker.status.set(WorkerStatus.Stopped)
      _ <- ZIO.logInfo(s"Worker ${worker.id.value} stopped")
    } yield ()
  }
}
```

### Deliverables Checklist

- [ ] DLQ database schema and repository
- [ ] `DeadLetterQueue` service implementation
- [ ] DLQ API endpoints (list, retry, skip, resolve)
- [ ] Prometheus metrics for all components
- [ ] `/metrics` endpoint
- [ ] `HealthChecker` implementation
- [ ] `/health` endpoint
- [ ] `GracefulShutdown` with worker draining
- [ ] Signal handling (SIGTERM, SIGINT)
- [ ] Alerting rules documentation

---

## 8. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **etcd complexity** | High | Medium | Start with single-node etcd; use Testcontainers for local dev |
| **ZIO learning curve** | Medium | High | Allocate week 1 for ZIO fundamentals; use `ZIO.attempt` liberally |
| **Task isolation failures** | Medium | High | Run tasks in separate processes; consider Docker for strict isolation |
| **Database connection exhaustion** | Medium | Medium | Configure HikariCP conservatively (10 connections); add pool metrics |
| **Scope creep** | High | High | Strict phase gates; no cron until Phase 4; no UI |
| **Distributed edge cases** | High | Medium | Invest in chaos testing; extensive logging |
| **Time overrun** | Medium | High | Prioritize core features; cut Python executor if needed |

---

## 9. Testing Strategy

### Test Pyramid

```
                    ┌───────────────┐
                    │   E2E Tests   │  (10%)
                    │  Full system  │
                    └───────┬───────┘
                            │
               ┌────────────┴────────────┐
               │   Integration Tests     │  (30%)
               │  DB, etcd, HTTP client  │
               └────────────┬────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        │           Unit Tests                   │  (60%)
        │  Domain logic, parsing, validation     │
        └────────────────────────────────────────┘
```

### Testing Tools

| Tool | Purpose |
|------|---------|
| ZIO Test | Unit and integration testing with ZIO |
| Testcontainers | PostgreSQL and etcd containers |
| http4s Client | API testing |
| Toxiproxy | Network failure simulation |

### Example Tests

```scala
// Unit test
object DagValidatorSpec extends ZIOSpecDefault {
  def spec = suite("DagValidator")(
    test("detects simple cycle") {
      val tasks = List(
        TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a"),
          dependencies = Set(TaskId("b"))),
        TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
          dependencies = Set(TaskId("a")))
      )
      assertTrue(DagValidator.detectCycle(tasks).isDefined)
    },

    test("accepts valid DAG") {
      val tasks = List(
        TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a")),
        TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
          dependencies = Set(TaskId("a"))),
        TaskDefinition(TaskId("c"), "C", TaskType.Shell("echo c"),
          dependencies = Set(TaskId("a"), TaskId("b")))
      )
      assertTrue(DagValidator.detectCycle(tasks).isEmpty)
    }
  )
}
```

---

## Appendix A: Project Structure

```
distributed-task-scheduler/
├── src/
│   ├── main/
│   │   ├── scala/scheduler/
│   │   │   ├── domain/
│   │   │   │   └── Task.scala
│   │   │   ├── parser/
│   │   │   │   ├── DagParser.scala
│   │   │   │   └── DagValidator.scala
│   │   │   ├── executor/
│   │   │   │   ├── TaskExecutor.scala
│   │   │   │   ├── ShellTaskExecutor.scala
│   │   │   │   └── HttpTaskExecutor.scala
│   │   │   ├── scheduler/
│   │   │   │   ├── SimpleScheduler.scala
│   │   │   │   ├── DistributedScheduler.scala
│   │   │   │   └── TaskRouter.scala
│   │   │   ├── worker/
│   │   │   │   ├── Worker.scala
│   │   │   │   ├── WorkerPool.scala
│   │   │   │   └── WorkStealing.scala
│   │   │   ├── coordination/
│   │   │   │   ├── DistributedCoordinator.scala
│   │   │   │   └── EtcdCoordinator.scala
│   │   │   ├── persistence/
│   │   │   │   ├── DagRepository.scala
│   │   │   │   ├── TaskInstanceRepository.scala
│   │   │   │   └── DlqRepository.scala
│   │   │   ├── dlq/
│   │   │   │   └── DeadLetterQueue.scala
│   │   │   ├── api/
│   │   │   │   ├── Endpoints.scala
│   │   │   │   └── Routes.scala
│   │   │   ├── metrics/
│   │   │   │   └── Metrics.scala
│   │   │   ├── health/
│   │   │   │   └── HealthChecker.scala
│   │   │   ├── lifecycle/
│   │   │   │   └── GracefulShutdown.scala
│   │   │   └── Main.scala
│   │   └── resources/
│   │       ├── application.conf
│   │       └── logback.xml
│   └── test/
│       └── scala/scheduler/
│           ├── domain/TaskSpec.scala
│           ├── parser/DagValidatorSpec.scala
│           └── integration/
│               ├── RepositorySpec.scala
│               └── E2ESpec.scala
├── migrations/
│   ├── V1__initial_schema.sql
│   └── V5__dead_letter_queue.sql
├── docs/
│   └── IMPLEMENTATION_PLAN.md
├── build.sbt
├── project/
│   └── plugins.sbt
└── README.md
```

---

## Appendix B: build.sbt

```scala
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "com.scheduler"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "distributed-task-scheduler",

    libraryDependencies ++= Seq(
      // ZIO
      "dev.zio" %% "zio" % "2.0.19",
      "dev.zio" %% "zio-streams" % "2.0.19",

      // HTTP
      "org.http4s" %% "http4s-ember-server" % "0.23.23",
      "org.http4s" %% "http4s-ember-client" % "0.23.23",
      "org.http4s" %% "http4s-circe" % "0.23.23",
      "org.http4s" %% "http4s-dsl" % "0.23.23",
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server-zio" % "1.9.6",
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % "1.9.6",
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % "1.9.6",

      // Database
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC4",
      "org.flywaydb" % "flyway-core" % "9.22.3",

      // JSON
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",
      "io.circe" %% "circe-yaml" % "0.14.2",

      // etcd
      "io.etcd" % "jetcd-core" % "0.7.7",

      // Metrics
      "io.prometheus" % "simpleclient" % "0.16.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.16.0",

      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.11",

      // Testing
      "dev.zio" %% "zio-test" % "2.0.19" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.19" % Test,
      "dev.zio" %% "zio-test-magnolia" % "2.0.19" % Test,
      "org.testcontainers" % "testcontainers" % "1.19.3" % Test,
      "org.testcontainers" % "postgresql" % "1.19.3" % Test
    ),

    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
```
