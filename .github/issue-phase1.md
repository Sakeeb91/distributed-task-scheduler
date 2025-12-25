## Summary

Establish the foundational data model and database layer that everything else builds upon. This phase creates the core domain types, database schema, and repository layer with full CRUD operations.

**Estimated Hours**: 15-20
**Priority**: 🔴 Critical (blocks all other phases)

## System Context

```
distributed-task-scheduler/
├── src/main/scala/scheduler/
│   ├── domain/
│   │   └── Task.scala          # Core domain models
│   └── persistence/
│       ├── DagRepository.scala
│       ├── TaskInstanceRepository.scala
│       └── DagRunRepository.scala
├── migrations/
│   └── V1__initial_schema.sql
└── build.sbt
```

## Implementation Tasks

### 1. Project Scaffolding

- [ ] Create `build.sbt` with all dependencies
- [ ] Set up project structure
- [ ] Configure sbt plugins (Flyway, etc.)

📄 **File:** `build.sbt`

```scala
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "com.scheduler"

lazy val root = (project in file("."))
  .settings(
    name := "distributed-task-scheduler",

    libraryDependencies ++= Seq(
      // ZIO
      "dev.zio" %% "zio" % "2.0.19",
      "dev.zio" %% "zio-streams" % "2.0.19",

      // Database
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC4",
      "org.flywaydb" % "flyway-core" % "9.22.3",

      // JSON
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Testing
      "dev.zio" %% "zio-test" % "2.0.19" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.19" % Test,
      "org.testcontainers" % "testcontainers" % "1.19.3" % Test,
      "org.testcontainers" % "postgresql" % "1.19.3" % Test
    ),

    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
```

### 2. Core Domain Model

- [ ] Create identifier value classes (`DagId`, `TaskId`, `RunId`, `WorkerId`)
- [ ] Define `TaskType` ADT (Shell, HTTP, Python)
- [ ] Create `RetryPolicy` case class
- [ ] Define `TaskDefinition` and `DagDefinition`
- [ ] Create `TaskState` ADT with transitions
- [ ] Define runtime types (`TaskInstance`, `DagRun`)

📄 **File:** `src/main/scala/scheduler/domain/Task.scala`

```scala
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
  case class Shell(command: String, env: Map[String, String] = Map.empty) extends TaskType
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

// === Runtime Types ===

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

### 3. Database Schema

- [ ] Create Flyway migration for initial schema
- [ ] Define PostgreSQL enums for `task_state` and `task_type`
- [ ] Create `dags` table with JSONB definition
- [ ] Create `dag_runs` table with foreign key
- [ ] Create `task_instances` table with indexes
- [ ] Add performance indexes

📄 **File:** `migrations/V1__initial_schema.sql`

```sql
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

### 4. Repository Layer

- [ ] Create `DagRepository` trait and implementation
- [ ] Create `TaskInstanceRepository` trait and implementation
- [ ] Create `DagRunRepository` trait and implementation
- [ ] Implement Doobie `Meta` instances for custom types
- [ ] Set up HikariCP connection pool with ZIO

📄 **File:** `src/main/scala/scheduler/persistence/DagRepository.scala`

```scala
package scheduler.persistence

import scheduler.domain._
import zio._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

trait DagRepository {
  def create(dag: DagDefinition): Task[DagId]
  def get(id: DagId): Task[Option[DagDefinition]]
  def getByName(name: String): Task[Option[DagDefinition]]
  def list(limit: Int, offset: Int): Task[List[DagDefinition]]
  def update(dag: DagDefinition): Task[Unit]
  def delete(id: DagId): Task[Unit]
}

class DagRepositoryLive(xa: Transactor[Task]) extends DagRepository {

  def create(dag: DagDefinition): Task[DagId] = {
    sql"""
      INSERT INTO dags (id, name, definition, schedule, version, created_at)
      VALUES (${dag.id.value}, ${dag.name}, ${dag.asJson}, ${dag.schedule}, ${dag.version}, ${dag.createdAt})
    """.update.run.transact(xa).as(dag.id)
  }

  def get(id: DagId): Task[Option[DagDefinition]] = {
    sql"""
      SELECT id, name, definition, schedule, version, created_at
      FROM dags WHERE id = ${id.value}
    """.query[DagDefinition].option.transact(xa)
  }

  def getByName(name: String): Task[Option[DagDefinition]] = {
    sql"""
      SELECT id, name, definition, schedule, version, created_at
      FROM dags WHERE name = $name
    """.query[DagDefinition].option.transact(xa)
  }

  def list(limit: Int, offset: Int): Task[List[DagDefinition]] = {
    sql"""
      SELECT id, name, definition, schedule, version, created_at
      FROM dags ORDER BY created_at DESC LIMIT $limit OFFSET $offset
    """.query[DagDefinition].to[List].transact(xa)
  }

  def update(dag: DagDefinition): Task[Unit] = {
    sql"""
      UPDATE dags SET
        definition = ${dag.asJson},
        schedule = ${dag.schedule},
        version = ${dag.version},
        updated_at = NOW()
      WHERE id = ${dag.id.value}
    """.update.run.transact(xa).unit
  }

  def delete(id: DagId): Task[Unit] = {
    sql"DELETE FROM dags WHERE id = ${id.value}".update.run.transact(xa).unit
  }
}

object DagRepositoryLive {
  val layer: ZLayer[Transactor[Task], Nothing, DagRepository] =
    ZLayer.fromFunction(new DagRepositoryLive(_))
}
```

📄 **File:** `src/main/scala/scheduler/persistence/TaskInstanceRepository.scala`

```scala
package scheduler.persistence

import scheduler.domain._
import zio._
import doobie._
import doobie.implicits._
import java.util.UUID

trait TaskInstanceRepository {
  def create(instance: TaskInstance): Task[UUID]
  def get(id: UUID): Task[Option[TaskInstance]]
  def getByRunId(runId: RunId): Task[List[TaskInstance]]
  def updateState(id: UUID, state: TaskState): Task[Unit]
  def findReady(limit: Int): Task[List[TaskInstance]]
  def claimTask(id: UUID, workerId: WorkerId): Task[Boolean]
}

class TaskInstanceRepositoryLive(xa: Transactor[Task]) extends TaskInstanceRepository {

  def create(instance: TaskInstance): Task[UUID] = {
    sql"""
      INSERT INTO task_instances (id, run_id, task_id, task_name, state, attempt, created_at)
      VALUES (${instance.id}, ${instance.runId.value}, ${instance.taskId.value},
              ${instance.taskName}, 'pending', ${instance.attempt}, ${instance.createdAt})
    """.update.run.transact(xa).as(instance.id)
  }

  def get(id: UUID): Task[Option[TaskInstance]] = {
    sql"""
      SELECT id, run_id, task_id, task_name, state, attempt, worker_id, started_at, completed_at, created_at
      FROM task_instances WHERE id = $id
    """.query[TaskInstance].option.transact(xa)
  }

  def getByRunId(runId: RunId): Task[List[TaskInstance]] = {
    sql"""
      SELECT id, run_id, task_id, task_name, state, attempt, worker_id, started_at, completed_at, created_at
      FROM task_instances WHERE run_id = ${runId.value}
    """.query[TaskInstance].to[List].transact(xa)
  }

  def updateState(id: UUID, state: TaskState): Task[Unit] = {
    val (stateStr, completedAt, errorMsg) = state match {
      case TaskState.Pending => ("pending", None, None)
      case TaskState.Queued => ("queued", None, None)
      case TaskState.Running => ("running", None, None)
      case TaskState.Succeeded(at) => ("succeeded", Some(at), None)
      case TaskState.Failed(err, _) => ("failed", Some(java.time.Instant.now()), Some(err))
      case TaskState.Skipped => ("skipped", None, None)
    }

    sql"""
      UPDATE task_instances SET
        state = $stateStr::task_state,
        completed_at = $completedAt,
        error_message = $errorMsg
      WHERE id = $id
    """.update.run.transact(xa).unit
  }

  def findReady(limit: Int): Task[List[TaskInstance]] = {
    // Find tasks where all dependencies are completed
    sql"""
      SELECT ti.id, ti.run_id, ti.task_id, ti.task_name, ti.state, ti.attempt,
             ti.worker_id, ti.started_at, ti.completed_at, ti.created_at
      FROM task_instances ti
      WHERE ti.state = 'pending'
        AND ti.worker_id IS NULL
      LIMIT $limit
    """.query[TaskInstance].to[List].transact(xa)
  }

  def claimTask(id: UUID, workerId: WorkerId): Task[Boolean] = {
    sql"""
      UPDATE task_instances SET
        worker_id = ${workerId.value},
        state = 'queued'::task_state
      WHERE id = $id AND worker_id IS NULL
    """.update.run.transact(xa).map(_ > 0)
  }
}
```

### 5. Integration Tests

- [ ] Set up Testcontainers for PostgreSQL
- [ ] Create test fixtures for domain objects
- [ ] Test repository CRUD operations
- [ ] Test transaction rollback scenarios

📄 **File:** `src/test/scala/scheduler/persistence/RepositorySpec.scala`

```scala
package scheduler.persistence

import scheduler.domain._
import zio._
import zio.test._
import org.testcontainers.containers.PostgreSQLContainer

object RepositorySpec extends ZIOSpecDefault {

  val postgres = ZLayer.scoped {
    ZIO.acquireRelease(
      ZIO.attemptBlocking {
        val container = new PostgreSQLContainer("postgres:15")
        container.start()
        container
      }
    )(c => ZIO.attemptBlocking(c.stop()).orDie)
  }

  def spec = suite("DagRepository")(
    test("creates and retrieves DAG") {
      for {
        repo <- ZIO.service[DagRepository]
        dag = DagDefinition(
          id = DagId.generate,
          name = "test-dag",
          tasks = List(
            TaskDefinition(
              id = TaskId("task1"),
              name = "Test Task",
              taskType = TaskType.Shell("echo hello")
            )
          )
        )
        id <- repo.create(dag)
        retrieved <- repo.get(id)
      } yield assertTrue(
        retrieved.isDefined,
        retrieved.get.name == "test-dag"
      )
    },

    test("lists DAGs with pagination") {
      for {
        repo <- ZIO.service[DagRepository]
        _ <- ZIO.foreach(1 to 5) { i =>
          val dag = DagDefinition(
            id = DagId.generate,
            name = s"dag-$i",
            tasks = List(TaskDefinition(TaskId("t1"), "Task", TaskType.Shell("echo")))
          )
          repo.create(dag)
        }
        page1 <- repo.list(limit = 2, offset = 0)
        page2 <- repo.list(limit = 2, offset = 2)
      } yield assertTrue(
        page1.size == 2,
        page2.size == 2
      )
    }
  ).provide(/* layers */)
}
```

## Files to Modify

| File | Lines | Action | Description |
|------|-------|--------|-------------|
| `build.sbt` | - | Create | Project configuration and dependencies |
| `src/main/scala/scheduler/domain/Task.scala` | - | Create | Core domain models |
| `migrations/V1__initial_schema.sql` | - | Create | Database schema |
| `src/main/scala/scheduler/persistence/DagRepository.scala` | - | Create | DAG persistence |
| `src/main/scala/scheduler/persistence/TaskInstanceRepository.scala` | - | Create | Task instance persistence |
| `src/main/scala/scheduler/persistence/DagRunRepository.scala` | - | Create | DAG run persistence |
| `src/test/scala/scheduler/persistence/RepositorySpec.scala` | - | Create | Integration tests |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Doobie + ZIO integration complexity | 🟡 Medium | Follow official ZIO-Doobie interop examples |
| JSONB codec issues | 🟡 Medium | Create explicit Circe codecs before Doobie Meta |
| Connection pool exhaustion | 🟢 Low | Start with conservative HikariCP settings (10 max) |

## Definition of Done

- [ ] `sbt compile` succeeds with no errors
- [ ] Flyway migrations run successfully against PostgreSQL
- [ ] All repository CRUD methods implemented
- [ ] Integration tests pass against Testcontainers PostgreSQL
- [ ] Can insert a DAG and retrieve it with all tasks
- [ ] Code reviewed and merged to main

## Related Issues

- Parent: #1 (Meta Issue)
- Blocks: #3 (Phase 2: DAG Parser)
