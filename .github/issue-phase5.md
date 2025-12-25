## Summary

Add production-ready features including Dead Letter Queue for failed tasks, Prometheus metrics, health checks, and graceful shutdown. This phase transforms the scheduler from functional to production-quality.

**Estimated Hours**: 25-30
**Priority**: 🟡 High (production readiness)
**Depends on**: #5 (Phase 4: Distributed Coordination)

## System Context

```
distributed-task-scheduler/
├── src/main/scala/scheduler/
│   ├── dlq/
│   │   ├── DeadLetterQueue.scala      # DLQ service
│   │   └── DlqRepository.scala        # DLQ persistence
│   ├── metrics/
│   │   └── Metrics.scala              # Prometheus metrics
│   ├── health/
│   │   └── HealthChecker.scala        # Health check service
│   └── lifecycle/
│       └── GracefulShutdown.scala     # Shutdown handling
├── migrations/
│   └── V5__dead_letter_queue.sql
```

## Implementation Tasks

### 1. Dead Letter Queue Schema

- [ ] Create DLQ table with full error context
- [ ] Add resolution status tracking
- [ ] Create indexes for efficient queries

📄 **File:** `migrations/V5__dead_letter_queue.sql`

```sql
CREATE TABLE dead_letter_queue (
  id UUID PRIMARY KEY,
  task_instance_id UUID NOT NULL REFERENCES task_instances(id),
  run_id UUID NOT NULL REFERENCES dag_runs(id),
  dag_id UUID NOT NULL REFERENCES dags(id),
  task_name VARCHAR(255) NOT NULL,

  -- Error information
  error_type VARCHAR(100) NOT NULL,
  error_message TEXT NOT NULL,
  stack_trace TEXT,
  last_attempt_at TIMESTAMPTZ NOT NULL,
  total_attempts INT NOT NULL,

  -- Task context for debugging
  task_config JSONB NOT NULL,
  execution_context JSONB,

  -- Resolution tracking
  resolution_status VARCHAR(50) NOT NULL DEFAULT 'pending',
  resolved_at TIMESTAMPTZ,
  resolved_by VARCHAR(255),
  resolution_notes TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Constraints
ALTER TABLE dead_letter_queue
  ADD CONSTRAINT check_resolution_status
  CHECK (resolution_status IN ('pending', 'retried', 'skipped', 'resolved'));

-- Indexes
CREATE INDEX idx_dlq_status ON dead_letter_queue(resolution_status);
CREATE INDEX idx_dlq_dag ON dead_letter_queue(dag_id);
CREATE INDEX idx_dlq_created ON dead_letter_queue(created_at DESC);
CREATE INDEX idx_dlq_task_instance ON dead_letter_queue(task_instance_id);
```

### 2. Dead Letter Queue Implementation

- [ ] Create DLQ service interface and implementation
- [ ] Implement automatic DLQ routing for exhausted retries
- [ ] Add retry, skip, and resolve operations
- [ ] Create API endpoints for DLQ management

📄 **File:** `src/main/scala/scheduler/dlq/DeadLetterQueue.scala`

```scala
package scheduler.dlq

import scheduler.domain._
import zio._
import java.util.UUID
import java.time.Instant

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
  taskConfig: io.circe.Json,
  resolutionStatus: String,
  resolvedAt: Option[Instant],
  resolvedBy: Option[String],
  resolutionNotes: Option[String],
  createdAt: Instant
)

case class TaskFailure(
  taskInstance: TaskInstance,
  taskDefinition: TaskDefinition,
  error: Throwable,
  executionContext: Option[io.circe.Json],
  isRetryExhausted: Boolean
)

trait DeadLetterQueue {
  /**
   * Add a failed task to the DLQ.
   */
  def enqueue(failure: TaskFailure): Task[UUID]

  /**
   * List pending DLQ items with pagination.
   */
  def listPending(limit: Int, offset: Int): Task[List[DeadLetter]]

  /**
   * List DLQ items by DAG with pagination.
   */
  def listByDag(dagId: DagId, limit: Int, offset: Int): Task[List[DeadLetter]]

  /**
   * Get a specific DLQ item.
   */
  def get(id: UUID): Task[Option[DeadLetter]]

  /**
   * Retry a failed task, creating a new run.
   */
  def retry(id: UUID): Task[RunId]

  /**
   * Skip the failed task and continue the DAG.
   */
  def skip(id: UUID, reason: String): Task[Unit]

  /**
   * Mark as resolved after manual intervention.
   */
  def resolve(id: UUID, notes: String, resolvedBy: String): Task[Unit]

  /**
   * Get DLQ statistics.
   */
  def getStats: Task[DlqStats]
}

case class DlqStats(
  pendingCount: Int,
  retriedCount: Int,
  skippedCount: Int,
  resolvedCount: Int,
  oldestPendingAge: Option[java.time.Duration]
)

class DeadLetterQueueImpl(
  repo: DlqRepository,
  scheduler: DistributedScheduler,
  metrics: Metrics
) extends DeadLetterQueue {

  def enqueue(failure: TaskFailure): Task[UUID] = {
    val id = UUID.randomUUID()
    val entry = DeadLetterEntry(
      id = id,
      taskInstanceId = failure.taskInstance.id,
      runId = failure.taskInstance.runId,
      dagId = failure.taskInstance.dagId,
      taskName = failure.taskInstance.taskName,
      errorType = failure.error.getClass.getSimpleName,
      errorMessage = failure.error.getMessage,
      stackTrace = Some(getStackTrace(failure.error)),
      lastAttemptAt = Instant.now(),
      totalAttempts = failure.taskInstance.attempt,
      taskConfig = failure.taskDefinition.asJson,
      executionContext = failure.executionContext
    )

    for {
      _ <- repo.insert(entry)
      _ <- metrics.dlqEnqueued.increment
      _ <- metrics.dlqSize.increment
      _ <- ZIO.logWarning(
        s"Task '${entry.taskName}' moved to DLQ after ${entry.totalAttempts} attempts: ${entry.errorMessage}"
      )
    } yield id
  }

  def listPending(limit: Int, offset: Int): Task[List[DeadLetter]] =
    repo.findByStatus("pending", limit, offset)

  def listByDag(dagId: DagId, limit: Int, offset: Int): Task[List[DeadLetter]] =
    repo.findByDag(dagId, limit, offset)

  def get(id: UUID): Task[Option[DeadLetter]] =
    repo.get(id)

  def retry(id: UUID): Task[RunId] = {
    for {
      entry <- repo.get(id).someOrFail(DlqError.NotFound(id))

      // Create new run for just this task
      runId <- scheduler.retryTask(entry.taskInstanceId)

      _ <- repo.updateStatus(id, "retried", None, None)
      _ <- metrics.dlqRetried.increment
      _ <- metrics.dlqSize.decrement

      _ <- ZIO.logInfo(s"Retried DLQ item ${id}, created run ${runId.value}")
    } yield runId
  }

  def skip(id: UUID, reason: String): Task[Unit] = {
    for {
      entry <- repo.get(id).someOrFail(DlqError.NotFound(id))
      _ <- repo.updateStatus(id, "skipped", Some(reason), None)
      _ <- metrics.dlqSize.decrement
      _ <- ZIO.logInfo(s"Skipped DLQ item ${id}: $reason")
    } yield ()
  }

  def resolve(id: UUID, notes: String, resolvedBy: String): Task[Unit] = {
    for {
      entry <- repo.get(id).someOrFail(DlqError.NotFound(id))
      _ <- repo.updateStatus(id, "resolved", Some(notes), Some(resolvedBy))
      _ <- metrics.dlqSize.decrement
      _ <- ZIO.logInfo(s"Resolved DLQ item ${id} by $resolvedBy")
    } yield ()
  }

  def getStats: Task[DlqStats] = repo.getStats

  private def getStackTrace(error: Throwable): String = {
    val sw = new java.io.StringWriter()
    error.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString.take(10000) // Limit size
  }
}

sealed trait DlqError extends Exception
object DlqError {
  case class NotFound(id: UUID) extends DlqError {
    override def getMessage = s"DLQ entry not found: $id"
  }
}
```

📄 **File:** `src/main/scala/scheduler/dlq/DlqRepository.scala`

```scala
package scheduler.dlq

import scheduler.domain._
import zio._
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import java.util.UUID
import java.time.Instant

case class DeadLetterEntry(
  id: UUID,
  taskInstanceId: UUID,
  runId: RunId,
  dagId: DagId,
  taskName: String,
  errorType: String,
  errorMessage: String,
  stackTrace: Option[String],
  lastAttemptAt: Instant,
  totalAttempts: Int,
  taskConfig: io.circe.Json,
  executionContext: Option[io.circe.Json]
)

trait DlqRepository {
  def insert(entry: DeadLetterEntry): Task[Unit]
  def get(id: UUID): Task[Option[DeadLetter]]
  def findByStatus(status: String, limit: Int, offset: Int): Task[List[DeadLetter]]
  def findByDag(dagId: DagId, limit: Int, offset: Int): Task[List[DeadLetter]]
  def updateStatus(
    id: UUID,
    status: String,
    notes: Option[String],
    resolvedBy: Option[String]
  ): Task[Unit]
  def getStats: Task[DlqStats]
}

class DlqRepositoryLive(xa: Transactor[Task]) extends DlqRepository {

  def insert(entry: DeadLetterEntry): Task[Unit] = {
    sql"""
      INSERT INTO dead_letter_queue (
        id, task_instance_id, run_id, dag_id, task_name,
        error_type, error_message, stack_trace, last_attempt_at,
        total_attempts, task_config, execution_context
      ) VALUES (
        ${entry.id}, ${entry.taskInstanceId}, ${entry.runId.value},
        ${entry.dagId.value}, ${entry.taskName}, ${entry.errorType},
        ${entry.errorMessage}, ${entry.stackTrace}, ${entry.lastAttemptAt},
        ${entry.totalAttempts}, ${entry.taskConfig}, ${entry.executionContext}
      )
    """.update.run.transact(xa).unit
  }

  def get(id: UUID): Task[Option[DeadLetter]] = {
    sql"""
      SELECT id, task_instance_id, run_id, dag_id, task_name,
             error_type, error_message, stack_trace, total_attempts,
             task_config, resolution_status, resolved_at, resolved_by,
             resolution_notes, created_at
      FROM dead_letter_queue WHERE id = $id
    """.query[DeadLetter].option.transact(xa)
  }

  def findByStatus(status: String, limit: Int, offset: Int): Task[List[DeadLetter]] = {
    sql"""
      SELECT id, task_instance_id, run_id, dag_id, task_name,
             error_type, error_message, stack_trace, total_attempts,
             task_config, resolution_status, resolved_at, resolved_by,
             resolution_notes, created_at
      FROM dead_letter_queue
      WHERE resolution_status = $status
      ORDER BY created_at DESC
      LIMIT $limit OFFSET $offset
    """.query[DeadLetter].to[List].transact(xa)
  }

  def findByDag(dagId: DagId, limit: Int, offset: Int): Task[List[DeadLetter]] = {
    sql"""
      SELECT id, task_instance_id, run_id, dag_id, task_name,
             error_type, error_message, stack_trace, total_attempts,
             task_config, resolution_status, resolved_at, resolved_by,
             resolution_notes, created_at
      FROM dead_letter_queue
      WHERE dag_id = ${dagId.value}
      ORDER BY created_at DESC
      LIMIT $limit OFFSET $offset
    """.query[DeadLetter].to[List].transact(xa)
  }

  def updateStatus(
    id: UUID,
    status: String,
    notes: Option[String],
    resolvedBy: Option[String]
  ): Task[Unit] = {
    sql"""
      UPDATE dead_letter_queue SET
        resolution_status = $status,
        resolution_notes = $notes,
        resolved_by = $resolvedBy,
        resolved_at = ${if (status != "pending") Some(Instant.now()) else None}
      WHERE id = $id
    """.update.run.transact(xa).unit
  }

  def getStats: Task[DlqStats] = {
    sql"""
      SELECT
        COUNT(*) FILTER (WHERE resolution_status = 'pending') as pending,
        COUNT(*) FILTER (WHERE resolution_status = 'retried') as retried,
        COUNT(*) FILTER (WHERE resolution_status = 'skipped') as skipped,
        COUNT(*) FILTER (WHERE resolution_status = 'resolved') as resolved,
        MIN(created_at) FILTER (WHERE resolution_status = 'pending') as oldest
      FROM dead_letter_queue
    """.query[(Int, Int, Int, Int, Option[Instant])].unique.transact(xa).map {
      case (pending, retried, skipped, resolved, oldest) =>
        DlqStats(
          pendingCount = pending,
          retriedCount = retried,
          skippedCount = skipped,
          resolvedCount = resolved,
          oldestPendingAge = oldest.map(o =>
            java.time.Duration.between(o, Instant.now())
          )
        )
    }
  }
}
```

### 3. Prometheus Metrics

- [ ] Define all metric types (counters, gauges, histograms)
- [ ] Add instrumentation to key code paths
- [ ] Create metrics endpoint

📄 **File:** `src/main/scala/scheduler/metrics/Metrics.scala`

```scala
package scheduler.metrics

import io.prometheus.client._
import zio._

trait Metrics {
  // Task metrics
  def tasksProcessed: MetricCounter
  def taskDuration: MetricHistogram
  def taskErrors: MetricCounter

  // Queue metrics
  def queueDepth: MetricGauge
  def tasksEnqueued: MetricCounter
  def tasksDequeued: MetricCounter

  // DLQ metrics
  def dlqSize: MetricGauge
  def dlqEnqueued: MetricCounter
  def dlqRetried: MetricCounter

  // Worker metrics
  def activeWorkers: MetricGauge
  def workerUtilization: MetricGauge

  // Scheduler metrics
  def isLeader: MetricGauge
  def leaderElections: MetricCounter
  def schedulerIterations: MetricCounter

  // Database metrics
  def dbConnections: MetricGauge
  def dbQueryDuration: MetricHistogram
}

trait MetricCounter {
  def increment: UIO[Unit]
  def increment(amount: Double): UIO[Unit]
  def labels(labelValues: String*): MetricCounter
}

trait MetricGauge {
  def set(value: Double): UIO[Unit]
  def increment: UIO[Unit]
  def decrement: UIO[Unit]
  def labels(labelValues: String*): MetricGauge
}

trait MetricHistogram {
  def observe(value: Double): UIO[Unit]
  def labels(labelValues: String*): MetricHistogram
  def time[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A]
}

class PrometheusMetrics extends Metrics {

  private val registry = new CollectorRegistry()

  // Task metrics
  private val tasksProcessedCounter = Counter.build()
    .name("scheduler_tasks_processed_total")
    .help("Total number of tasks processed")
    .labelNames("status", "task_type")
    .register(registry)

  private val taskDurationHistogram = Histogram.build()
    .name("scheduler_task_duration_seconds")
    .help("Task execution duration in seconds")
    .labelNames("task_type")
    .buckets(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0)
    .register(registry)

  private val taskErrorsCounter = Counter.build()
    .name("scheduler_task_errors_total")
    .help("Total number of task execution errors")
    .labelNames("task_type", "error_type")
    .register(registry)

  // Queue metrics
  private val queueDepthGauge = Gauge.build()
    .name("scheduler_queue_depth")
    .help("Current number of tasks in queue")
    .labelNames("worker_id")
    .register(registry)

  // DLQ metrics
  private val dlqSizeGauge = Gauge.build()
    .name("scheduler_dlq_size")
    .help("Current number of items in dead letter queue")
    .register(registry)

  private val dlqEnqueuedCounter = Counter.build()
    .name("scheduler_dlq_enqueued_total")
    .help("Total items added to dead letter queue")
    .register(registry)

  private val dlqRetriedCounter = Counter.build()
    .name("scheduler_dlq_retried_total")
    .help("Total items retried from dead letter queue")
    .register(registry)

  // Worker metrics
  private val activeWorkersGauge = Gauge.build()
    .name("scheduler_active_workers")
    .help("Number of active workers")
    .register(registry)

  private val workerUtilizationGauge = Gauge.build()
    .name("scheduler_worker_utilization")
    .help("Worker utilization (0-1)")
    .labelNames("worker_id")
    .register(registry)

  // Scheduler metrics
  private val isLeaderGauge = Gauge.build()
    .name("scheduler_is_leader")
    .help("Whether this instance is the cluster leader (1 or 0)")
    .register(registry)

  private val leaderElectionsCounter = Counter.build()
    .name("scheduler_leader_elections_total")
    .help("Total number of leader elections")
    .register(registry)

  // Implementations
  val tasksProcessed = wrapCounter(tasksProcessedCounter)
  val taskDuration = wrapHistogram(taskDurationHistogram)
  val taskErrors = wrapCounter(taskErrorsCounter)
  val queueDepth = wrapGauge(queueDepthGauge)
  val tasksEnqueued = wrapCounter(Counter.build().name("scheduler_tasks_enqueued_total").help("").register(registry))
  val tasksDequeued = wrapCounter(Counter.build().name("scheduler_tasks_dequeued_total").help("").register(registry))
  val dlqSize = wrapGauge(dlqSizeGauge)
  val dlqEnqueued = wrapCounter(dlqEnqueuedCounter)
  val dlqRetried = wrapCounter(dlqRetriedCounter)
  val activeWorkers = wrapGauge(activeWorkersGauge)
  val workerUtilization = wrapGauge(workerUtilizationGauge)
  val isLeader = wrapGauge(isLeaderGauge)
  val leaderElections = wrapCounter(leaderElectionsCounter)
  val schedulerIterations = wrapCounter(Counter.build().name("scheduler_iterations_total").help("").register(registry))
  val dbConnections = wrapGauge(Gauge.build().name("scheduler_db_connections").help("").register(registry))
  val dbQueryDuration = wrapHistogram(Histogram.build().name("scheduler_db_query_duration_seconds").help("").register(registry))

  def getRegistry: CollectorRegistry = registry

  private def wrapCounter(counter: Counter): MetricCounter = new MetricCounter {
    def increment: UIO[Unit] = ZIO.succeed(counter.inc())
    def increment(amount: Double): UIO[Unit] = ZIO.succeed(counter.inc(amount))
    def labels(labelValues: String*): MetricCounter = wrapCounter(counter.labels(labelValues: _*))
  }

  private def wrapGauge(gauge: Gauge): MetricGauge = new MetricGauge {
    def set(value: Double): UIO[Unit] = ZIO.succeed(gauge.set(value))
    def increment: UIO[Unit] = ZIO.succeed(gauge.inc())
    def decrement: UIO[Unit] = ZIO.succeed(gauge.dec())
    def labels(labelValues: String*): MetricGauge = wrapGauge(gauge.labels(labelValues: _*))
  }

  private def wrapHistogram(histogram: Histogram): MetricHistogram = new MetricHistogram {
    def observe(value: Double): UIO[Unit] = ZIO.succeed(histogram.observe(value))
    def labels(labelValues: String*): MetricHistogram = wrapHistogram(histogram.labels(labelValues: _*))
    def time[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] = {
      for {
        start <- Clock.nanoTime
        result <- zio
        end <- Clock.nanoTime
        _ <- observe((end - start) / 1e9)
      } yield result
    }
  }
}

object PrometheusMetrics {
  val layer: ULayer[Metrics] = ZLayer.succeed(new PrometheusMetrics)
}
```

### 4. Health Checks

- [ ] Implement health check for each component
- [ ] Create aggregated health endpoint
- [ ] Add readiness and liveness probes

📄 **File:** `src/main/scala/scheduler/health/HealthChecker.scala`

```scala
package scheduler.health

import scheduler.coordination._
import scheduler.worker._
import zio._
import doobie.util.transactor.Transactor
import java.time.Instant

case class HealthStatus(
  status: String,
  timestamp: Instant,
  components: Map[String, ComponentHealth]
)

case class ComponentHealth(
  status: String,
  latencyMs: Option[Long],
  message: Option[String],
  details: Map[String, String] = Map.empty
)

trait HealthChecker {
  def check: Task[HealthStatus]
  def isHealthy: Task[Boolean]
  def isReady: Task[Boolean]
}

class HealthCheckerImpl(
  db: Transactor[Task],
  coordinator: DistributedCoordinator,
  workerPool: WorkerPoolManager
) extends HealthChecker {

  def check: Task[HealthStatus] = {
    for {
      timestamp <- Clock.instant
      dbHealth <- checkDatabase
      etcdHealth <- checkEtcd
      workersHealth <- checkWorkers

      components = Map(
        "database" -> dbHealth,
        "etcd" -> etcdHealth,
        "workers" -> workersHealth
      )

      overallStatus = determineOverallStatus(components.values.toList)
    } yield HealthStatus(overallStatus, timestamp, components)
  }

  def isHealthy: Task[Boolean] = {
    check.map(_.status == "healthy")
  }

  def isReady: Task[Boolean] = {
    // Ready = can serve requests (may be degraded but functional)
    check.map(status => status.status != "unhealthy")
  }

  private def checkDatabase: Task[ComponentHealth] = {
    val startTime = System.currentTimeMillis()

    // Simple query to verify connection
    import doobie.implicits._
    sql"SELECT 1".query[Int].unique.transact(db)
      .map { _ =>
        val latency = System.currentTimeMillis() - startTime
        ComponentHealth(
          status = if (latency < 1000) "healthy" else "degraded",
          latencyMs = Some(latency),
          message = None,
          details = Map("connectionPool" -> "active")
        )
      }
      .timeout(5.seconds)
      .someOrElse(ComponentHealth(
        status = "unhealthy",
        latencyMs = None,
        message = Some("Database query timeout")
      ))
      .catchAll { error =>
        ZIO.succeed(ComponentHealth(
          status = "unhealthy",
          latencyMs = None,
          message = Some(s"Database error: ${error.getMessage}")
        ))
      }
  }

  private def checkEtcd: Task[ComponentHealth] = {
    val startTime = System.currentTimeMillis()

    coordinator.getClusterState
      .map { state =>
        val latency = System.currentTimeMillis() - startTime
        ComponentHealth(
          status = "healthy",
          latencyMs = Some(latency),
          message = None,
          details = Map(
            "leader" -> state.leaderId.getOrElse("none"),
            "workers" -> state.workers.size.toString
          )
        )
      }
      .timeout(5.seconds)
      .someOrElse(ComponentHealth(
        status = "unhealthy",
        latencyMs = None,
        message = Some("etcd query timeout")
      ))
      .catchAll { error =>
        ZIO.succeed(ComponentHealth(
          status = "unhealthy",
          latencyMs = None,
          message = Some(s"etcd error: ${error.getMessage}")
        ))
      }
  }

  private def checkWorkers: Task[ComponentHealth] = {
    workerPool.getWorkerStatuses.map { statuses =>
      val activeCount = statuses.count { case (_, status, _) =>
        status match {
          case WorkerStatus.Idle | WorkerStatus.Running(_) => true
          case _ => false
        }
      }

      val status = if (activeCount > 0) "healthy"
        else if (statuses.nonEmpty) "degraded"
        else "unhealthy"

      ComponentHealth(
        status = status,
        latencyMs = None,
        message = if (activeCount == 0) Some("No active workers") else None,
        details = Map(
          "totalWorkers" -> statuses.size.toString,
          "activeWorkers" -> activeCount.toString
        )
      )
    }
  }

  private def determineOverallStatus(components: List[ComponentHealth]): String = {
    if (components.forall(_.status == "healthy")) "healthy"
    else if (components.exists(_.status == "unhealthy")) "unhealthy"
    else "degraded"
  }
}

object HealthCheckerImpl {
  val layer: ZLayer[
    Transactor[Task] with DistributedCoordinator with WorkerPoolManager,
    Nothing,
    HealthChecker
  ] = ZLayer.fromFunction(new HealthCheckerImpl(_, _, _))
}
```

### 5. Graceful Shutdown

- [ ] Handle SIGTERM and SIGINT signals
- [ ] Drain workers before shutdown
- [ ] Release cluster resources (leadership, locks)

📄 **File:** `src/main/scala/scheduler/lifecycle/GracefulShutdown.scala`

```scala
package scheduler.lifecycle

import scheduler.coordination._
import scheduler.worker._
import zio._
import java.util.concurrent.atomic.AtomicBoolean

class GracefulShutdown(
  coordinator: DistributedCoordinator,
  workerPool: WorkerPoolManager,
  leaderLease: Ref[Option[LeadershipLease]],
  shutdownTimeout: zio.Duration = 60.seconds
) {

  private val isShuttingDown = new AtomicBoolean(false)

  def registerShutdownHook: Task[Unit] = {
    ZIO.attempt {
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          if (isShuttingDown.compareAndSet(false, true)) {
            // Run shutdown in a blocking manner for the JVM hook
            zio.Unsafe.unsafe { implicit unsafe =>
              zio.Runtime.default.unsafe.run(shutdown).getOrThrowFiberFailure()
            }
          }
        }
      })
    }
  }

  def shutdown: Task[Unit] = {
    if (!isShuttingDown.compareAndSet(false, true)) {
      ZIO.logInfo("Shutdown already in progress")
    } else {
      for {
        _ <- ZIO.logInfo("Initiating graceful shutdown...")
        startTime <- Clock.instant

        // Phase 1: Stop accepting new work
        _ <- ZIO.logInfo("Phase 1: Releasing leadership...")
        _ <- releaseLeadership

        // Phase 2: Drain workers
        _ <- ZIO.logInfo("Phase 2: Draining workers...")
        _ <- workerPool.shutdown
          .timeout(shutdownTimeout)
          .someOrElse(ZIO.logWarning("Worker drain timeout - forcing shutdown"))

        // Phase 3: Final cleanup
        _ <- ZIO.logInfo("Phase 3: Final cleanup...")

        endTime <- Clock.instant
        duration = java.time.Duration.between(startTime, endTime)
        _ <- ZIO.logInfo(s"Graceful shutdown complete in ${duration.toMillis}ms")
      } yield ()
    }
  }

  private def releaseLeadership: Task[Unit] = {
    leaderLease.get.flatMap {
      case Some(lease) =>
        lease.release
          .catchAll(e => ZIO.logError(s"Error releasing leadership: ${e.getMessage}"))
      case None =>
        ZIO.unit
    }
  }

  def isShutdownInProgress: UIO[Boolean] =
    ZIO.succeed(isShuttingDown.get())
}

object GracefulShutdown {
  def layer(timeout: zio.Duration): ZLayer[
    DistributedCoordinator with WorkerPoolManager with Ref[Option[LeadershipLease]],
    Nothing,
    GracefulShutdown
  ] = ZLayer.fromFunction(new GracefulShutdown(_, _, _, timeout))
}
```

### 6. DLQ API Endpoints

```scala
// Add to Endpoints.scala

val listDlq = baseEndpoint.get
  .in("api" / "v1" / "dlq")
  .in(query[Option[String]]("status"))
  .in(query[Option[UUID]]("dagId"))
  .in(query[Int]("limit").default(20))
  .in(query[Int]("offset").default(0))
  .out(jsonBody[List[DeadLetterResponse]])
  .summary("List dead letter queue items")

val getDlqItem = baseEndpoint.get
  .in("api" / "v1" / "dlq" / path[UUID]("id"))
  .out(jsonBody[DeadLetterResponse])
  .summary("Get DLQ item details")

val retryDlqItem = baseEndpoint.post
  .in("api" / "v1" / "dlq" / path[UUID]("id") / "retry")
  .out(jsonBody[RunResponse])
  .summary("Retry a failed task")

val skipDlqItem = baseEndpoint.post
  .in("api" / "v1" / "dlq" / path[UUID]("id") / "skip")
  .in(jsonBody[SkipRequest])
  .out(statusCode(sttp.model.StatusCode.NoContent))
  .summary("Skip a failed task")

val resolveDlqItem = baseEndpoint.post
  .in("api" / "v1" / "dlq" / path[UUID]("id") / "resolve")
  .in(jsonBody[ResolveRequest])
  .out(statusCode(sttp.model.StatusCode.NoContent))
  .summary("Mark DLQ item as resolved")

val getDlqStats = baseEndpoint.get
  .in("api" / "v1" / "dlq" / "stats")
  .out(jsonBody[DlqStatsResponse])
  .summary("Get DLQ statistics")

// Health endpoints
val health = endpoint.get
  .in("health")
  .out(jsonBody[HealthStatus])
  .summary("Health check")

val ready = endpoint.get
  .in("ready")
  .out(statusCode(sttp.model.StatusCode.Ok))
  .errorOut(statusCode(sttp.model.StatusCode.ServiceUnavailable))
  .summary("Readiness probe")

val live = endpoint.get
  .in("live")
  .out(statusCode(sttp.model.StatusCode.Ok))
  .summary("Liveness probe")

// Metrics endpoint
val metrics = endpoint.get
  .in("metrics")
  .out(stringBody)
  .summary("Prometheus metrics")
```

## Files to Modify

| File | Lines | Action | Description |
|------|-------|--------|-------------|
| `migrations/V5__dead_letter_queue.sql` | - | Create | DLQ schema |
| `src/main/scala/scheduler/dlq/DeadLetterQueue.scala` | - | Create | DLQ service |
| `src/main/scala/scheduler/dlq/DlqRepository.scala` | - | Create | DLQ persistence |
| `src/main/scala/scheduler/metrics/Metrics.scala` | - | Create | Prometheus metrics |
| `src/main/scala/scheduler/health/HealthChecker.scala` | - | Create | Health checks |
| `src/main/scala/scheduler/lifecycle/GracefulShutdown.scala` | - | Create | Shutdown handling |
| `src/main/scala/scheduler/api/Endpoints.scala` | - | Modify | Add DLQ, health, metrics endpoints |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Metric cardinality explosion | 🟡 Medium | Limit label values, use bounded histograms |
| DLQ table growth | 🟢 Low | Add cleanup job for old resolved items |
| Shutdown timeout too short | 🟡 Medium | Make configurable, log warnings |
| Health check false positives | 🟢 Low | Use appropriate timeouts, test edge cases |

## Definition of Done

- [ ] Failed tasks (retry exhausted) appear in DLQ
- [ ] DLQ items contain full error context and task config
- [ ] Can retry/skip/resolve DLQ items via API
- [ ] Prometheus metrics exposed at `/metrics`
- [ ] All key operations instrumented with metrics
- [ ] `/health` returns component status
- [ ] `/ready` and `/live` work for k8s probes
- [ ] SIGTERM triggers graceful shutdown
- [ ] No tasks lost during graceful shutdown
- [ ] Grafana dashboard template created
- [ ] Alert rules documented

## Related Issues

- Depends on: #5 (Phase 4)
- Parent: #1 (Meta Issue)
- Final phase - no blockers
