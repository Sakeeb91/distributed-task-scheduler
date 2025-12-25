## Summary

Expose the scheduler via REST API with Tapir and implement concurrent worker pool for parallel task execution. This phase enables external access and improves throughput through parallelization.

**Estimated Hours**: 25-30
**Priority**: 🔴 Critical (enables external access)
**Depends on**: #3 (Phase 2: DAG Parser + Executor)

## System Context

```
distributed-task-scheduler/
├── src/main/scala/scheduler/
│   ├── api/
│   │   ├── Endpoints.scala      # Tapir endpoint definitions
│   │   ├── Routes.scala         # http4s route handlers
│   │   └── ApiServer.scala      # Server setup
│   ├── worker/
│   │   ├── Worker.scala         # Single worker implementation
│   │   ├── WorkerPool.scala     # Worker pool management
│   │   └── TaskAssignment.scala # Task distribution
│   └── scheduler/
│       └── TaskRouter.scala     # Task routing logic
```

## Implementation Tasks

### 1. API Endpoint Definitions (Tapir)

- [ ] Define DAG management endpoints (create, get, list, delete)
- [ ] Define execution endpoints (trigger, status)
- [ ] Define worker management endpoints (list)
- [ ] Configure OpenAPI documentation generation

📄 **File:** `src/main/scala/scheduler/api/Endpoints.scala`

```scala
package scheduler.api

import scheduler.domain._
import sttp.tapir._
import sttp.tapir.json.circe._
import sttp.tapir.generic.auto._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto._
import java.util.UUID
import java.time.Instant

object Endpoints {

  // === Request/Response DTOs ===

  case class CreateDagRequest(
    name: String,
    yaml: String
  )
  object CreateDagRequest {
    implicit val encoder: Encoder[CreateDagRequest] = deriveEncoder
    implicit val decoder: Decoder[CreateDagRequest] = deriveDecoder
  }

  case class DagResponse(
    id: UUID,
    name: String,
    taskCount: Int,
    schedule: Option[String],
    version: Int,
    createdAt: Instant
  )
  object DagResponse {
    implicit val encoder: Encoder[DagResponse] = deriveEncoder

    def from(dag: DagDefinition): DagResponse = DagResponse(
      id = dag.id.value,
      name = dag.name,
      taskCount = dag.tasks.size,
      schedule = dag.schedule,
      version = dag.version,
      createdAt = dag.createdAt
    )
  }

  case class RunResponse(
    runId: UUID,
    dagId: UUID,
    state: String,
    triggeredBy: String,
    createdAt: Instant
  )
  object RunResponse {
    implicit val encoder: Encoder[RunResponse] = deriveEncoder
  }

  case class RunStatusResponse(
    runId: UUID,
    dagId: UUID,
    state: String,
    tasks: List[TaskStatusResponse],
    startedAt: Option[Instant],
    completedAt: Option[Instant],
    createdAt: Instant
  )
  object RunStatusResponse {
    implicit val encoder: Encoder[RunStatusResponse] = deriveEncoder
  }

  case class TaskStatusResponse(
    taskId: String,
    name: String,
    state: String,
    attempt: Int,
    workerId: Option[UUID],
    startedAt: Option[Instant],
    completedAt: Option[Instant]
  )
  object TaskStatusResponse {
    implicit val encoder: Encoder[TaskStatusResponse] = deriveEncoder

    def from(instance: TaskInstance): TaskStatusResponse = TaskStatusResponse(
      taskId = instance.taskId.value,
      name = instance.taskName,
      state = stateToString(instance.state),
      attempt = instance.attempt,
      workerId = instance.workerId.map(_.value),
      startedAt = instance.startedAt,
      completedAt = instance.completedAt
    )

    private def stateToString(state: TaskState): String = state match {
      case TaskState.Pending => "pending"
      case TaskState.Queued => "queued"
      case TaskState.Running => "running"
      case TaskState.Succeeded(_) => "succeeded"
      case TaskState.Failed(_, _) => "failed"
      case TaskState.Skipped => "skipped"
    }
  }

  case class WorkerResponse(
    id: UUID,
    status: String,
    runningTasks: Int,
    queueDepth: Int
  )
  object WorkerResponse {
    implicit val encoder: Encoder[WorkerResponse] = deriveEncoder
  }

  case class ApiError(
    code: String,
    message: String,
    details: Option[String] = None
  )
  object ApiError {
    implicit val encoder: Encoder[ApiError] = deriveEncoder
    implicit val decoder: Decoder[ApiError] = deriveDecoder
  }

  // === Base Endpoint ===

  private val baseEndpoint = endpoint
    .errorOut(
      oneOf[ApiError](
        oneOfVariant(statusCode(sttp.model.StatusCode.NotFound)
          .and(jsonBody[ApiError])),
        oneOfVariant(statusCode(sttp.model.StatusCode.BadRequest)
          .and(jsonBody[ApiError])),
        oneOfVariant(statusCode(sttp.model.StatusCode.InternalServerError)
          .and(jsonBody[ApiError]))
      )
    )

  // === DAG Endpoints ===

  val createDag = baseEndpoint.post
    .in("api" / "v1" / "dags")
    .in(jsonBody[CreateDagRequest])
    .out(statusCode(sttp.model.StatusCode.Created).and(jsonBody[DagResponse]))
    .summary("Create a new DAG")
    .description("Parse and validate a DAG from YAML definition")

  val getDag = baseEndpoint.get
    .in("api" / "v1" / "dags" / path[UUID]("dagId"))
    .out(jsonBody[DagResponse])
    .summary("Get DAG by ID")

  val listDags = baseEndpoint.get
    .in("api" / "v1" / "dags")
    .in(query[Int]("limit").default(20).description("Maximum number of results"))
    .in(query[Int]("offset").default(0).description("Offset for pagination"))
    .out(jsonBody[List[DagResponse]])
    .summary("List all DAGs")
    .description("Returns paginated list of DAG definitions")

  val deleteDag = baseEndpoint.delete
    .in("api" / "v1" / "dags" / path[UUID]("dagId"))
    .out(statusCode(sttp.model.StatusCode.NoContent))
    .summary("Delete a DAG")

  // === Execution Endpoints ===

  val triggerRun = baseEndpoint.post
    .in("api" / "v1" / "dags" / path[UUID]("dagId") / "runs")
    .out(statusCode(sttp.model.StatusCode.Accepted).and(jsonBody[RunResponse]))
    .summary("Trigger a DAG run")
    .description("Start execution of a DAG. Returns immediately with run ID.")

  val getRunStatus = baseEndpoint.get
    .in("api" / "v1" / "runs" / path[UUID]("runId"))
    .out(jsonBody[RunStatusResponse])
    .summary("Get run status")
    .description("Returns detailed status of a DAG run including all task states")

  val listRuns = baseEndpoint.get
    .in("api" / "v1" / "dags" / path[UUID]("dagId") / "runs")
    .in(query[Int]("limit").default(20))
    .out(jsonBody[List[RunResponse]])
    .summary("List runs for a DAG")

  // === Worker Endpoints ===

  val listWorkers = baseEndpoint.get
    .in("api" / "v1" / "workers")
    .out(jsonBody[List[WorkerResponse]])
    .summary("List active workers")
    .description("Returns status of all workers in the pool")

  // === OpenAPI ===

  val all = List(
    createDag, getDag, listDags, deleteDag,
    triggerRun, getRunStatus, listRuns,
    listWorkers
  )
}
```

### 2. Route Handlers

- [ ] Implement handlers for each endpoint
- [ ] Wire up service layer dependencies
- [ ] Handle error mapping to API responses
- [ ] Configure http4s server with middleware

📄 **File:** `src/main/scala/scheduler/api/Routes.scala`

```scala
package scheduler.api

import scheduler.domain._
import scheduler.persistence._
import scheduler.parser._
import scheduler.worker._
import zio._
import sttp.tapir.server.http4s.ztapir._
import org.http4s._

class Routes(
  dagRepo: DagRepository,
  runRepo: DagRunRepository,
  taskRepo: TaskInstanceRepository,
  dagParser: DagParser,
  workerPool: WorkerPoolManager
) {

  private def handleError(error: Throwable): ApiError = error match {
    case SchedulerError.DagNotFound(id) =>
      ApiError("DAG_NOT_FOUND", s"DAG with ID ${id.value} not found")
    case SchedulerError.RunNotFound(id) =>
      ApiError("RUN_NOT_FOUND", s"Run with ID ${id.value} not found")
    case SchedulerError.ValidationFailed(msg) =>
      ApiError("VALIDATION_ERROR", msg)
    case e: DagParseError =>
      ApiError("PARSE_ERROR", DagParseError.render(e))
    case e: DagValidationError =>
      ApiError("VALIDATION_ERROR", DagValidationError.render(e))
    case e =>
      ApiError("INTERNAL_ERROR", e.getMessage)
  }

  val createDagRoute = Endpoints.createDag.zServerLogic { request =>
    (for {
      dag <- ZIO.fromEither(dagParser.parseAndValidate(request.yaml))
        .mapError(handleError)
      savedDag = dag.copy(id = DagId.generate, name = request.name)
      _ <- dagRepo.create(savedDag).mapError(handleError)
    } yield DagResponse.from(savedDag)).either.map {
      case Right(response) => Right(response)
      case Left(error) => Left(error)
    }
  }

  val getDagRoute = Endpoints.getDag.zServerLogic { dagId =>
    dagRepo.get(DagId(dagId))
      .someOrFail(SchedulerError.DagNotFound(DagId(dagId)))
      .map(DagResponse.from)
      .mapError(handleError)
      .either
  }

  val listDagsRoute = Endpoints.listDags.zServerLogic { case (limit, offset) =>
    dagRepo.list(limit, offset)
      .map(_.map(DagResponse.from))
      .mapError(handleError)
      .either
  }

  val deleteDagRoute = Endpoints.deleteDag.zServerLogic { dagId =>
    dagRepo.delete(DagId(dagId))
      .mapError(handleError)
      .either
  }

  val triggerRunRoute = Endpoints.triggerRun.zServerLogic { dagId =>
    (for {
      dag <- dagRepo.get(DagId(dagId))
        .someOrFail(SchedulerError.DagNotFound(DagId(dagId)))
      runId = RunId.generate
      run = DagRun(
        id = runId,
        dagId = dag.id,
        state = TaskState.Pending,
        triggeredBy = "api"
      )
      _ <- runRepo.create(run)
      _ <- workerPool.scheduleRun(dag, runId) // Async execution
    } yield RunResponse(
      runId = runId.value,
      dagId = dag.id.value,
      state = "pending",
      triggeredBy = "api",
      createdAt = run.createdAt
    )).mapError(handleError).either
  }

  val getRunStatusRoute = Endpoints.getRunStatus.zServerLogic { runId =>
    (for {
      run <- runRepo.get(RunId(runId))
        .someOrFail(SchedulerError.RunNotFound(RunId(runId)))
      tasks <- taskRepo.getByRunId(RunId(runId))
    } yield RunStatusResponse(
      runId = run.id.value,
      dagId = run.dagId.value,
      state = stateToString(run.state),
      tasks = tasks.map(TaskStatusResponse.from),
      startedAt = run.startedAt,
      completedAt = run.completedAt,
      createdAt = run.createdAt
    )).mapError(handleError).either
  }

  val listWorkersRoute = Endpoints.listWorkers.zServerLogic { _ =>
    workerPool.getWorkerStatuses
      .map(_.map { case (worker, status, queueDepth) =>
        WorkerResponse(
          id = worker.id.value,
          status = statusToString(status),
          runningTasks = countRunningTasks(status),
          queueDepth = queueDepth
        )
      })
      .mapError(handleError)
      .either
  }

  val allRoutes: HttpRoutes[Task] = {
    import sttp.tapir.server.http4s.ztapir.ZHttp4sServerInterpreter

    ZHttp4sServerInterpreter().from(List(
      createDagRoute,
      getDagRoute,
      listDagsRoute,
      deleteDagRoute,
      triggerRunRoute,
      getRunStatusRoute,
      listWorkersRoute
    )).toRoutes
  }

  private def stateToString(state: TaskState): String = state match {
    case TaskState.Pending => "pending"
    case TaskState.Queued => "queued"
    case TaskState.Running => "running"
    case TaskState.Succeeded(_) => "succeeded"
    case TaskState.Failed(_, _) => "failed"
    case TaskState.Skipped => "skipped"
  }

  private def statusToString(status: WorkerStatus): String = status match {
    case WorkerStatus.Idle => "idle"
    case WorkerStatus.Running(_) => "running"
    case WorkerStatus.Draining => "draining"
    case WorkerStatus.Stopped => "stopped"
  }

  private def countRunningTasks(status: WorkerStatus): Int = status match {
    case WorkerStatus.Running(tasks) => tasks.size
    case _ => 0
  }
}
```

### 3. Worker Pool Implementation

- [ ] Create `Worker` class with task queue and status
- [ ] Implement `WorkerPool` with configurable size
- [ ] Add concurrent task execution with ZIO fibers
- [ ] Implement graceful draining for shutdown

📄 **File:** `src/main/scala/scheduler/worker/Worker.scala`

```scala
package scheduler.worker

import scheduler.domain._
import scheduler.executor._
import scheduler.persistence._
import zio._
import java.util.UUID

case class TaskAssignment(
  instanceId: UUID,
  taskId: TaskId,
  taskName: String,
  taskDefinition: TaskDefinition,
  runId: RunId,
  dagId: DagId,
  attempt: Int
)

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
  statusRef: Ref[WorkerStatus],
  private val taskRepo: TaskInstanceRepository,
  private val executors: Map[String, TaskExecutor]
) {

  def status: UIO[WorkerStatus] = statusRef.get

  def queueDepth: UIO[Int] = taskQueue.size

  def offer(assignment: TaskAssignment): UIO[Boolean] =
    taskQueue.offer(assignment)

  def start: Task[Fiber[Throwable, Nothing]] = {
    ZStream
      .fromQueue(taskQueue)
      .mapZIOPar(concurrency) { assignment =>
        executeAssignment(assignment)
          .catchAll(handleExecutionError(assignment, _))
      }
      .runDrain
      .fork
  }

  private def executeAssignment(assignment: TaskAssignment): Task[Unit] = {
    val executorKey = assignment.taskDefinition.taskType match {
      case _: TaskType.Shell => "shell"
      case _: TaskType.Http => "http"
      case _: TaskType.Python => "python"
    }

    val executor = executors.getOrElse(executorKey,
      throw new IllegalStateException(s"No executor for $executorKey"))

    for {
      // Update status to running
      _ <- statusRef.update {
        case WorkerStatus.Running(tasks) => WorkerStatus.Running(tasks + assignment.instanceId)
        case WorkerStatus.Idle => WorkerStatus.Running(Set(assignment.instanceId))
        case other => other
      }

      // Update task state to running
      _ <- taskRepo.updateState(assignment.instanceId, TaskState.Running)

      // Execute
      ctx = ExecutionContext.create(assignment.runId, assignment.dagId, assignment.attempt)
      result <- executor.execute(assignment.taskDefinition, ctx)
        .timeout(zio.Duration.fromJava(assignment.taskDefinition.timeout))
        .map(_.getOrElse(TaskResult(
          success = false,
          output = None,
          error = Some("Timeout"),
          duration = assignment.taskDefinition.timeout
        )))

      // Update task state based on result
      newState = if (result.success) {
        TaskState.Succeeded(java.time.Instant.now())
      } else {
        TaskState.Failed(result.error.getOrElse("Unknown error"), assignment.attempt)
      }
      _ <- taskRepo.updateState(assignment.instanceId, newState)

      // Update worker status
      _ <- statusRef.update {
        case WorkerStatus.Running(tasks) =>
          val remaining = tasks - assignment.instanceId
          if (remaining.isEmpty) WorkerStatus.Idle else WorkerStatus.Running(remaining)
        case other => other
      }

      _ <- ZIO.logInfo(
        s"Task ${assignment.taskName} completed with status: ${if (result.success) "SUCCESS" else "FAILED"}"
      )
    } yield ()
  }

  private def handleExecutionError(
    assignment: TaskAssignment,
    error: Throwable
  ): UIO[Unit] = {
    for {
      _ <- ZIO.logError(s"Task ${assignment.taskName} failed with error: ${error.getMessage}")
      _ <- taskRepo.updateState(
        assignment.instanceId,
        TaskState.Failed(error.getMessage, assignment.attempt)
      ).orDie
      _ <- statusRef.update {
        case WorkerStatus.Running(tasks) =>
          val remaining = tasks - assignment.instanceId
          if (remaining.isEmpty) WorkerStatus.Idle else WorkerStatus.Running(remaining)
        case other => other
      }
    } yield ()
  }

  def drain(timeout: zio.Duration): Task[Unit] = {
    for {
      _ <- statusRef.set(WorkerStatus.Draining)
      _ <- ZIO.logInfo(s"Draining worker ${id.value}")

      // Wait for running tasks to complete
      _ <- statusRef.get.repeatUntil {
        case WorkerStatus.Running(tasks) => tasks.isEmpty
        case _ => true
      }.timeout(timeout)

      _ <- statusRef.set(WorkerStatus.Stopped)
      _ <- ZIO.logInfo(s"Worker ${id.value} stopped")
    } yield ()
  }
}

object Worker {
  def create(
    concurrency: Int,
    taskRepo: TaskInstanceRepository,
    executors: Map[String, TaskExecutor]
  ): UIO[Worker] = {
    for {
      id <- ZIO.succeed(WorkerId.generate)
      queue <- Queue.bounded[TaskAssignment](100)
      status <- Ref.make[WorkerStatus](WorkerStatus.Idle)
    } yield Worker(id, concurrency, queue, status, taskRepo, executors)
  }
}
```

📄 **File:** `src/main/scala/scheduler/worker/WorkerPool.scala`

```scala
package scheduler.worker

import scheduler.domain._
import scheduler.persistence._
import scheduler.executor._
import scheduler.parser._
import zio._
import java.util.UUID

trait WorkerPoolManager {
  def scheduleRun(dag: DagDefinition, runId: RunId): Task[Unit]
  def getWorkerStatuses: UIO[List[(Worker, WorkerStatus, Int)]]
  def shutdown: Task[Unit]
}

class WorkerPool(
  poolSize: Int,
  concurrencyPerWorker: Int,
  taskRepo: TaskInstanceRepository,
  executors: Map[String, TaskExecutor],
  workersRef: Ref[List[Worker]],
  runningFibers: Ref[List[Fiber[Throwable, Nothing]]]
) extends WorkerPoolManager {

  def start: Task[Unit] = {
    for {
      workers <- ZIO.foreach(1 to poolSize) { _ =>
        Worker.create(concurrencyPerWorker, taskRepo, executors)
      }
      _ <- workersRef.set(workers.toList)
      fibers <- ZIO.foreach(workers.toList)(_.start)
      _ <- runningFibers.set(fibers)
      _ <- ZIO.logInfo(s"Started worker pool with $poolSize workers")
    } yield ()
  }

  def scheduleRun(dag: DagDefinition, runId: RunId): Task[Unit] = {
    for {
      // Create task instances
      _ <- createTaskInstances(dag, runId)

      // Get sorted tasks
      sortedTasks <- ZIO.fromEither(DagValidator.topologicalSort(dag.tasks))
        .mapError(e => new RuntimeException(e.toString))

      // Schedule ready tasks
      _ <- scheduleReadyTasks(dag.id, runId, sortedTasks)
    } yield ()
  }

  private def createTaskInstances(dag: DagDefinition, runId: RunId): Task[Unit] = {
    ZIO.foreachDiscard(dag.tasks) { task =>
      val instance = TaskInstance(
        id = UUID.randomUUID(),
        taskId = task.id,
        runId = runId,
        dagId = dag.id,
        taskName = task.name,
        state = TaskState.Pending
      )
      taskRepo.create(instance)
    }
  }

  private def scheduleReadyTasks(
    dagId: DagId,
    runId: RunId,
    sortedTasks: List[TaskDefinition]
  ): Task[Unit] = {
    // Simple implementation: schedule tasks as dependencies complete
    // In Phase 4, this becomes the TaskRouter with polling

    def scheduleTask(task: TaskDefinition): Task[Unit] = {
      for {
        instances <- taskRepo.getByRunId(runId)
        instance = instances.find(_.taskId == task.id).get

        // Wait for dependencies to complete
        _ <- waitForDependencies(task.dependencies.toList, instances)

        // Check if dependencies failed
        depsFailed = task.dependencies.exists { depId =>
          instances.find(_.taskId == depId).exists(_.state match {
            case TaskState.Failed(_, _) | TaskState.Skipped => true
            case _ => false
          })
        }

        _ <- if (depsFailed) {
          taskRepo.updateState(instance.id, TaskState.Skipped)
        } else {
          assignToWorker(TaskAssignment(
            instanceId = instance.id,
            taskId = task.id,
            taskName = task.name,
            taskDefinition = task,
            runId = runId,
            dagId = dagId,
            attempt = 1
          ))
        }
      } yield ()
    }

    ZIO.foreachDiscard(sortedTasks)(scheduleTask)
  }

  private def waitForDependencies(
    deps: List[TaskId],
    instances: List[TaskInstance]
  ): Task[Unit] = {
    if (deps.isEmpty) ZIO.unit
    else {
      val checkComplete = ZIO.foreach(deps) { depId =>
        ZIO.succeed(instances.find(_.taskId == depId).exists(_.state.isTerminal))
      }.map(_.forall(identity))

      checkComplete.flatMap { allComplete =>
        if (allComplete) ZIO.unit
        else ZIO.sleep(100.millis) *> waitForDependencies(deps, instances)
      }
    }
  }

  private def assignToWorker(assignment: TaskAssignment): Task[Unit] = {
    for {
      workers <- workersRef.get

      // Find least loaded worker
      workerLoads <- ZIO.foreach(workers) { worker =>
        worker.queueDepth.map(depth => (worker, depth))
      }
      leastLoaded = workerLoads.minBy(_._2)._1

      // Claim and assign
      claimed <- taskRepo.claimTask(assignment.instanceId, leastLoaded.id)
      _ <- ZIO.when(claimed)(leastLoaded.offer(assignment).unit)
    } yield ()
  }

  def getWorkerStatuses: UIO[List[(Worker, WorkerStatus, Int)]] = {
    for {
      workers <- workersRef.get
      statuses <- ZIO.foreach(workers) { worker =>
        for {
          status <- worker.status
          depth <- worker.queueDepth
        } yield (worker, status, depth)
      }
    } yield statuses
  }

  def shutdown: Task[Unit] = {
    for {
      _ <- ZIO.logInfo("Shutting down worker pool...")
      workers <- workersRef.get
      _ <- ZIO.foreachParDiscard(workers)(_.drain(60.seconds))
      fibers <- runningFibers.get
      _ <- ZIO.foreachDiscard(fibers)(_.interrupt)
      _ <- ZIO.logInfo("Worker pool shutdown complete")
    } yield ()
  }
}

object WorkerPool {
  def create(
    poolSize: Int,
    concurrencyPerWorker: Int,
    taskRepo: TaskInstanceRepository,
    executors: Map[String, TaskExecutor]
  ): UIO[WorkerPool] = {
    for {
      workersRef <- Ref.make(List.empty[Worker])
      fibersRef <- Ref.make(List.empty[Fiber[Throwable, Nothing]])
    } yield new WorkerPool(
      poolSize, concurrencyPerWorker, taskRepo, executors, workersRef, fibersRef
    )
  }

  val layer: ZLayer[
    TaskInstanceRepository with Map[String, TaskExecutor],
    Nothing,
    WorkerPoolManager
  ] = ZLayer.fromZIO {
    for {
      taskRepo <- ZIO.service[TaskInstanceRepository]
      executors <- ZIO.service[Map[String, TaskExecutor]]
      pool <- create(4, 2, taskRepo, executors)
      _ <- pool.start
    } yield pool: WorkerPoolManager
  }
}
```

### 4. Task Router

- [ ] Implement polling for ready tasks
- [ ] Add least-loaded worker selection
- [ ] Handle task claiming with optimistic locking

📄 **File:** `src/main/scala/scheduler/scheduler/TaskRouter.scala`

```scala
package scheduler.scheduler

import scheduler.domain._
import scheduler.persistence._
import scheduler.worker._
import zio._

class TaskRouter(
  taskRepo: TaskInstanceRepository,
  dagRepo: DagRepository,
  workers: Ref[List[Worker]]
) {

  def start: Task[Fiber[Throwable, Nothing]] = {
    pollForReadyTasks
      .repeat(Schedule.fixed(100.millis))
      .forever
      .fork
  }

  private def pollForReadyTasks: Task[Unit] = {
    for {
      ready <- taskRepo.findReady(limit = 50)
      _ <- ZIO.foreachParDiscard(ready)(assignToWorker)
    } yield ()
  }

  private def assignToWorker(instance: TaskInstance): Task[Unit] = {
    for {
      workerList <- workers.get

      // Find least loaded worker
      workerLoads <- ZIO.foreach(workerList) { worker =>
        worker.queueDepth.map(depth => (worker, depth))
      }

      _ <- workerLoads.minByOption(_._2) match {
        case Some((worker, _)) =>
          for {
            // Try to claim the task
            claimed <- taskRepo.claimTask(instance.id, worker.id)
            _ <- ZIO.when(claimed) {
              // Fetch task definition and create assignment
              createAssignment(instance).flatMap(worker.offer).unit
            }
          } yield ()
        case None =>
          ZIO.logWarning("No workers available")
      }
    } yield ()
  }

  private def createAssignment(instance: TaskInstance): Task[TaskAssignment] = {
    for {
      dag <- dagRepo.get(instance.dagId)
        .someOrFail(new RuntimeException(s"DAG not found: ${instance.dagId}"))
      task = dag.tasks.find(_.id == instance.taskId)
        .getOrElse(throw new RuntimeException(s"Task not found: ${instance.taskId}"))
    } yield TaskAssignment(
      instanceId = instance.id,
      taskId = instance.taskId,
      taskName = instance.taskName,
      taskDefinition = task,
      runId = instance.runId,
      dagId = instance.dagId,
      attempt = instance.attempt
    )
  }
}
```

### 5. API Server Setup

- [ ] Configure http4s Ember server
- [ ] Add request logging middleware
- [ ] Configure CORS if needed
- [ ] Add OpenAPI documentation endpoint

📄 **File:** `src/main/scala/scheduler/api/ApiServer.scala`

```scala
package scheduler.api

import zio._
import org.http4s._
import org.http4s.ember.server._
import org.http4s.server.middleware._
import com.comcast.ip4s._
import sttp.tapir.docs.openapi._
import sttp.tapir.openapi.circe.yaml._

class ApiServer(
  routes: Routes,
  host: String,
  port: Int
) {

  private val openApiDocs = OpenAPIDocsInterpreter()
    .toOpenAPI(Endpoints.all, "Distributed Task Scheduler", "1.0.0")

  private val docsRoute = org.http4s.dsl.Http4sDsl[Task]
  import docsRoute._

  private val openApiRoute = HttpRoutes.of[Task] {
    case GET -> Root / "docs" / "openapi.yaml" =>
      Ok(openApiDocs.toYaml)
  }

  private val allRoutes = {
    val combined = routes.allRoutes <+> openApiRoute

    // Add middleware
    Logger.httpRoutes[Task](
      logHeaders = true,
      logBody = false
    )(combined)
  }

  def start: Task[Unit] = {
    EmberServerBuilder
      .default[Task]
      .withHost(Host.fromString(host).getOrElse(host"0.0.0.0"))
      .withPort(Port.fromInt(port).getOrElse(port"8080"))
      .withHttpApp(allRoutes.orNotFound)
      .build
      .use(_ => ZIO.never)
  }
}

object ApiServer {
  def layer(host: String, port: Int): ZLayer[Routes, Nothing, ApiServer] =
    ZLayer.fromFunction(new ApiServer(_, host, port))
}
```

## Files to Modify

| File | Lines | Action | Description |
|------|-------|--------|-------------|
| `build.sbt` | 20-30 | Modify | Add http4s, tapir dependencies |
| `src/main/scala/scheduler/api/Endpoints.scala` | - | Create | Tapir endpoint definitions |
| `src/main/scala/scheduler/api/Routes.scala` | - | Create | HTTP route handlers |
| `src/main/scala/scheduler/api/ApiServer.scala` | - | Create | Server configuration |
| `src/main/scala/scheduler/worker/Worker.scala` | - | Create | Single worker |
| `src/main/scala/scheduler/worker/WorkerPool.scala` | - | Create | Pool management |
| `src/main/scala/scheduler/scheduler/TaskRouter.scala` | - | Create | Task routing |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Worker queue overflow | 🟡 Medium | Use bounded queue with backpressure |
| Task claiming race conditions | 🟡 Medium | Optimistic locking with retry |
| Memory pressure from many workers | 🟢 Low | Start with 4 workers, monitor heap |
| Connection pool exhaustion | 🟡 Medium | Separate pools for API and workers |

## Definition of Done

- [ ] All API endpoints functional (test with curl)
- [ ] OpenAPI documentation accessible at `/docs/openapi.yaml`
- [ ] Can submit DAG via `POST /api/v1/dags`
- [ ] Can trigger run via `POST /api/v1/dags/{id}/runs`
- [ ] Run status shows real-time task states
- [ ] Worker pool executes tasks in parallel
- [ ] Least-loaded worker selection working
- [ ] Integration tests for API endpoints
- [ ] Load test with 10 concurrent DAG submissions

## Related Issues

- Depends on: #3 (Phase 2)
- Parent: #1 (Meta Issue)
- Blocks: #5 (Phase 4: Distributed Coordination)
