## Summary

Parse DAG definitions from YAML, validate them for correctness (including cycle detection), and execute tasks sequentially on a single worker. This phase establishes the execution foundation before parallelization.

**Estimated Hours**: 20-25
**Priority**: 🔴 Critical (enables execution)
**Depends on**: #2 (Phase 1: Core Domain Model)

## System Context

```
distributed-task-scheduler/
├── src/main/scala/scheduler/
│   ├── domain/           # From Phase 1
│   ├── parser/
│   │   ├── DagParser.scala
│   │   └── DagValidator.scala
│   ├── executor/
│   │   ├── TaskExecutor.scala
│   │   ├── ShellTaskExecutor.scala
│   │   └── HttpTaskExecutor.scala
│   └── scheduler/
│       └── SimpleScheduler.scala
```

## Implementation Tasks

### 1. DAG Parser

- [ ] Create YAML parsing with circe-yaml
- [ ] Define custom decoders for task types
- [ ] Handle duration parsing (e.g., "5m", "30s")
- [ ] Create error types for parse failures

📄 **File:** `src/main/scala/scheduler/parser/DagParser.scala`

```scala
package scheduler.parser

import scheduler.domain._
import io.circe.yaml.parser
import io.circe._
import io.circe.generic.semiauto._
import cats.syntax.all._

sealed trait DagParseError
object DagParseError {
  case class InvalidYaml(message: String) extends DagParseError
  case class MissingField(field: String) extends DagParseError
  case class InvalidTaskType(given: String) extends DagParseError
  case class InvalidDuration(value: String) extends DagParseError

  def render(error: DagParseError): String = error match {
    case InvalidYaml(msg) => s"Invalid YAML: $msg"
    case MissingField(field) => s"Missing required field: $field"
    case InvalidTaskType(t) => s"Invalid task type: $t (expected: shell, http, python)"
    case InvalidDuration(v) => s"Invalid duration format: $v (expected: 30s, 5m, 1h)"
  }
}

trait DagParser {
  def parse(yaml: String): Either[DagParseError, DagDefinition]
  def validate(dag: DagDefinition): Either[DagValidationError, DagDefinition]
  def parseAndValidate(yaml: String): Either[DagParseError | DagValidationError, DagDefinition]
}

class DagParserImpl extends DagParser {

  // Custom decoder for Duration from strings like "5m", "30s"
  implicit val durationDecoder: Decoder[java.time.Duration] = Decoder.decodeString.emap { str =>
    parseDuration(str).toRight(s"Invalid duration: $str")
  }

  private def parseDuration(str: String): Option[java.time.Duration] = {
    val pattern = """(\d+)(s|m|h|d)""".r
    str.toLowerCase match {
      case pattern(num, "s") => Some(java.time.Duration.ofSeconds(num.toLong))
      case pattern(num, "m") => Some(java.time.Duration.ofMinutes(num.toLong))
      case pattern(num, "h") => Some(java.time.Duration.ofHours(num.toLong))
      case pattern(num, "d") => Some(java.time.Duration.ofDays(num.toLong))
      case _ => None
    }
  }

  def parse(yaml: String): Either[DagParseError, DagDefinition] = {
    parser.parse(yaml)
      .leftMap(e => DagParseError.InvalidYaml(e.getMessage))
      .flatMap { json =>
        json.as[DagDefinition]
          .leftMap(e => DagParseError.MissingField(e.getMessage))
      }
  }

  def validate(dag: DagDefinition): Either[DagValidationError, DagDefinition] = {
    DagValidator.validate(dag)
  }

  def parseAndValidate(yaml: String): Either[DagParseError | DagValidationError, DagDefinition] = {
    parse(yaml).flatMap(dag => validate(dag).leftMap(identity))
  }
}
```

### 2. DAG Validator with Cycle Detection

- [ ] Implement Kahn's algorithm for cycle detection
- [ ] Validate all dependencies exist
- [ ] Check for duplicate task IDs
- [ ] Implement topological sort for execution order

📄 **File:** `src/main/scala/scheduler/parser/DagValidator.scala`

```scala
package scheduler.parser

import scheduler.domain._
import scala.collection.mutable

sealed trait DagValidationError
object DagValidationError {
  case class CycleDetected(cycle: List[TaskId]) extends DagValidationError
  case class MissingDependency(task: TaskId, missing: TaskId) extends DagValidationError
  case class DuplicateTaskId(id: TaskId) extends DagValidationError
  case class EmptyDag(name: String) extends DagValidationError

  def render(error: DagValidationError): String = error match {
    case CycleDetected(cycle) =>
      s"Cycle detected: ${cycle.map(_.value).mkString(" -> ")}"
    case MissingDependency(task, missing) =>
      s"Task '${task.value}' depends on '${missing.value}' which does not exist"
    case DuplicateTaskId(id) =>
      s"Duplicate task ID: ${id.value}"
    case EmptyDag(name) =>
      s"DAG '$name' has no tasks"
  }
}

object DagValidator {

  def validate(dag: DagDefinition): Either[DagValidationError, DagDefinition] = {
    for {
      _ <- validateNonEmpty(dag)
      _ <- validateUniqueIds(dag)
      _ <- validateDependenciesExist(dag)
      _ <- validateNoCycles(dag)
    } yield dag
  }

  private def validateNonEmpty(dag: DagDefinition): Either[DagValidationError, Unit] = {
    if (dag.tasks.isEmpty) Left(DagValidationError.EmptyDag(dag.name))
    else Right(())
  }

  private def validateUniqueIds(dag: DagDefinition): Either[DagValidationError, Unit] = {
    val ids = dag.tasks.map(_.id)
    val duplicates = ids.diff(ids.distinct)
    duplicates.headOption match {
      case Some(dup) => Left(DagValidationError.DuplicateTaskId(dup))
      case None => Right(())
    }
  }

  private def validateDependenciesExist(dag: DagDefinition): Either[DagValidationError, Unit] = {
    val taskIds = dag.tasks.map(_.id).toSet
    dag.tasks.foreach { task =>
      task.dependencies.foreach { dep =>
        if (!taskIds.contains(dep)) {
          return Left(DagValidationError.MissingDependency(task.id, dep))
        }
      }
    }
    Right(())
  }

  private def validateNoCycles(dag: DagDefinition): Either[DagValidationError, Unit] = {
    detectCycle(dag.tasks) match {
      case Some(cycle) => Left(DagValidationError.CycleDetected(cycle))
      case None => Right(())
    }
  }

  /**
   * Detect cycles using Kahn's algorithm (topological sort).
   * Returns Some(cycle) if a cycle is found, None otherwise.
   */
  def detectCycle(tasks: List[TaskDefinition]): Option[List[TaskId]] = {
    // Build adjacency list: task -> tasks that depend on it
    val graph = mutable.Map[TaskId, mutable.Set[TaskId]]()
    val inDegree = mutable.Map[TaskId, Int]().withDefaultValue(0)

    // Initialize graph
    tasks.foreach(t => graph(t.id) = mutable.Set.empty)

    // Build edges: for each dependency, add edge from dependency to task
    tasks.foreach { task =>
      task.dependencies.foreach { dep =>
        graph.getOrElseUpdate(dep, mutable.Set.empty) += task.id
        inDegree(task.id) = inDegree(task.id) + 1
      }
    }

    // Queue nodes with no incoming edges
    val queue = mutable.Queue[TaskId]()
    tasks.map(_.id).filter(id => inDegree(id) == 0).foreach(queue.enqueue)

    val sorted = mutable.ListBuffer[TaskId]()

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

    // If we couldn't sort all nodes, there's a cycle
    if (sorted.size != tasks.size) {
      Some(findCyclePath(tasks, inDegree.filter(_._2 > 0).keys.toSet))
    } else {
      None
    }
  }

  /**
   * Returns tasks in topological order (dependencies first).
   */
  def topologicalSort(tasks: List[TaskDefinition]): Either[DagValidationError, List[TaskDefinition]] = {
    detectCycle(tasks) match {
      case Some(cycle) => Left(DagValidationError.CycleDetected(cycle))
      case None =>
        val taskMap = tasks.map(t => t.id -> t).toMap
        Right(kahnSort(tasks).flatMap(taskMap.get))
    }
  }

  private def kahnSort(tasks: List[TaskDefinition]): List[TaskId] = {
    val graph = mutable.Map[TaskId, mutable.Set[TaskId]]()
    val inDegree = mutable.Map[TaskId, Int]().withDefaultValue(0)

    tasks.foreach(t => graph(t.id) = mutable.Set.empty)
    tasks.foreach { task =>
      task.dependencies.foreach { dep =>
        graph.getOrElseUpdate(dep, mutable.Set.empty) += task.id
        inDegree(task.id) = inDegree(task.id) + 1
      }
    }

    val queue = mutable.Queue[TaskId]()
    val result = mutable.ListBuffer[TaskId]()

    tasks.map(_.id).filter(id => inDegree(id) == 0).foreach(queue.enqueue)

    while (queue.nonEmpty) {
      val node = queue.dequeue()
      result += node
      graph.getOrElse(node, Set.empty).foreach { neighbor =>
        inDegree(neighbor) = inDegree(neighbor) - 1
        if (inDegree(neighbor) == 0) queue.enqueue(neighbor)
      }
    }

    result.toList
  }

  private def findCyclePath(
    tasks: List[TaskDefinition],
    cycleNodes: Set[TaskId]
  ): List[TaskId] = {
    // DFS to find actual cycle path for error reporting
    if (cycleNodes.isEmpty) return Nil

    val taskMap = tasks.map(t => t.id -> t).toMap
    val visited = mutable.Set[TaskId]()
    val path = mutable.ListBuffer[TaskId]()

    def dfs(node: TaskId): Option[List[TaskId]] = {
      if (path.contains(node)) {
        val cycleStart = path.indexOf(node)
        return Some((path.drop(cycleStart) :+ node).toList)
      }
      if (visited.contains(node)) return None

      visited += node
      path += node

      taskMap.get(node).flatMap { task =>
        task.dependencies.flatMap(dfs).headOption
      } match {
        case result @ Some(_) => result
        case None =>
          path.dropRightInPlace(1)
          None
      }
    }

    cycleNodes.flatMap(dfs).headOption.getOrElse(cycleNodes.toList)
  }
}
```

### 3. Task Executors

- [ ] Define `TaskExecutor` trait with execution context
- [ ] Implement `ShellTaskExecutor` with process management
- [ ] Implement `HttpTaskExecutor` with http4s client
- [ ] Handle timeouts and resource cleanup

📄 **File:** `src/main/scala/scheduler/executor/TaskExecutor.scala`

```scala
package scheduler.executor

import scheduler.domain._
import zio._
import java.time.{Duration, Instant}

case class ExecutionContext(
  runId: RunId,
  dagId: DagId,
  attempt: Int,
  startTime: Instant
) {
  def elapsed: Duration = Duration.between(startTime, Instant.now())
}

object ExecutionContext {
  def create(runId: RunId, dagId: DagId, attempt: Int = 1): ExecutionContext =
    ExecutionContext(runId, dagId, attempt, Instant.now())
}

case class TaskResult(
  success: Boolean,
  output: Option[String],
  error: Option[String],
  duration: Duration,
  exitCode: Option[Int] = None
)

trait TaskExecutor {
  def execute(task: TaskDefinition, context: ExecutionContext): Task[TaskResult]

  def supports(taskType: TaskType): Boolean
}

case class TaskExecutionError(
  taskId: TaskId,
  message: String,
  cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)

case class TaskTimeoutError(
  taskId: TaskId,
  timeout: Duration
) extends Exception(s"Task ${taskId.value} timed out after $timeout")
```

📄 **File:** `src/main/scala/scheduler/executor/ShellTaskExecutor.scala`

```scala
package scheduler.executor

import scheduler.domain._
import zio._
import java.io.{BufferedReader, InputStreamReader}
import java.time.Duration
import scala.jdk.CollectionConverters._

class ShellTaskExecutor extends TaskExecutor {

  def supports(taskType: TaskType): Boolean = taskType.isInstanceOf[TaskType.Shell]

  def execute(task: TaskDefinition, ctx: ExecutionContext): Task[TaskResult] = {
    task.taskType match {
      case TaskType.Shell(command, env) =>
        executeShellCommand(task.id, command, env, task.timeout, ctx)
      case other =>
        ZIO.fail(TaskExecutionError(
          task.id,
          s"ShellTaskExecutor cannot execute ${other.getClass.getSimpleName}"
        ))
    }
  }

  private def executeShellCommand(
    taskId: TaskId,
    command: String,
    env: Map[String, String],
    timeout: Duration,
    ctx: ExecutionContext
  ): Task[TaskResult] = {
    ZIO.scoped {
      for {
        startTime <- Clock.instant

        // Create process
        process <- ZIO.acquireRelease(
          ZIO.attemptBlocking {
            val pb = new ProcessBuilder("sh", "-c", command)
            pb.redirectErrorStream(true)
            env.foreach { case (k, v) => pb.environment().put(k, v) }
            pb.start()
          }
        )(p => ZIO.succeed(p.destroyForcibly()).when(p.isAlive).unit)

        // Read output in a fiber
        outputFiber <- ZIO.attemptBlocking {
          val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
          val output = new StringBuilder
          var line: String = null
          while ({ line = reader.readLine(); line != null }) {
            output.append(line).append("\n")
          }
          output.toString().take(50000) // Limit output size
        }.fork

        // Wait for process with timeout
        exitCodeOpt <- ZIO.attemptBlocking(process.waitFor())
          .timeout(zio.Duration.fromJava(timeout))

        output <- outputFiber.join
        endTime <- Clock.instant

        result = exitCodeOpt match {
          case Some(exitCode) =>
            TaskResult(
              success = exitCode == 0,
              output = Some(output),
              error = if (exitCode != 0) Some(s"Process exited with code $exitCode") else None,
              duration = Duration.between(startTime, endTime),
              exitCode = Some(exitCode)
            )
          case None =>
            // Timeout occurred
            TaskResult(
              success = false,
              output = Some(output),
              error = Some(s"Task timed out after $timeout"),
              duration = timeout,
              exitCode = None
            )
        }
      } yield result
    }
  }
}
```

📄 **File:** `src/main/scala/scheduler/executor/HttpTaskExecutor.scala`

```scala
package scheduler.executor

import scheduler.domain._
import zio._
import org.http4s._
import org.http4s.client.Client
import org.http4s.circe._
import io.circe.Json
import java.time.Duration

class HttpTaskExecutor(client: Client[Task]) extends TaskExecutor {

  def supports(taskType: TaskType): Boolean = taskType.isInstanceOf[TaskType.Http]

  def execute(task: TaskDefinition, ctx: ExecutionContext): Task[TaskResult] = {
    task.taskType match {
      case TaskType.Http(method, url, headers, body) =>
        executeHttpRequest(task.id, method, url, headers, body, task.timeout)
      case other =>
        ZIO.fail(TaskExecutionError(
          task.id,
          s"HttpTaskExecutor cannot execute ${other.getClass.getSimpleName}"
        ))
    }
  }

  private def executeHttpRequest(
    taskId: TaskId,
    method: String,
    url: String,
    headers: Map[String, String],
    body: Option[Json],
    timeout: Duration
  ): Task[TaskResult] = {
    for {
      startTime <- Clock.instant

      uri <- ZIO.fromEither(Uri.fromString(url))
        .mapError(e => TaskExecutionError(taskId, s"Invalid URL: $url"))

      httpMethod <- ZIO.fromOption(Method.fromString(method).toOption)
        .orElseFail(TaskExecutionError(taskId, s"Invalid HTTP method: $method"))

      request = {
        val base = Request[Task](method = httpMethod, uri = uri)
        val withHeaders = headers.foldLeft(base) { case (req, (k, v)) =>
          req.putHeaders(Header.Raw(CIString(k), v))
        }
        body.fold(withHeaders)(json => withHeaders.withEntity(json))
      }

      response <- client.run(request).use { response =>
        response.bodyText.compile.string.map { responseBody =>
          (response.status, responseBody)
        }
      }.timeout(zio.Duration.fromJava(timeout))

      endTime <- Clock.instant

      result = response match {
        case Some((status, responseBody)) =>
          TaskResult(
            success = status.isSuccess,
            output = Some(responseBody.take(10000)),
            error = if (!status.isSuccess) Some(s"HTTP ${status.code}: ${status.reason}") else None,
            duration = Duration.between(startTime, endTime)
          )
        case None =>
          TaskResult(
            success = false,
            output = None,
            error = Some(s"Request timed out after $timeout"),
            duration = timeout
          )
      }
    } yield result
  }
}
```

### 4. Simple Scheduler

- [ ] Create sequential executor for single-worker mode
- [ ] Implement retry logic with exponential backoff
- [ ] Track task state transitions in database
- [ ] Handle dependency resolution

📄 **File:** `src/main/scala/scheduler/SimpleScheduler.scala`

```scala
package scheduler

import scheduler.domain._
import scheduler.persistence._
import scheduler.executor._
import scheduler.parser._
import zio._
import java.util.UUID

class SimpleScheduler(
  dagRepo: DagRepository,
  taskRepo: TaskInstanceRepository,
  runRepo: DagRunRepository,
  executors: Map[Class[_], TaskExecutor]
) {

  def executeDag(dagId: DagId): Task[DagRun] = {
    for {
      dag <- dagRepo.get(dagId)
        .someOrFail(SchedulerError.DagNotFound(dagId))

      _ <- ZIO.logInfo(s"Starting execution of DAG: ${dag.name}")

      runId = RunId.generate
      run = DagRun(
        id = runId,
        dagId = dagId,
        state = TaskState.Running,
        triggeredBy = "manual"
      )
      _ <- runRepo.create(run)

      // Get topological order
      sortedTasks <- ZIO.fromEither(DagValidator.topologicalSort(dag.tasks))
        .mapError(e => SchedulerError.ValidationFailed(e.toString))

      // Create task instances
      _ <- createTaskInstances(sortedTasks, runId, dagId)

      // Execute tasks in order
      _ <- executeTasks(sortedTasks, runId, dagId)

      // Mark run as completed
      _ <- runRepo.updateState(runId, TaskState.Succeeded(java.time.Instant.now()))

      completedRun <- runRepo.get(runId)
        .someOrFail(SchedulerError.RunNotFound(runId))
    } yield completedRun
  }

  private def createTaskInstances(
    tasks: List[TaskDefinition],
    runId: RunId,
    dagId: DagId
  ): Task[Unit] = {
    ZIO.foreachDiscard(tasks) { task =>
      val instance = TaskInstance(
        id = UUID.randomUUID(),
        taskId = task.id,
        runId = runId,
        dagId = dagId,
        taskName = task.name,
        state = TaskState.Pending
      )
      taskRepo.create(instance)
    }
  }

  private def executeTasks(
    tasks: List[TaskDefinition],
    runId: RunId,
    dagId: DagId
  ): Task[Unit] = {
    ZIO.foreachDiscard(tasks) { task =>
      for {
        _ <- ZIO.logInfo(s"Executing task: ${task.name}")

        instances <- taskRepo.getByRunId(runId)
        instance = instances.find(_.taskId == task.id).get

        // Check if dependencies succeeded
        depsFailed = task.dependencies.exists { depId =>
          instances.find(_.taskId == depId).exists { depInstance =>
            depInstance.state match {
              case TaskState.Failed(_, _) | TaskState.Skipped => true
              case _ => false
            }
          }
        }

        _ <- if (depsFailed) {
          // Skip this task if dependencies failed
          ZIO.logWarning(s"Skipping task ${task.name} due to failed dependencies") *>
          taskRepo.updateState(instance.id, TaskState.Skipped)
        } else {
          executeWithRetry(task, instance, runId, dagId)
        }
      } yield ()
    }
  }

  private def executeWithRetry(
    task: TaskDefinition,
    instance: TaskInstance,
    runId: RunId,
    dagId: DagId
  ): Task[Unit] = {
    val executor = executors.find(_._1.isInstance(task.taskType))
      .map(_._2)
      .getOrElse(throw new IllegalStateException(s"No executor for ${task.taskType}"))

    val ctx = ExecutionContext.create(runId, dagId)

    def attemptExecution(attempt: Int): Task[TaskResult] = {
      for {
        _ <- taskRepo.updateState(instance.id, TaskState.Running)
        result <- executor.execute(task, ctx.copy(attempt = attempt))
        _ <- if (result.success) {
          taskRepo.updateState(instance.id, TaskState.Succeeded(java.time.Instant.now()))
        } else if (attempt < task.retryPolicy.maxAttempts) {
          val delay = calculateBackoff(task.retryPolicy, attempt)
          ZIO.logWarning(s"Task ${task.name} failed, retrying in $delay (attempt $attempt)") *>
          ZIO.sleep(delay) *>
          attemptExecution(attempt + 1)
        } else {
          ZIO.logError(s"Task ${task.name} failed after ${task.retryPolicy.maxAttempts} attempts") *>
          taskRepo.updateState(instance.id, TaskState.Failed(
            result.error.getOrElse("Unknown error"),
            attempt
          ))
        }
      } yield result
    }

    attemptExecution(1).unit
  }

  private def calculateBackoff(policy: RetryPolicy, attempt: Int): zio.Duration = {
    val delay = policy.initialDelay.toMillis * math.pow(policy.backoffMultiplier, attempt - 1)
    val cappedDelay = math.min(delay, policy.maxDelay.toMillis)
    zio.Duration.fromMillis(cappedDelay.toLong)
  }
}

sealed trait SchedulerError extends Exception
object SchedulerError {
  case class DagNotFound(id: DagId) extends SchedulerError
  case class RunNotFound(id: RunId) extends SchedulerError
  case class ValidationFailed(message: String) extends SchedulerError
}
```

## Unit Tests

📄 **File:** `src/test/scala/scheduler/parser/DagValidatorSpec.scala`

```scala
package scheduler.parser

import scheduler.domain._
import zio.test._

object DagValidatorSpec extends ZIOSpecDefault {

  def spec = suite("DagValidator")(

    suite("cycle detection")(
      test("detects simple A -> B -> A cycle") {
        val tasks = List(
          TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a"),
            dependencies = Set(TaskId("b"))),
          TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
            dependencies = Set(TaskId("a")))
        )

        val result = DagValidator.detectCycle(tasks)
        assertTrue(result.isDefined)
      },

      test("detects three-node cycle") {
        val tasks = List(
          TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a"),
            dependencies = Set(TaskId("c"))),
          TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
            dependencies = Set(TaskId("a"))),
          TaskDefinition(TaskId("c"), "C", TaskType.Shell("echo c"),
            dependencies = Set(TaskId("b")))
        )

        val result = DagValidator.detectCycle(tasks)
        assertTrue(result.isDefined)
      },

      test("accepts valid linear DAG") {
        val tasks = List(
          TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a")),
          TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
            dependencies = Set(TaskId("a"))),
          TaskDefinition(TaskId("c"), "C", TaskType.Shell("echo c"),
            dependencies = Set(TaskId("b")))
        )

        val result = DagValidator.detectCycle(tasks)
        assertTrue(result.isEmpty)
      },

      test("accepts diamond dependency pattern") {
        val tasks = List(
          TaskDefinition(TaskId("start"), "Start", TaskType.Shell("echo start")),
          TaskDefinition(TaskId("left"), "Left", TaskType.Shell("echo left"),
            dependencies = Set(TaskId("start"))),
          TaskDefinition(TaskId("right"), "Right", TaskType.Shell("echo right"),
            dependencies = Set(TaskId("start"))),
          TaskDefinition(TaskId("end"), "End", TaskType.Shell("echo end"),
            dependencies = Set(TaskId("left"), TaskId("right")))
        )

        val result = DagValidator.detectCycle(tasks)
        assertTrue(result.isEmpty)
      }
    ),

    suite("topological sort")(
      test("returns correct order for linear DAG") {
        val tasks = List(
          TaskDefinition(TaskId("c"), "C", TaskType.Shell("echo c"),
            dependencies = Set(TaskId("b"))),
          TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo a")),
          TaskDefinition(TaskId("b"), "B", TaskType.Shell("echo b"),
            dependencies = Set(TaskId("a")))
        )

        val result = DagValidator.topologicalSort(tasks)
        assertTrue(
          result.isRight,
          result.toOption.get.map(_.id) == List(TaskId("a"), TaskId("b"), TaskId("c"))
        )
      }
    ),

    suite("validation")(
      test("rejects missing dependency") {
        val dag = DagDefinition(
          id = DagId.generate,
          name = "test",
          tasks = List(
            TaskDefinition(TaskId("a"), "A", TaskType.Shell("echo"),
              dependencies = Set(TaskId("nonexistent")))
          )
        )

        val result = DagValidator.validate(dag)
        assertTrue(result.isLeft)
      }
    )
  )
}
```

## Files to Modify

| File | Lines | Action | Description |
|------|-------|--------|-------------|
| `build.sbt` | 15-20 | Modify | Add circe-yaml, http4s-client dependencies |
| `src/main/scala/scheduler/parser/DagParser.scala` | - | Create | YAML parsing logic |
| `src/main/scala/scheduler/parser/DagValidator.scala` | - | Create | Cycle detection and validation |
| `src/main/scala/scheduler/executor/TaskExecutor.scala` | - | Create | Executor trait and context |
| `src/main/scala/scheduler/executor/ShellTaskExecutor.scala` | - | Create | Shell command execution |
| `src/main/scala/scheduler/executor/HttpTaskExecutor.scala` | - | Create | HTTP request execution |
| `src/main/scala/scheduler/SimpleScheduler.scala` | - | Create | Sequential task execution |
| `src/test/scala/scheduler/parser/DagValidatorSpec.scala` | - | Create | Validation tests |

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Process zombies from shell execution | 🟡 Medium | Use ZIO.scoped with destroyForcibly cleanup |
| HTTP client resource leaks | 🟡 Medium | Use client.run with proper resource management |
| YAML parsing edge cases | 🟢 Low | Comprehensive test cases for malformed input |
| Timeout handling race conditions | 🟡 Medium | Use ZIO timeout with proper fiber interruption |

## Definition of Done

- [ ] Can parse valid YAML DAG definitions
- [ ] Rejects DAGs with cycles (with helpful error message showing the cycle)
- [ ] Rejects DAGs with missing dependencies
- [ ] Can execute a 3-task linear DAG with shell commands
- [ ] Shell task executor handles exit codes correctly
- [ ] HTTP task executor makes real requests
- [ ] Retry logic triggers on failure with exponential backoff
- [ ] All execution state persisted to database
- [ ] Unit tests for parser and validator pass
- [ ] Integration tests for executors pass

## Related Issues

- Depends on: #2 (Phase 1)
- Parent: #1 (Meta Issue)
- Blocks: #4 (Phase 3: REST API)
