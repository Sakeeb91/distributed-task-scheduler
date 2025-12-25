## Summary

Enable multiple scheduler instances with leader election, distributed locks, and worker registry using etcd. This phase transforms the single-node scheduler into a distributed system with fault tolerance.

**Estimated Hours**: 30-35
**Priority**: 🟡 High (enables fault tolerance)
**Depends on**: #4 (Phase 3: REST API + Worker Pool)

## System Context

```
distributed-task-scheduler/
├── src/main/scala/scheduler/
│   ├── coordination/
│   │   ├── DistributedCoordinator.scala  # Trait definition
│   │   ├── EtcdCoordinator.scala         # etcd implementation
│   │   └── LeaderElection.scala          # Leader election logic
│   ├── scheduler/
│   │   └── DistributedScheduler.scala    # Leader-aware scheduler
│   └── worker/
│       └── WorkStealing.scala            # Work stealing between workers
```

## Distributed Systems Concepts

This phase implements key distributed systems patterns:

1. **Leader Election**: Only one scheduler instance processes work at a time
2. **Distributed Locks**: Prevent double-execution of tasks
3. **Service Discovery**: Workers register/deregister dynamically
4. **Work Stealing**: Balance load across workers

## Implementation Tasks

### 1. Distributed Coordinator Interface

- [ ] Define coordinator trait with all operations
- [ ] Create data types for leases, locks, and events
- [ ] Design watch/stream APIs for cluster events

📄 **File:** `src/main/scala/scheduler/coordination/DistributedCoordinator.scala`

```scala
package scheduler.coordination

import scheduler.domain._
import zio._
import zio.stream._
import java.time.Instant

trait DistributedCoordinator {
  /**
   * Attempt to become the cluster leader.
   * Returns a lease that must be kept alive.
   */
  def acquireLeadership(candidateId: String): Task[LeadershipLease]

  /**
   * Acquire a distributed lock for exclusive access.
   * Lock is released when the returned DistributedLock is released.
   */
  def acquireLock(lockName: String, ttl: zio.Duration): Task[DistributedLock]

  /**
   * Register a worker in the cluster registry.
   * Registration expires if heartbeat stops.
   */
  def registerWorker(worker: WorkerInfo): Task[WorkerRegistration]

  /**
   * Remove worker from registry.
   */
  def deregisterWorker(workerId: WorkerId): Task[Unit]

  /**
   * Stream of worker join/leave events.
   */
  def watchWorkers: ZStream[Any, Throwable, WorkerEvent]

  /**
   * Get current cluster state snapshot.
   */
  def getClusterState: Task[ClusterState]
}

/**
 * Represents leadership lease that must be kept alive.
 */
case class LeadershipLease(
  candidateId: String,
  isLeader: Ref[Boolean],
  acquiredAt: Instant,
  release: Task[Unit]
) {
  def checkIsLeader: UIO[Boolean] = isLeader.get
}

/**
 * Represents a distributed lock.
 */
case class DistributedLock(
  key: String,
  leaseId: Long,
  acquiredAt: Instant,
  release: Task[Unit]
)

/**
 * Worker information stored in registry.
 */
case class WorkerInfo(
  id: WorkerId,
  host: String,
  port: Int,
  capacity: Int,
  startedAt: Instant
)

/**
 * Worker registration handle for heartbeats.
 */
case class WorkerRegistration(
  workerId: WorkerId,
  leaseId: Long,
  keepAlive: Fiber[Throwable, Unit],
  deregister: Task[Unit]
)

/**
 * Events from worker registry watch.
 */
sealed trait WorkerEvent
object WorkerEvent {
  case class Joined(worker: WorkerInfo) extends WorkerEvent
  case class Left(workerId: WorkerId) extends WorkerEvent
  case class Updated(worker: WorkerInfo) extends WorkerEvent
}

/**
 * Snapshot of cluster state.
 */
case class ClusterState(
  leaderId: Option[String],
  leaderSince: Option[Instant],
  workers: List[WorkerInfo],
  activeRuns: Int
)
```

### 2. etcd Implementation

- [ ] Configure jetcd client with connection pool
- [ ] Implement leader election with lease renewal
- [ ] Implement distributed locking
- [ ] Implement worker registry with watches

📄 **File:** `src/main/scala/scheduler/coordination/EtcdCoordinator.scala`

```scala
package scheduler.coordination

import scheduler.domain._
import io.etcd.jetcd._
import io.etcd.jetcd.options._
import io.etcd.jetcd.watch._
import io.etcd.jetcd.lease._
import io.etcd.jetcd.kv._
import io.etcd.jetcd.op._
import zio._
import zio.stream._
import io.circe.syntax._
import io.circe.parser._
import scala.jdk.CollectionConverters._
import java.time.Instant
import java.nio.charset.StandardCharsets

class EtcdCoordinator(client: Client) extends DistributedCoordinator {

  private val charset = StandardCharsets.UTF_8

  // Key prefixes
  private val leaderKey = "/scheduler/leader"
  private val workersPrefix = "/scheduler/workers/"
  private val locksPrefix = "/scheduler/locks/"

  // Lease TTL in seconds
  private val leaderLeaseTtl = 30L
  private val workerLeaseTtl = 30L
  private val lockLeaseTtl = 30L

  def acquireLeadership(candidateId: String): Task[LeadershipLease] = {
    for {
      isLeaderRef <- Ref.make(false)

      // Create lease for leader key
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(leaderLeaseTtl).get()
      )

      // Try to become leader with compare-and-swap
      acquired <- attemptLeaderElection(candidateId, lease.getID)
      _ <- isLeaderRef.set(acquired)

      // Start lease keep-alive
      keepAliveFiber <- startLeaseKeepAlive(lease.getID).fork

      // Watch for leadership changes
      watchFiber <- watchLeadershipChanges(candidateId, isLeaderRef).fork

      acquiredAt <- Clock.instant

    } yield LeadershipLease(
      candidateId = candidateId,
      isLeader = isLeaderRef,
      acquiredAt = acquiredAt,
      release = for {
        _ <- keepAliveFiber.interrupt
        _ <- watchFiber.interrupt
        _ <- ZIO.attemptBlocking(client.getLeaseClient.revoke(lease.getID).get())
        _ <- ZIO.logInfo(s"Released leadership for $candidateId")
      } yield ()
    )
  }

  private def attemptLeaderElection(candidateId: String, leaseId: Long): Task[Boolean] = {
    ZIO.attemptBlocking {
      val keyBytes = ByteSequence.from(leaderKey, charset)
      val valueBytes = ByteSequence.from(candidateId, charset)

      // Compare-and-swap: only set if key doesn't exist (version = 0)
      val txn = client.getKVClient.txn()
      val response = txn
        .If(new Cmp(keyBytes, Cmp.Op.EQUAL, CmpTarget.version(0)))
        .Then(Op.put(keyBytes, valueBytes, PutOption.builder().withLeaseId(leaseId).build()))
        .commit()
        .get()

      if (response.isSucceeded) {
        true
      } else {
        // Check if we're already the leader
        val getResponse = client.getKVClient.get(keyBytes).get()
        getResponse.getKvs.asScala.headOption
          .map(kv => kv.getValue.toString(charset) == candidateId)
          .getOrElse(false)
      }
    }
  }

  private def startLeaseKeepAlive(leaseId: Long): Task[Unit] = {
    ZIO.attemptBlocking {
      val keepAlive = client.getLeaseClient.keepAlive(
        leaseId,
        new StreamObserver[LeaseKeepAliveResponse] {
          override def onNext(response: LeaseKeepAliveResponse): Unit = ()
          override def onError(t: Throwable): Unit = {
            // Log error but don't crash - leadership will be lost
          }
          override def onCompleted(): Unit = ()
        }
      )
      keepAlive
    }.unit
  }

  private def watchLeadershipChanges(
    candidateId: String,
    isLeaderRef: Ref[Boolean]
  ): Task[Unit] = {
    ZStream.asyncZIO[Any, Throwable, Boolean] { callback =>
      ZIO.attemptBlocking {
        val keyBytes = ByteSequence.from(leaderKey, charset)
        client.getWatchClient.watch(
          keyBytes,
          new Watch.Listener {
            override def onNext(response: WatchResponse): Unit = {
              response.getEvents.asScala.foreach { event =>
                val newLeader = event.getKeyValue.getValue.toString(charset)
                val isLeader = newLeader == candidateId
                callback(ZIO.succeed(Chunk(isLeader)))
              }
            }
            override def onError(t: Throwable): Unit =
              callback(ZIO.fail(Some(t)))
            override def onCompleted(): Unit =
              callback(ZIO.fail(None))
          }
        )
      }.unit
    }.foreach(isLeader => isLeaderRef.set(isLeader))
  }

  def acquireLock(lockName: String, ttl: zio.Duration): Task[DistributedLock] = {
    val lockKey = s"$locksPrefix$lockName"

    for {
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(ttl.toSeconds).get()
      )

      lock <- ZIO.attemptBlocking {
        client.getLockClient.lock(
          ByteSequence.from(lockKey, charset),
          lease.getID
        ).get()
      }

      acquiredAt <- Clock.instant

    } yield DistributedLock(
      key = lock.getKey.toString(charset),
      leaseId = lease.getID,
      acquiredAt = acquiredAt,
      release = ZIO.attemptBlocking {
        client.getLockClient.unlock(lock.getKey).get()
      }.unit
    )
  }

  def registerWorker(worker: WorkerInfo): Task[WorkerRegistration] = {
    val key = s"$workersPrefix${worker.id.value}"

    for {
      lease <- ZIO.attemptBlocking(
        client.getLeaseClient.grant(workerLeaseTtl).get()
      )

      _ <- ZIO.attemptBlocking {
        val keyBytes = ByteSequence.from(key, charset)
        val valueBytes = ByteSequence.from(workerToJson(worker), charset)
        client.getKVClient.put(
          keyBytes,
          valueBytes,
          PutOption.builder().withLeaseId(lease.getID).build()
        ).get()
      }

      keepAliveFiber <- startLeaseKeepAlive(lease.getID).fork

      _ <- ZIO.logInfo(s"Registered worker ${worker.id.value}")

    } yield WorkerRegistration(
      workerId = worker.id,
      leaseId = lease.getID,
      keepAlive = keepAliveFiber,
      deregister = for {
        _ <- keepAliveFiber.interrupt
        _ <- ZIO.attemptBlocking(client.getLeaseClient.revoke(lease.getID).get())
        _ <- ZIO.logInfo(s"Deregistered worker ${worker.id.value}")
      } yield ()
    )
  }

  def deregisterWorker(workerId: WorkerId): Task[Unit] = {
    val key = s"$workersPrefix${workerId.value}"
    ZIO.attemptBlocking {
      client.getKVClient.delete(ByteSequence.from(key, charset)).get()
    }.unit
  }

  def watchWorkers: ZStream[Any, Throwable, WorkerEvent] = {
    ZStream.asyncZIO[Any, Throwable, WorkerEvent] { callback =>
      ZIO.attemptBlocking {
        val prefixBytes = ByteSequence.from(workersPrefix, charset)
        client.getWatchClient.watch(
          prefixBytes,
          WatchOption.builder().withPrefix(prefixBytes).build(),
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

  def getClusterState: Task[ClusterState] = {
    for {
      // Get leader
      leaderOpt <- ZIO.attemptBlocking {
        val keyBytes = ByteSequence.from(leaderKey, charset)
        val response = client.getKVClient.get(keyBytes).get()
        response.getKvs.asScala.headOption.map { kv =>
          (kv.getValue.toString(charset), Instant.ofEpochSecond(kv.getCreateRevision))
        }
      }

      // Get workers
      workers <- ZIO.attemptBlocking {
        val prefixBytes = ByteSequence.from(workersPrefix, charset)
        val response = client.getKVClient.get(
          prefixBytes,
          GetOption.builder().withPrefix(prefixBytes).build()
        ).get()
        response.getKvs.asScala.map(kv => parseWorker(kv)).toList
      }

    } yield ClusterState(
      leaderId = leaderOpt.map(_._1),
      leaderSince = leaderOpt.map(_._2),
      workers = workers,
      activeRuns = 0 // TODO: Track this
    )
  }

  // JSON serialization helpers
  private def workerToJson(worker: WorkerInfo): String = {
    import io.circe.generic.auto._
    worker.asJson.noSpaces
  }

  private def parseWorker(kv: KeyValue): WorkerInfo = {
    import io.circe.generic.auto._
    val json = kv.getValue.toString(charset)
    decode[WorkerInfo](json).getOrElse(
      throw new RuntimeException(s"Failed to parse worker: $json")
    )
  }

  private def extractWorkerId(kv: KeyValue): WorkerId = {
    val key = kv.getKey.toString(charset)
    val id = key.stripPrefix(workersPrefix)
    WorkerId(java.util.UUID.fromString(id))
  }
}

object EtcdCoordinator {
  def layer(endpoints: List[String]): ZLayer[Any, Throwable, DistributedCoordinator] =
    ZLayer.scoped {
      ZIO.acquireRelease(
        ZIO.attemptBlocking {
          Client.builder()
            .endpoints(endpoints: _*)
            .build()
        }
      )(client => ZIO.attemptBlocking(client.close()).orDie)
        .map(new EtcdCoordinator(_))
    }
}
```

### 3. Distributed Scheduler

- [ ] Implement leader-only scheduling
- [ ] Add distributed lock for run execution
- [ ] Handle leader failover gracefully

📄 **File:** `src/main/scala/scheduler/scheduler/DistributedScheduler.scala`

```scala
package scheduler.scheduler

import scheduler.domain._
import scheduler.coordination._
import scheduler.persistence._
import scheduler.worker._
import zio._
import java.util.UUID

class DistributedScheduler(
  coordinator: DistributedCoordinator,
  workerPool: WorkerPoolManager,
  dagRepo: DagRepository,
  runRepo: DagRunRepository,
  taskRepo: TaskInstanceRepository
) {

  private val instanceId = UUID.randomUUID().toString

  def start: Task[Unit] = {
    for {
      _ <- ZIO.logInfo(s"Starting distributed scheduler instance: $instanceId")

      lease <- coordinator.acquireLeadership(instanceId)

      _ <- lease.checkIsLeader.flatMap { isLeader =>
        if (isLeader) {
          ZIO.logInfo("Acquired leadership, starting scheduler loop")
        } else {
          ZIO.logInfo("Standing by as follower")
        }
      }

      // Main scheduler loop
      _ <- runSchedulerLoop(lease).forever.ensuring(lease.release)
    } yield ()
  }

  private def runSchedulerLoop(lease: LeadershipLease): Task[Unit] = {
    lease.checkIsLeader.flatMap { isLeader =>
      if (isLeader) {
        for {
          _ <- ZIO.logDebug("Running scheduler iteration as leader")

          // Find pending runs
          pendingRuns <- runRepo.findPending(limit = 50)
          _ <- ZIO.logDebug(s"Found ${pendingRuns.size} pending runs")

          // Process each run with distributed lock
          _ <- ZIO.foreachParDiscard(pendingRuns) { run =>
            processRunWithLock(run)
          }
        } yield ()
      } else {
        // Not leader, just wait
        ZIO.sleep(1.second)
      }
    }
  }

  private def processRunWithLock(run: DagRun): Task[Unit] = {
    val lockName = s"run-${run.id.value}"

    coordinator.acquireLock(lockName, 60.seconds).flatMap { lock =>
      processRun(run)
        .ensuring(lock.release)
        .catchAll { error =>
          ZIO.logError(s"Error processing run ${run.id.value}: ${error.getMessage}")
        }
    }.catchAll { error =>
      // Could not acquire lock - another instance is processing
      ZIO.logDebug(s"Could not acquire lock for run ${run.id.value}: ${error.getMessage}")
    }
  }

  private def processRun(run: DagRun): Task[Unit] = {
    for {
      dag <- dagRepo.get(run.dagId)
        .someOrFail(new RuntimeException(s"DAG not found: ${run.dagId}"))

      _ <- runRepo.updateState(run.id, TaskState.Running)

      _ <- workerPool.scheduleRun(dag, run.id)

      _ <- ZIO.logInfo(s"Started processing run ${run.id.value} for DAG ${dag.name}")
    } yield ()
  }
}

object DistributedScheduler {
  val layer: ZLayer[
    DistributedCoordinator
      with WorkerPoolManager
      with DagRepository
      with DagRunRepository
      with TaskInstanceRepository,
    Nothing,
    DistributedScheduler
  ] = ZLayer.fromFunction(new DistributedScheduler(_, _, _, _, _))
}
```

### 4. Work Stealing

- [ ] Implement load monitoring across workers
- [ ] Add task stealing from overloaded workers
- [ ] Configure stealing thresholds

📄 **File:** `src/main/scala/scheduler/worker/WorkStealing.scala`

```scala
package scheduler.worker

import scheduler.domain._
import zio._

class WorkStealingRouter(
  workers: Ref[List[Worker]],
  stealThreshold: Int = 5,
  checkInterval: zio.Duration = 500.millis
) {

  def start: Task[Fiber[Throwable, Nothing]] = {
    checkAndSteal
      .repeat(Schedule.fixed(checkInterval))
      .forever
      .fork
  }

  private def checkAndSteal: Task[Unit] = {
    for {
      workerList <- workers.get
      loads <- getWorkerLoads(workerList)
      _ <- rebalanceIfNeeded(loads)
    } yield ()
  }

  private def getWorkerLoads(
    workerList: List[Worker]
  ): Task[List[(Worker, Int)]] = {
    ZIO.foreach(workerList) { worker =>
      worker.queueDepth.map(depth => (worker, depth))
    }
  }

  private def rebalanceIfNeeded(loads: List[(Worker, Int)]): Task[Unit] = {
    if (loads.size < 2) ZIO.unit
    else {
      val sorted = loads.sortBy(_._2)
      val (leastLoaded, minLoad) = sorted.head
      val (mostLoaded, maxLoad) = sorted.last

      val imbalance = maxLoad - minLoad

      if (imbalance > stealThreshold) {
        val tasksToSteal = imbalance / 2
        ZIO.logDebug(
          s"Rebalancing: stealing $tasksToSteal tasks from worker ${mostLoaded.id.value}"
        ) *>
        stealTasks(mostLoaded, leastLoaded, tasksToSteal)
      } else {
        ZIO.unit
      }
    }
  }

  private def stealTasks(
    from: Worker,
    to: Worker,
    count: Int
  ): Task[Unit] = {
    ZIO.replicateZIO(count) {
      // Poll from the victim's queue (non-blocking)
      from.taskQueue.poll.flatMap {
        case Some(task) =>
          // Re-assign to the thief
          to.offer(task).as(1)
        case None =>
          ZIO.succeed(0)
      }
    }.map(_.sum).flatMap { stolen =>
      ZIO.logDebug(s"Stole $stolen tasks from ${from.id.value} to ${to.id.value}")
    }
  }
}

object WorkStealingRouter {
  def create(
    workers: Ref[List[Worker]],
    stealThreshold: Int = 5
  ): UIO[WorkStealingRouter] = {
    ZIO.succeed(new WorkStealingRouter(workers, stealThreshold))
  }
}
```

## Integration with etcd

### etcd Setup

```yaml
# docker-compose.yml
version: '3.8'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.9
    command:
      - etcd
      - --name=etcd0
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://etcd:2379
    ports:
      - "2379:2379"
    volumes:
      - etcd-data:/etcd-data

volumes:
  etcd-data:
```

### Configuration

```scala
// Application configuration
case class EtcdConfig(
  endpoints: List[String] = List("http://localhost:2379"),
  leaseTtlSeconds: Long = 30
)
```

## Files to Modify

| File | Lines | Action | Description |
|------|-------|--------|-------------|
| `build.sbt` | 25-30 | Modify | Add jetcd dependency |
| `src/main/scala/scheduler/coordination/DistributedCoordinator.scala` | - | Create | Trait definition |
| `src/main/scala/scheduler/coordination/EtcdCoordinator.scala` | - | Create | etcd implementation |
| `src/main/scala/scheduler/scheduler/DistributedScheduler.scala` | - | Create | Leader-aware scheduler |
| `src/main/scala/scheduler/worker/WorkStealing.scala` | - | Create | Work stealing logic |
| `docker-compose.yml` | - | Create | etcd container |

## Testing Distributed Behavior

### Chaos Tests

```scala
object ChaosSpec extends ZIOSpecDefault {

  def spec = suite("Distributed Scheduler")(

    test("recovers from leader failure") {
      for {
        // Start 3 scheduler instances
        scheduler1 <- startScheduler("instance-1").fork
        scheduler2 <- startScheduler("instance-2").fork
        scheduler3 <- startScheduler("instance-3").fork

        // Wait for leader election
        _ <- ZIO.sleep(5.seconds)

        // Verify one is leader
        state <- coordinator.getClusterState
        _ <- assertTrue(state.leaderId.isDefined)

        // Kill the leader
        leader = state.leaderId.get
        _ <- killInstance(leader)

        // Wait for failover
        _ <- ZIO.sleep(35.seconds) // Lease TTL + election time

        // Verify new leader elected
        newState <- coordinator.getClusterState
        _ <- assertTrue(
          newState.leaderId.isDefined,
          newState.leaderId != state.leaderId
        )
      } yield ()
    },

    test("distributed lock prevents double execution") {
      for {
        // Start 2 instances
        _ <- startScheduler("instance-1").fork
        _ <- startScheduler("instance-2").fork

        // Submit a DAG
        runId <- submitDag(testDag)

        // Both instances will try to process
        _ <- ZIO.sleep(5.seconds)

        // Verify task executed exactly once
        instances <- taskRepo.getByRunId(runId)
        executedCount = instances.count(_.state match {
          case TaskState.Succeeded(_) => true
          case _ => false
        })

        _ <- assertTrue(executedCount == 1)
      } yield ()
    }
  )
}
```

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| etcd connection loss | 🔴 High | Implement reconnection with backoff |
| Split brain scenario | 🔴 High | Use etcd's consistency guarantees, verify leases |
| Lease expiration during processing | 🟡 Medium | Check leadership before critical operations |
| High latency to etcd | 🟡 Medium | Use local caching with TTL |

## Definition of Done

- [ ] etcd client connects and authenticates
- [ ] Only one scheduler instance is leader at a time
- [ ] Leader failure triggers election within 30 seconds
- [ ] Workers register/deregister on start/stop
- [ ] Worker failure detected within lease TTL
- [ ] Distributed locks prevent double-execution
- [ ] Work stealing visibly balances load
- [ ] Chaos tests pass consistently
- [ ] Multi-node integration tests pass

## Related Issues

- Depends on: #4 (Phase 3)
- Parent: #1 (Meta Issue)
- Blocks: #6 (Phase 5: Production Hardening)
