package spark.scheduler.cluster

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import akka.actor._
import akka.util.duration._
import akka.pattern.ask

import spark.{SparkException, Logging, TaskState}
import akka.dispatch.Await
import java.util.concurrent.atomic.AtomicInteger
import akka.remote.{RemoteClientShutdown, RemoteClientDisconnected, RemoteClientLifeCycleEvent}

/**
 * A standalone scheduler backend, which waits for standalone executors to connect to it through
 * Akka. These may be executed in a variety of ways, such as Mesos tasks for the coarse-grained
 * Mesos mode or standalone processes for Spark's standalone deploy mode (spark.deploy.*).
 */
private[spark]
class StandaloneSchedulerBackend(scheduler: ClusterScheduler, actorSystem: ActorSystem)
  extends SchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)

  class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor {
    val slaveActor = new HashMap[String, ActorRef]
    val slaveAddress = new HashMap[String, Address]
    val slaveHost = new HashMap[String, String]
    val freeCores = new HashMap[String, Int]
    val actorToSlaveId = new HashMap[ActorRef, String]
    val addressToSlaveId = new HashMap[Address, String]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    }

    def receive = {
      case RegisterSlave(workerId, host, cores) =>
        if (slaveActor.contains(workerId)) {
          sender ! RegisterSlaveFailed("Duplicate slave ID: " + workerId)
        } else {
          logInfo("Registered slave: " + sender + " with ID " + workerId)
          sender ! RegisteredSlave(sparkProperties)
          context.watch(sender)
          slaveActor(workerId) = sender
          slaveHost(workerId) = host
          freeCores(workerId) = cores
          slaveAddress(workerId) = sender.path.address
          actorToSlaveId(sender) = workerId
          addressToSlaveId(sender.path.address) = workerId
          totalCoreCount.addAndGet(cores)
          makeOffers()
        }

      case StatusUpdate(workerId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          freeCores(workerId) += 1
          makeOffers(workerId)
        }

      case ReviveOffers =>
        makeOffers()

      case StopDriver =>
        sender ! true
        context.stop(self)

      case Terminated(actor) =>
        actorToSlaveId.get(actor).foreach(removeSlave(_, "Akka actor terminated"))

      case RemoteClientDisconnected(transport, address) =>
        addressToSlaveId.get(address).foreach(removeSlave(_, "remote Akka client disconnected"))

      case RemoteClientShutdown(transport, address) =>
        addressToSlaveId.get(address).foreach(removeSlave(_, "remote Akka client shutdown"))
    }

    // Make fake resource offers on all slaves
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(
        slaveHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id))}))
    }

    // Make fake resource offers on just one slave
    def makeOffers(workerId: String) {
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(workerId, slaveHost(workerId), freeCores(workerId)))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        freeCores(task.slaveId) -= 1
        slaveActor(task.slaveId) ! LaunchTask(task)
      }
    }

    // Remove a disconnected slave from the cluster
    def removeSlave(workerId: String, reason: String) {
      logInfo("Slave " + workerId + " disconnected, so removing it")
      val numCores = freeCores(workerId)
      actorToSlaveId -= slaveActor(workerId)
      addressToSlaveId -= slaveAddress(workerId)
      slaveActor -= workerId
      slaveHost -= workerId
      freeCores -= workerId
      slaveHost -= workerId
      totalCoreCount.addAndGet(-numCores)
      scheduler.slaveLost(workerId, SlaveLost(reason))
    }
  }

  var driverActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    val iterator = System.getProperties.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = StandaloneSchedulerBackend.ACTOR_NAME)
  }

  override def stop() {
    try {
      if (driverActor != null) {
        val timeout = 5.seconds
        val future = driverActor.ask(StopDriver)(timeout)
        Await.result(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's master actor", e)
    }
  }

  override def reviveOffers() {
    driverActor ! ReviveOffers
  }

  override def defaultParallelism(): Int = math.max(totalCoreCount.get(), 2)
}

private[spark] object StandaloneSchedulerBackend {
  val ACTOR_NAME = "StandaloneScheduler"
}
