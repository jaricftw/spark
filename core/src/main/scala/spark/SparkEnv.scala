package spark

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.remote.RemoteActorRefProvider

import spark.broadcast.BroadcastManager
import spark.storage.BlockManager
import spark.storage.BlockManagerMaster
import spark.network.ConnectionManager
import spark.util.AkkaUtils

class SparkEnv (
    val actorSystem: ActorSystem,
    val cache: Cache,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheTracker: CacheTracker,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val httpFileServer: HttpFileServer
  ) {

  /** No-parameter constructor for unit tests. */
  def this() = {
    this(null, null, new JavaSerializer, new JavaSerializer, null, null, null, null, null, null, null, null)
  }

  def stop() {
    httpFileServer.stop()
    mapOutputTracker.stop()
    cacheTracker.stop()
    shuffleFetcher.stop()
    shuffleManager.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    actorSystem.shutdown()
    // Akka's awaitTermination doesn't actually wait until the port is unbound, so sleep a bit
    Thread.sleep(100)
    actorSystem.awaitTermination()
    // Akka's awaitTermination doesn't actually wait until the port is unbound, so sleep a bit
    Thread.sleep(100)
  }
}

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(
      hostname: String,
      port: Int,
      isMaster: Boolean,
      isLocal: Boolean
    ) : SparkEnv = {

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, port)

    // Bit of a hack: If this is the master and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.master.port to it.
    if (isMaster && port == 0) {
      System.setProperty("spark.master.port", boundPort.toString)
    }

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = System.getProperty(propertyName, defaultClassName)
      Class.forName(name, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializer = instantiateClass[Serializer]("spark.serializer", "spark.JavaSerializer")
    
    val blockManagerMaster = new BlockManagerMaster(actorSystem, isMaster, isLocal)
    val blockManager = new BlockManager(blockManagerMaster, serializer)
    
    val connectionManager = blockManager.connectionManager 
    
    val shuffleManager = new ShuffleManager()

    val broadcastManager = new BroadcastManager(isMaster)

    val closureSerializer = instantiateClass[Serializer](
      "spark.closure.serializer", "spark.JavaSerializer")

    val cache = instantiateClass[Cache]("spark.cache.class", "spark.BoundedMemoryCache")

    val cacheTracker = new CacheTracker(actorSystem, isMaster, blockManager)
    blockManager.cacheTracker = cacheTracker

    val mapOutputTracker = new MapOutputTracker(actorSystem, isMaster)

    val shuffleFetcher = instantiateClass[ShuffleFetcher](
      "spark.shuffle.fetcher", "spark.BlockStoreShuffleFetcher")
    
    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    System.setProperty("spark.fileserver.uri", httpFileServer.serverUri)

    new SparkEnv(
      actorSystem,
      cache,
      serializer,
      closureSerializer,
      cacheTracker,
      mapOutputTracker,
      shuffleFetcher,
      shuffleManager,
      broadcastManager,
      blockManager,
      connectionManager,
      httpFileServer)
  }
}
