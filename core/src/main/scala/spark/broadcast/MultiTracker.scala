package spark.broadcast

import java.io._
import java.net._
import java.util.{UUID, Random}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.Map

import spark._

private object MultiTracker
extends Logging {

  // Tracker Messages
  val REGISTER_BROADCAST_TRACKER = 0
  val UNREGISTER_BROADCAST_TRACKER = 1
  val FIND_BROADCAST_TRACKER = 2

  // Map to keep track of guides of ongoing broadcasts
  var valueToGuideMap = Map[UUID, SourceInfo]()

  // Random number generator
  var ranGen = new Random

  private var initialized = false
  private var isMaster_ = false

  private var stopBroadcast = false

  private var trackMV: TrackMultipleValues = null

  def initialize(isMaster__ : Boolean) {
    synchronized {
      if (!initialized) {

        isMaster_ = isMaster__

        if (isMaster) {
          trackMV = new TrackMultipleValues
          trackMV.setDaemon(true)
          trackMV.start()
        
          // Set masterHostAddress to the master's IP address for the slaves to read
          System.setProperty("spark.MultiTracker.MasterHostAddress", Utils.localIpAddress)
        }

        initialized = true
      }
    }
  }
  
  def stop() {
    stopBroadcast = true
  }

  // Load common parameters
  private var MasterHostAddress_ = System.getProperty(
    "spark.MultiTracker.MasterHostAddress", "")
  private var MasterTrackerPort_ = System.getProperty(
    "spark.broadcast.masterTrackerPort", "11111").toInt
  private var BlockSize_ = System.getProperty(
    "spark.broadcast.blockSize", "4096").toInt * 1024
  private var MaxRetryCount_ = System.getProperty(
    "spark.broadcast.maxRetryCount", "2").toInt

  private var TrackerSocketTimeout_ = System.getProperty(
    "spark.broadcast.trackerSocketTimeout", "50000").toInt
  private var ServerSocketTimeout_ = System.getProperty(
    "spark.broadcast.serverSocketTimeout", "10000").toInt

  private var MinKnockInterval_ = System.getProperty(
    "spark.broadcast.minKnockInterval", "500").toInt
  private var MaxKnockInterval_ = System.getProperty(
    "spark.broadcast.maxKnockInterval", "999").toInt

  // Load TreeBroadcast config params
  private var MaxDegree_ = System.getProperty(
    "spark.broadcast.maxDegree", "2").toInt

  // Load BitTorrentBroadcast config params
  private var MaxPeersInGuideResponse_ = System.getProperty(
    "spark.broadcast.maxPeersInGuideResponse", "4").toInt

  private var MaxChatSlots_ = System.getProperty(
    "spark.broadcast.maxChatSlots", "4").toInt
  private var MaxChatTime_ = System.getProperty(
    "spark.broadcast.maxChatTime", "500").toInt
  private var MaxChatBlocks_ = System.getProperty(
    "spark.broadcast.maxChatBlocks", "1024").toInt

  private var EndGameFraction_ = System.getProperty(
      "spark.broadcast.endGameFraction", "0.95").toDouble

  def isMaster = isMaster_

  // Common config params
  def MasterHostAddress = MasterHostAddress_
  def MasterTrackerPort = MasterTrackerPort_
  def BlockSize = BlockSize_
  def MaxRetryCount = MaxRetryCount_

  def TrackerSocketTimeout = TrackerSocketTimeout_
  def ServerSocketTimeout = ServerSocketTimeout_

  def MinKnockInterval = MinKnockInterval_
  def MaxKnockInterval = MaxKnockInterval_

  // TreeBroadcast configs
  def MaxDegree = MaxDegree_

  // BitTorrentBroadcast configs
  def MaxPeersInGuideResponse = MaxPeersInGuideResponse_

  def MaxChatSlots = MaxChatSlots_
  def MaxChatTime = MaxChatTime_
  def MaxChatBlocks = MaxChatBlocks_

  def EndGameFraction = EndGameFraction_

  class TrackMultipleValues
  extends Thread with Logging {
    override def run() {
      var threadPool = Utils.newDaemonCachedThreadPool()
      var serverSocket: ServerSocket = null

      serverSocket = new ServerSocket(MasterTrackerPort)
      logInfo("TrackMultipleValues started at " + serverSocket)

      try {
        while (!stopBroadcast) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(TrackerSocketTimeout)
            clientSocket = serverSocket.accept()
          } catch {
            case e: Exception => {
              if (stopBroadcast) {
                logInfo("Stopping TrackMultipleValues...")
              }              
            }
          }

          if (clientSocket != null) {
            try {
              threadPool.execute(new Thread {
                override def run() {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush()
                  val ois = new ObjectInputStream(clientSocket.getInputStream)

                  try {
                    // First, read message type
                    val messageType = ois.readObject.asInstanceOf[Int]

                    if (messageType == REGISTER_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]
                      // Receive hostAddress and listenPort
                      val gInfo = ois.readObject.asInstanceOf[SourceInfo]

                      // Add to the map
                      valueToGuideMap.synchronized {
                        valueToGuideMap += (uuid -> gInfo)
                      }

                      logInfo ("New broadcast " + uuid + " registered with TrackMultipleValues. Ongoing ones: " + valueToGuideMap)

                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    } else if (messageType == UNREGISTER_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]

                      // Remove from the map
                      valueToGuideMap.synchronized {
                        valueToGuideMap(uuid) = SourceInfo("", SourceInfo.TxOverGoToDefault)
                      }

                      logInfo ("Broadcast " + uuid + " unregistered from TrackMultipleValues. Ongoing ones: " + valueToGuideMap)

                      // Send dummy ACK
                      oos.writeObject(-1)
                      oos.flush()
                    } else if (messageType == FIND_BROADCAST_TRACKER) {
                      // Receive UUID
                      val uuid = ois.readObject.asInstanceOf[UUID]

                      var gInfo =
                        if (valueToGuideMap.contains(uuid)) valueToGuideMap(uuid)
                        else SourceInfo("", SourceInfo.TxNotStartedRetry)

                      logDebug("Got new request: " + clientSocket + " for " + uuid + " : " + gInfo.listenPort)

                      // Send reply back
                      oos.writeObject(gInfo)
                      oos.flush()
                    } else {
                      throw new SparkException("Undefined messageType at TrackMultipleValues")
                    }
                  } catch {
                    case e: Exception => {
                      logError("TrackMultipleValues had a " + e)
                    }
                  } finally {
                    ois.close()
                    oos.close()
                    clientSocket.close()
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case ioe: IOException => clientSocket.close()
            }
          }
        }
      } finally {
        serverSocket.close()
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }
  }
  
  def getGuideInfo(variableUUID: UUID): SourceInfo = {
    var clientSocketToTracker: Socket = null
    var oosTracker: ObjectOutputStream = null
    var oisTracker: ObjectInputStream = null

    var gInfo: SourceInfo = SourceInfo("", SourceInfo.TxNotStartedRetry)

    var retriesLeft = MultiTracker.MaxRetryCount
    do {
      try {
        // Connect to the tracker to find out GuideInfo
        clientSocketToTracker =
          new Socket(MultiTracker.MasterHostAddress, MultiTracker.MasterTrackerPort)
        oosTracker =
          new ObjectOutputStream(clientSocketToTracker.getOutputStream)
        oosTracker.flush()
        oisTracker =
          new ObjectInputStream(clientSocketToTracker.getInputStream)

        // Send messageType/intention
        oosTracker.writeObject(MultiTracker.FIND_BROADCAST_TRACKER)
        oosTracker.flush()

        // Send UUID and receive GuideInfo
        oosTracker.writeObject(variableUUID)
        oosTracker.flush()
        gInfo = oisTracker.readObject.asInstanceOf[SourceInfo]
      } catch {
        case e: Exception => logError("getGuideInfo had a " + e)
      } finally {
        if (oisTracker != null) {
          oisTracker.close()
        }
        if (oosTracker != null) {
          oosTracker.close()
        }
        if (clientSocketToTracker != null) {
          clientSocketToTracker.close()
        }
      }

      Thread.sleep(MultiTracker.ranGen.nextInt(
        MultiTracker.MaxKnockInterval - MultiTracker.MinKnockInterval) +
        MultiTracker.MinKnockInterval)

      retriesLeft -= 1
    } while (retriesLeft > 0 && gInfo.listenPort == SourceInfo.TxNotStartedRetry)

    logDebug("Got this guidePort from Tracker: " + gInfo.listenPort)
    return gInfo
  }
  
  def registerBroadcast(uuid: UUID, gInfo: SourceInfo) {
    val socket = new Socket(MultiTracker.MasterHostAddress, MasterTrackerPort)
    val oosST = new ObjectOutputStream(socket.getOutputStream)
    oosST.flush()
    val oisST = new ObjectInputStream(socket.getInputStream)

    // Send messageType/intention
    oosST.writeObject(REGISTER_BROADCAST_TRACKER)
    oosST.flush()

    // Send UUID of this broadcast
    oosST.writeObject(uuid)
    oosST.flush()

    // Send this tracker's information
    oosST.writeObject(gInfo)
    oosST.flush()

    // Receive ACK and throw it away
    oisST.readObject.asInstanceOf[Int]

    // Shut stuff down
    oisST.close()
    oosST.close()
    socket.close()
  }

  def unregisterBroadcast(uuid: UUID) {
    val socket = new Socket(MultiTracker.MasterHostAddress, MasterTrackerPort)
    val oosST = new ObjectOutputStream(socket.getOutputStream)
    oosST.flush()
    val oisST = new ObjectInputStream(socket.getInputStream)

    // Send messageType/intention
    oosST.writeObject(UNREGISTER_BROADCAST_TRACKER)
    oosST.flush()

    // Send UUID of this broadcast
    oosST.writeObject(uuid)
    oosST.flush()

    // Receive ACK and throw it away
    oisST.readObject.asInstanceOf[Int]

    // Shut stuff down
    oisST.close()
    oosST.close()
    socket.close()
  }

  // Helper method to convert an object to Array[BroadcastBlock]
  def blockifyObject[IN](obj: IN): VariableInfo = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.close()
    val byteArray = baos.toByteArray
    val bais = new ByteArrayInputStream(byteArray)

    var blockNum = (byteArray.length / BlockSize)
    if (byteArray.length % BlockSize != 0)
      blockNum += 1

    var retVal = new Array[BroadcastBlock](blockNum)
    var blockID = 0

    for (i <- 0 until (byteArray.length, BlockSize)) {
      val thisBlockSize = math.min(BlockSize, byteArray.length - i)
      var tempByteArray = new Array[Byte](thisBlockSize)
      val hasRead = bais.read(tempByteArray, 0, thisBlockSize)

      retVal(blockID) = new BroadcastBlock(blockID, tempByteArray)
      blockID += 1
    }
    bais.close()

    var variableInfo = VariableInfo(retVal, blockNum, byteArray.length)
    variableInfo.hasBlocks = blockNum

    return variableInfo
  }

  // Helper method to convert Array[BroadcastBlock] to object
  def unBlockifyObject[OUT](arrayOfBlocks: Array[BroadcastBlock],
                            totalBytes: Int, 
                            totalBlocks: Int): OUT = {

    var retByteArray = new Array[Byte](totalBytes)
    for (i <- 0 until totalBlocks) {
      System.arraycopy(arrayOfBlocks(i).byteArray, 0, retByteArray,
        i * BlockSize, arrayOfBlocks(i).byteArray.length)
    }
    byteArrayToObject(retByteArray)
  }

  private def byteArrayToObject[OUT](bytes: Array[Byte]): OUT = {
    val in = new ObjectInputStream (new ByteArrayInputStream (bytes)){
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
    }    
    val retVal = in.readObject.asInstanceOf[OUT]
    in.close()
    return retVal
  }
}

case class BroadcastBlock(blockID: Int, byteArray: Array[Byte]) 
extends Serializable

case class VariableInfo(@transient arrayOfBlocks : Array[BroadcastBlock],
                        totalBlocks: Int, 
                        totalBytes: Int) 
extends Serializable {
 @transient var hasBlocks = 0 
}
