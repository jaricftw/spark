package spark

import java.io._
import java.net._
import java.util.{BitSet, Random, Timer, TimerTask, UUID}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadPoolExecutor, ThreadFactory}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * A simple implementation of shuffle using local files served through HTTP.
 *
 * TODO: Add support for compression when spark.compress is set to true.
 */
@serializable
class LocalFileShuffle[K, V, C] extends Shuffle[K, V, C] with Logging {
  @transient var totalSplits = 0
  @transient var hasSplits = 0 
  @transient var hasSplitsBitVector: BitSet = null
  @transient var combiners: HashMap[K,C] = null

  override def compute(input: RDD[(K, V)],
                       numOutputSplits: Int,
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C)
  : RDD[(K, C)] =
  {
    val sc = input.sparkContext
    val shuffleId = LocalFileShuffle.newShuffleId()
    logInfo("Shuffle ID: " + shuffleId)

    val splitRdd = new NumberedSplitRDD(input)
    val numInputSplits = splitRdd.splits.size

    // Run a parallel map and collect to write the intermediate data files,
    // returning a list of inputSplitId -> serverUri pairs
    val outputLocs = splitRdd.map((pair: (Int, Iterator[(K, V)])) => {
      val myIndex = pair._1
      val myIterator = pair._2
      val buckets = Array.tabulate(numOutputSplits)(_ => new HashMap[K, C])
      for ((k, v) <- myIterator) {
        var bucketId = k.hashCode % numOutputSplits
        if (bucketId < 0) { // Fix bucket ID if hash code was negative
          bucketId += numOutputSplits
        }
        val bucket = buckets(bucketId)
        bucket(k) = bucket.get(k) match {
          case Some(c) => mergeValue(c, v)
          case None => createCombiner(v)
        }
      }
      
      for (i <- 0 until numOutputSplits) {
        val file = LocalFileShuffle.getOutputFile(shuffleId, myIndex, i)
        val writeStartTime = System.currentTimeMillis
        logInfo ("BEGIN WRITE: " + file)
        val out = new ObjectOutputStream(new FileOutputStream(file))
        buckets(i).foreach(pair => out.writeObject(pair))
        out.close()
        logInfo ("END WRITE: " + file)
        val writeTime = (System.currentTimeMillis - writeStartTime)
        logInfo ("Writing " + file + " of size " + file.length + " bytes took " + writeTime + " millis.")
      }
      (myIndex, LocalFileShuffle.serverAddress, LocalFileShuffle.serverPort)
    }).collect()

    val splitsByUri = new ArrayBuffer[(String, Int, Int)]
    for ((inputId, serverAddress, serverPort) <- outputLocs) {
      splitsByUri += ((serverAddress, serverPort, inputId))
    }

    // TODO: Could broadcast splitsByUri

    // Return an RDD that does each of the merges for a given partition
    val indexes = sc.parallelize(0 until numOutputSplits, numOutputSplits)
    return indexes.flatMap((myId: Int) => {
      totalSplits = splitsByUri.size
      hasSplitsBitVector = new BitSet (totalSplits)
      combiners = new HashMap[K, C]
      
      while (hasSplits < totalSplits) {
        // Select a random split to pull
        val splitIndex = selectRandomSplit
        val (serverAddress, serverPort, inputId) = 
          splitsByUri (splitIndex)

        val requestPath = "%d/%d/%d".format(shuffleId, inputId, myId)
        
        val shuffleClient = 
          new ShuffleClient(serverAddress, serverPort, requestPath)
        val readStartTime = System.currentTimeMillis
        logInfo ("BEGIN READ: " + requestPath)
        shuffleClient.start
        shuffleClient.join

        val inputStream = new ObjectInputStream (
          new ByteArrayInputStream(shuffleClient.byteArray))
        try {
          while (true) {
            val (k, c) = inputStream.readObject().asInstanceOf[(K, C)]
            combiners(k) = combiners.get(k) match {
              case Some(oldC) => mergeCombiners(oldC, c)
              case None => c
            }
          }
        } catch {
          case e: EOFException => {}
        }
        inputStream.close
        
        hasSplits += 1
        hasSplitsBitVector.synchronized {
          hasSplitsBitVector.set (splitIndex)
        }
        
        logInfo ("END READ: " + requestPath)
        val readTime = (System.currentTimeMillis - readStartTime)
        logInfo ("Reading " + requestPath + " took " + readTime + " millis.")
      }
      combiners
    })
  }
  
  def selectRandomSplit: Int = {
    var requiredSplits = new ArrayBuffer[Int]
    
    hasSplitsBitVector.synchronized {
      for (i <- 0 until totalSplits) {
        if (!hasSplitsBitVector.get(i)) {
          requiredSplits += i
        }
      }
    }
    
    if (requiredSplits.size > 0) {
      requiredSplits(LocalFileShuffle.ranGen.nextInt (requiredSplits.size))
    } else {
      -1
    }
  }
  
  class ShuffleClient (hostAddress: String, listenPort: Int, requestPath: String)
  extends Thread with Logging {
    private var peerSocketToSource: Socket = null
    private var oosSource: ObjectOutputStream = null
    private var oisSource: ObjectInputStream = null
    
    var byteArray: Array[Byte] = null

    override def run: Unit = {
      // Setup the timeout mechanism
      var timeOutTask = new TimerTask {
        override def run: Unit = {
          cleanUpConnections
        }
      }
      
      var timeOutTimer = new Timer
      // TODO: Set wait timer
      // TODO: If its too small, things FAIL
      timeOutTimer.schedule (timeOutTask, 10000)
      
      logInfo ("ShuffleClient started... => %s:%d#%s".format(hostAddress, listenPort, requestPath))
      
      try {
        // Connect to the source
        peerSocketToSource = new Socket (hostAddress, listenPort)
        oosSource =
          new ObjectOutputStream (peerSocketToSource.getOutputStream)
        oosSource.flush
        var isSource = peerSocketToSource.getInputStream
        oisSource = new ObjectInputStream (isSource)
        
        // Send the request
        oosSource.writeObject(requestPath)
        
        // Receive the length of the requested file
        var requestedFileLen = oisSource.readObject.asInstanceOf[Int]
        logInfo ("Received requestedFileLen = " + requestedFileLen)

        // Turn the timer OFF, if the sender responds before timeout
        timeOutTimer.cancel
        
        // Receive the file
        if (requestedFileLen != -1) {
          byteArray = new Array[Byte] (requestedFileLen)
          var bytesRead = isSource.read (byteArray, 0, byteArray.length)
          var alreadyRead = bytesRead

          while (alreadyRead < requestedFileLen) {
            bytesRead = isSource.read(byteArray, alreadyRead, 
              (byteArray.length - alreadyRead))
            if(bytesRead > 0) {
              alreadyRead = alreadyRead + bytesRead
            }
          } 
        } else {
          throw new SparkException("ShuffleServer " + hostAddress + " does not have " + requestPath)
        }
      } catch {
        // EOFException is expected to happen because sender can break
        // connection due to timeout
        case eofe: java.io.EOFException => { }
        case e: Exception => {
          logInfo ("ShuffleClient had a " + e)
        }
      } finally {
        cleanUpConnections
      }
    }
    
    private def cleanUpConnections: Unit = {
      if (oisSource != null) {
        oisSource.close
      }
      if (oosSource != null) {
        oosSource.close
      }
      if (peerSocketToSource != null) {
        peerSocketToSource.close
      }
    }
  }  
}

object LocalFileShuffle extends Logging {
  private var initialized = false
  private var nextShuffleId = new AtomicLong(0)

  // Variables initialized by initializeIfNeeded()
  private var shuffleDir: File = null
  
  private var shuffleServer: ShuffleServer = null
  private var serverAddress = InetAddress.getLocalHost.getHostAddress
  private var serverPort: Int = -1
  
  // Random number generator
  var ranGen = new Random
  
  private def initializeIfNeeded() = synchronized {
    if (!initialized) {
      // TODO: localDir should be created by some mechanism common to Spark
      // so that it can be shared among shuffle, broadcast, etc
      val localDirRoot = System.getProperty("spark.local.dir", "/tmp")
      var tries = 0
      var foundLocalDir = false
      var localDir: File = null
      var localDirUuid: UUID = null
      
      while (!foundLocalDir && tries < 10) {
        tries += 1
        try {
          localDirUuid = UUID.randomUUID()
          localDir = new File(localDirRoot, "spark-local-" + localDirUuid)          
          if (!localDir.exists()) {
            localDir.mkdirs()
            foundLocalDir = true
          }
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create local dir failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed 10 attempts to create local dir in " + localDirRoot)
        System.exit(1)
      }
      shuffleDir = new File(localDir, "shuffle")
      shuffleDir.mkdirs()
      logInfo("Shuffle dir: " + shuffleDir)

      // Create and start the shuffleServer      
      shuffleServer = new ShuffleServer
      shuffleServer.setDaemon (true)
      shuffleServer.start
      logInfo ("ShuffleServer started...")
      
      initialized = true
    }
  }
  
  def getOutputFile(shuffleId: Long, inputId: Int, outputId: Int): File = {
    initializeIfNeeded()
    val dir = new File(shuffleDir, shuffleId + "/" + inputId)
    dir.mkdirs()
    val file = new File(dir, "" + outputId)
    return file
  }

  def newShuffleId(): Long = {
    nextShuffleId.getAndIncrement()
  }
  
  // Returns a standard ThreadFactory except all threads are daemons
  private def newDaemonThreadFactory: ThreadFactory = {
    new ThreadFactory {
      def newThread(r: Runnable): Thread = {
        var t = Executors.defaultThreadFactory.newThread (r)
        t.setDaemon (true)
        return t
      }
    }
  }

  // Wrapper over newFixedThreadPool
  def newDaemonFixedThreadPool (nThreads: Int): ThreadPoolExecutor = {
    var threadPool =
      Executors.newFixedThreadPool (nThreads).asInstanceOf[ThreadPoolExecutor]

    threadPool.setThreadFactory (newDaemonThreadFactory)
    
    return threadPool
  }  
  
  class ShuffleServer
  extends Thread with Logging {
    // TODO: Set config param
    var threadPool = newDaemonFixedThreadPool(2)

    var serverSocket: ServerSocket = null

    override def run: Unit = {
      serverSocket = new ServerSocket (0)
      serverPort = serverSocket.getLocalPort

      logInfo ("ShuffleServer started with " + serverSocket)
      logInfo ("Local URI: http://" + serverAddress + ":" + serverPort)

      try {
        while (true) {
          var clientSocket: Socket = null
          try {
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => { }
          }
          if (clientSocket != null) {
            logInfo ("Serve: Accepted new client connection:" + clientSocket)
            try {
              threadPool.execute (new ShuffleServerThread (clientSocket))
            } catch {
              // In failure, close socket here; else, the thread will close it
              case ioe: IOException => {
                clientSocket.close
              }
            }
          }
        }
      } finally {
        if (serverSocket != null) {
          logInfo ("ShuffleServer now stopping...")
          serverSocket.close
        }
      }
      // Shutdown the thread pool
      threadPool.shutdown
    }
    
    class ShuffleServerThread (val clientSocket: Socket)
    extends Thread with Logging {
      private val os = clientSocket.getOutputStream.asInstanceOf[OutputStream]
      os.flush
      private val bos = new BufferedOutputStream (os)
      bos.flush
      private val oos = new ObjectOutputStream (os)
      oos.flush
      private val ois = new ObjectInputStream (clientSocket.getInputStream)

      logInfo ("new ShuffleServerThread is running")
      
      override def run: Unit = {
        try {
          // Receive requestPath from the receiver
          var requestPath = ois.readObject.asInstanceOf[String]
          logInfo("requestPath: " + shuffleDir + "/" + requestPath)
          
          // Open the file
          var requestedFile: File = null
          var requestedFileLen = -1
          try {
            requestedFile = new File(shuffleDir + "/" + requestPath)
            requestedFileLen = requestedFile.length.toInt
          } catch {
            case e: Exception => { }
          }
          
          // Send the lendth of the requestPath to let the receiver know that 
          // transfer is about to start
          // In the case of receiver timeout and connection close, this will
          // throw a java.net.SocketException: Broken pipe
          oos.writeObject(requestedFileLen)
          oos.flush
          
          logInfo ("requestedFileLen = " + requestedFileLen)

          // Read and send the requested file
          if (requestedFileLen != -1) {
            // Read
            var byteArray = new Array[Byte](requestedFileLen)
            val bis = 
              new BufferedInputStream (new FileInputStream (requestedFile))

            var bytesRead = bis.read (byteArray, 0, byteArray.length)
            var alreadyRead = bytesRead

            while (alreadyRead < requestedFileLen) {
              bytesRead = bis.read(byteArray, alreadyRead, 
                (byteArray.length - alreadyRead))
              if(bytesRead > 0) {
                alreadyRead = alreadyRead + bytesRead
              }
            }            
            bis.close
            
            // Send
            bos.write (byteArray, 0, byteArray.length)
            bos.flush
          } else {
            // Close the connection
          }
        } catch {
          // If something went wrong, e.g., the worker at the other end died etc.
          // then close everything up
          // Exception can happen if the receiver stops receiving
          case e: Exception => {
            logInfo ("ShuffleServerThread had a " + e)
          }
        } finally {
          logInfo ("ShuffleServerThread is closing streams and sockets")
          ois.close
          // TODO: Following can cause "java.net.SocketException: Socket closed"
          oos.close
          bos.close
          clientSocket.close
        }
      }
    }
  }
}
