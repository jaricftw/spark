package spark.streaming

import java.net.{SocketException, Socket, ServerSocket}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}
import collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import util.ManualClock
import spark.storage.StorageLevel


class InputStreamsSuite extends TestSuiteBase {

  test("network input stream") {
    val serverPort = 9999
    val server = new TestServer(9999)
    server.start()
    val ssc = new StreamingContext(master, framework)
    ssc.setBatchDuration(batchDuration)

    val networkStream = ssc.networkTextStream("localhost", serverPort, StorageLevel.DISK_AND_MEMORY)
    val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String  ]]
    val outputStream = new TestOutputStream(networkStream, outputBuffer)
    ssc.registerOutputStream(outputStream)
    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    val input = Seq(1, 2, 3)
    val expectedOutput = input.map(_.toString)
    for (i <- 0 until input.size) {
      server.send(input(i).toString + "\n")
      Thread.sleep(1000)
      clock.addToTime(1000)
    }
    val startTime = System.currentTimeMillis()
    while (outputBuffer.size < expectedOutput.size && System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo("output.size = " + outputBuffer.size + ", expectedOutput.size = " + expectedOutput.size)
      Thread.sleep(100)
    }
    Thread.sleep(5000)
    val timeTaken = System.currentTimeMillis() - startTime
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")

    ssc.stop()
    server.stop()

    assert(outputBuffer.size === expectedOutput.size)
    for (i <- 0 until outputBuffer.size) {
      assert(outputBuffer(i).size === 1)
      assert(outputBuffer(i).head === expectedOutput(i))
    }
  }
}


class TestServer(port: Int) {

  val queue = new ArrayBlockingQueue[String](100)

  val serverSocket = new ServerSocket(port)

  val servingThread = new Thread() {
    override def run() {
      try {
        while(true) {
          println("Accepting connections on port " + port)
          val clientSocket = serverSocket.accept()
          println("New connection")
          try {
            clientSocket.setTcpNoDelay(true)
            val outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream))

            while(clientSocket.isConnected) {
              val msg = queue.poll(100, TimeUnit.MILLISECONDS)
              if (msg != null) {
                outputStream.write(msg)
                outputStream.flush()
                println("Message '" + msg + "' sent")
              }
            }
          } catch {
            case e: SocketException => println(e)
          } finally {
            println("Connection closed")
            if (!clientSocket.isClosed) clientSocket.close()
          }
        }
      } catch {
        case ie: InterruptedException =>

      } finally {
        serverSocket.close()
      }
    }
  }

  def start() { servingThread.start() }

  def send(msg: String) { queue.add(msg) }

  def stop() { servingThread.interrupt() }
}

object TestServer {
  def main(args: Array[String]) {
    val s = new TestServer(9999)
    s.start()
    while(true) {
      Thread.sleep(1000)
      s.send("hello")
    }
  }
}
