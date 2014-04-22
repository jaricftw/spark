/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId
import java.util.concurrent.CountDownLatch
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * Abstract class that is responsible for supervising a Receiver in the worker.
 * It provides all the necessary interfaces for handling the data received by the receiver.
 */
private[streaming] abstract class ReceiverSupervisor(
    receiver: Receiver[_],
    conf: SparkConf
  ) extends Logging {

  /** Enumeration to identify current state of the StreamingContext */
  object ReceiverState extends Enumeration {
    type CheckpointState = Value
    val Initialized, Started, Stopped = Value
  }
  import ReceiverState._

  // Attach the executor to the receiver
  receiver.attachExecutor(this)

  /** Receiver id */
  protected val streamId = receiver.streamId

  /** Has the receiver been marked for stop. */
  private val stopLatch = new CountDownLatch(1)

  /** Time between a receiver is stopped and started again */
  private val defaultRestartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)

  /** Exception associated with the stopping of the receiver */
  @volatile protected var stoppingError: Throwable = null

  /** State of the receiver */
  @volatile private[streaming] var receiverState = Initialized

  /** Push a single data item to backend data store. */
  def pushSingle(data: Any)

  /** Store the bytes of received data as a data block into Spark's memory. */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Store a iterator of received data as a data block into Spark's memory. */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Store an ArrayBuffer of received data as a data block into Spark's memory. */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** Report errors. */
  def reportError(message: String, throwable: Throwable)

  /** Start the executor */
  def start() {
    startReceiver()
  }

  /** Mark the executor and the receiver for stopping */
  def stop(message: String, error: Option[Throwable]) {
    stoppingError = error.orNull
    stopReceiver(message, error)
    stopLatch.countDown()
  }

  /** Start receiver */
  def startReceiver(): Unit = synchronized {
    try {
      logInfo("Starting receiver")
      onReceiverStart()
      receiverState = Started
    } catch {
      case t: Throwable =>
        stop("Error starting receiver " + streamId, Some(t))
    }
  }

  /** Stop receiver */
  def stopReceiver(message: String, error: Option[Throwable]): Unit = synchronized {
    try {
      receiverState = Stopped
      onReceiverStop(message, error)
    } catch {
      case t: Throwable =>
        stop("Error stopping receiver " + streamId, Some(t))
    }
  }

  /** Restart receiver with delay */
  def restartReceiver(message: String, error: Option[Throwable] = None) {
    restartReceiver(message, error, defaultRestartDelay)
  }

  /** Restart receiver with delay */
  def restartReceiver(message: String, error: Option[Throwable], delay: Int) {
    logWarning("Restarting receiver with delay " + delay + " ms: " + message,
      error.getOrElse(null))
    stopReceiver("Restarting receiver with delay " + delay + "ms: " + message, error)
    future {
      logDebug("Sleeping for " + delay)
      Thread.sleep(delay)
      logDebug("Starting receiver again")
      startReceiver()
      logInfo("Receiver started again")
    }
  }

  /** Called when the receiver needs to be started */
  protected def onReceiverStart(): Unit = synchronized {
    // Call user-defined onStart()
    logInfo("Calling receiver onStart")
    receiver.onStart()
    logInfo("Called receiver onStart")
  }

  /** Called when the receiver needs to be stopped */
  protected def onReceiverStop(message: String, error: Option[Throwable]): Unit = synchronized {
    // Call user-defined onStop()
    logInfo("Calling receiver onStop")
    receiver.onStop()
    logInfo("Called receiver onStop")
  }

  /** Check if receiver has been marked for stopping */
  def isReceiverStarted() = {
    logDebug("state = " + receiverState)
    receiverState == Started
  }

  /** Wait the thread until the executor is stopped */
  def awaitTermination() {
    stopLatch.await()
    logInfo("Waiting for executor stop is over")
    if (stoppingError != null) {
      logError("Stopped executor with error: " + stoppingError)
    } else {
      logWarning("Stopped executor without error")
    }
    if (stoppingError != null) {
      throw stoppingError
    }
  }
}
