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

package org.apache.spark.util

import akka.actor.{ActorSystem, ExtendedActorSystem}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.remote.RemoteActorRefProvider

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   */
  def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
    val akkaThreads   = System.getProperty("spark.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt

    val akkaTimeout = System.getProperty("spark.akka.timeout", "60").toInt

    val akkaFrameSize = System.getProperty("spark.akka.frameSize", "10").toInt
    val lifecycleEvents = if (System.getProperty("spark.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"

    val akkaHeartBeatPauses = System.getProperty("spark.akka.pauses", "60").toInt
    val akkaFailureDetector = System.getProperty("spark.akka.failure-detector.threshold", "12.0").toDouble
    // Since we have our own Heart Beat mechanism and TCP already tracks connections. 
    // Using this makes very little sense. So setting this to a relatively larger value suffices.
    val akkaHeartBeatInterval = System.getProperty("spark.akka.heartbeat.interval", "3").toInt 

    val akkaConf = ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.remote.watch-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPauses s
      |akka.remote.watch-failure-detector.heartbeat-interval = $akkaHeartBeatInterval s
      |akka.remote.watch-failure-detector.threshold = $akkaFailureDetector
      |akka.remote.transport-failure-detector.heartbeat-interval = 30 s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = ${akkaHeartBeatPauses + 10} s
      |akka.remote.transport-failure-detector.threshold = $akkaFailureDetector
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeout s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}MiB
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      """.stripMargin)

    val actorSystem = ActorSystem(name, akkaConf)

    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

}
