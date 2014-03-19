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

package org.apache.spark.scheduler

import org.apache.spark.storage.RDDInfo

/**
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
private[spark]
class StageInfo(val stageId: Int, val name: String, val numTasks: Int, val rddInfo: RDDInfo) {
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  var completionTime: Option[Long] = None
  var emittedTaskSizeWarning = false
}

private[spark]
object StageInfo {
  def fromStage(stage: Stage): StageInfo = {
    val rdd = stage.rdd
    val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
    val rddInfo = new RDDInfo(rdd.id, rddName, rdd.partitions.size, rdd.getStorageLevel)
    new StageInfo(stage.id, stage.name, stage.numTasks, rddInfo)
  }
}
