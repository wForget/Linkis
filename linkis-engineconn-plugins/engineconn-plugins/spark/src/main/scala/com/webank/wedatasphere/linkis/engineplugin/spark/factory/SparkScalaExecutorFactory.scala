/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.engineplugin.spark.factory

import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconn.common.engineconn.EngineConn
import com.webank.wedatasphere.linkis.engineconn.core.executor.ExecutorManager
import com.webank.wedatasphere.linkis.engineconn.executor.entity.Executor
import com.webank.wedatasphere.linkis.engineplugin.spark.entity.SparkEngineSession
import com.webank.wedatasphere.linkis.engineplugin.spark.executor.{SparkExecutorOrder, SparkScalaExecutor}
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.ExecutorFactory
import com.webank.wedatasphere.linkis.manager.label.entity.Label
import com.webank.wedatasphere.linkis.manager.label.entity.engine.{EngineRunTypeLabel, RunType}

/**
  *
  * @date 2020/11/2
  */
class SparkScalaExecutorFactory extends ExecutorFactory {
  /**
   * Order of executors, the smallest one is the default
   *
   * @return
   */
  override def getOrder: Int = SparkExecutorOrder.SCALA.id

  override def canCreate(labels: Array[Label[_]]): Boolean = {
    labels.foreach(l => l match {
      case label: EngineRunTypeLabel =>
        if (RunType.SCALA.toString.equalsIgnoreCase(label.getRunType)) {
          return true
        }
      case _ =>
    })
    false
  }

  /**
   *
   * @param engineCreationContext
   * @param engineConn
   * @param labels
   * @return
   */
  override def createExecutor(engineCreationContext: EngineCreationContext, engineConn: EngineConn, labels: Array[Label[_]]): Executor = {
    engineConn.getEngine match {
      case sparkEngineSession: SparkEngineSession =>
        val id = ExecutorManager.getInstance().generateId()
        val executor = new SparkScalaExecutor(sparkEngineSession, id)
        executor.getExecutorLabels().add(getDefaultEngineRunTypeLabel())
        executor
    }
  }

  override def getDefaultEngineRunTypeLabel(): EngineRunTypeLabel = {
    val runTypeLabel = new EngineRunTypeLabel
    runTypeLabel.setRunType(RunType.SCALA.toString)
    runTypeLabel
  }
}
