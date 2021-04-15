package com.webank.wedatasphere.linkis.engineplugin.presto.executer

import java.util

import com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.{ConcurrentComputationExecutor, EngineExecutionContext}
import com.webank.wedatasphere.linkis.manager.common.entity.resource.NodeResource
import com.webank.wedatasphere.linkis.manager.label.entity.Label
import com.webank.wedatasphere.linkis.protocol.engine.JobProgressInfo
import com.webank.wedatasphere.linkis.scheduler.executer.ExecuteResponse

class PrestoEngineConnExecutor(override val outputPrintLimit: Int, val id: Int) extends ConcurrentComputationExecutor(outputPrintLimit) {
  override def getConcurrentLimit: Int = ???

  override def killAll(): Unit = ???

  override def executeLine(engineExecutorContext: EngineExecutionContext, code: String): ExecuteResponse = ???

  override def executeCompletely(engineExecutorContext: EngineExecutionContext, code: String, completedLine: String): ExecuteResponse = ???

  override def progress(): Float = ???

  override def getProgressInfo: Array[JobProgressInfo] = ???

  override def getExecutorLabels(): util.List[Label[_]] = ???

  override def setExecutorLabels(labels: util.List[Label[_]]): Unit = ???

  override def supportCallBackLogs(): Boolean = ???

  override def requestExpectedResource(expectedResource: NodeResource): NodeResource = ???

  override def getCurrentNodeResource(): NodeResource = ???

  override def getId(): String = ???
}
