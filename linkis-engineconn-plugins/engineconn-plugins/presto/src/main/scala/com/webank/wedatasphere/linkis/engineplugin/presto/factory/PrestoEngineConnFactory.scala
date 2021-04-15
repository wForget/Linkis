package com.webank.wedatasphere.linkis.engineplugin.presto.factory

import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconn.common.engineconn.{DefaultEngineConn, EngineConn}
import com.webank.wedatasphere.linkis.engineconn.core.executor.ExecutorManager
import com.webank.wedatasphere.linkis.engineconn.executor.entity.Executor
import com.webank.wedatasphere.linkis.engineplugin.presto.conf.PrestoConfiguration
import com.webank.wedatasphere.linkis.engineplugin.presto.executer.PrestoEngineConnExecutor
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.SingleExecutorEngineConnFactory
import com.webank.wedatasphere.linkis.manager.label.entity.engine.{EngineRunTypeLabel, EngineType, RunType}

class PrestoEngineConnFactory extends SingleExecutorEngineConnFactory with Logging{

  override def createExecutor(engineCreationContext: EngineCreationContext, engineConn: EngineConn): Executor = {
    val id = ExecutorManager.getInstance().generateId()
    val executor = new PrestoEngineConnExecutor(PrestoConfiguration.ENGINE_DEFAULT_LIMIT.getValue, id)
    val runTypeLabel = getDefaultEngineRunTypeLabel()
    executor.getExecutorLabels().add(runTypeLabel)
    executor
  }

  override def getDefaultEngineRunTypeLabel(): EngineRunTypeLabel = {
    val runTypeLabel = new EngineRunTypeLabel
    runTypeLabel.setRunType(RunType.PRESTO_SQL.toString)
    runTypeLabel
  }

  override def createEngineConn(engineCreationContext: EngineCreationContext): EngineConn = {
    val engineConn = new DefaultEngineConn(engineCreationContext)
    engineConn.setEngineType(EngineType.PRESTO.toString)
    engineConn
  }

}
