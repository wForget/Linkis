package com.webank.wedatasphere.linkis.engineplugin.elasticsearch.factory

import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconn.common.engineconn.EngineConn
import com.webank.wedatasphere.linkis.engineconn.executor.entity.Executor
import com.webank.wedatasphere.linkis.engineplugin.elasticsearch.executer.ElasticSearchExecutorOrder
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.ExecutorFactory
import com.webank.wedatasphere.linkis.manager.label.entity.Label
import com.webank.wedatasphere.linkis.manager.label.entity.engine.{EngineRunTypeLabel, RunType}

class ElasticSearchSqlExecutorFactory extends ExecutorFactory {
  /**
   * Order of executors, the smallest one is the default
   *
   * @return
   */
  override def getOrder: Int = ElasticSearchExecutorOrder.SQL.id

  /**
   *
   * @param engineCreationContext
   * @param engineConn
   * @param labels
   * @return
   */
  override def createExecutor(engineCreationContext: EngineCreationContext, engineConn: EngineConn, labels: Array[Label[_]]): Executor = ???

  override def getDefaultEngineRunTypeLabel(): EngineRunTypeLabel = {
    val runTypeLabel = new EngineRunTypeLabel
    runTypeLabel.setRunType(RunType.ES_SQL.toString)
    runTypeLabel
  }
}
