package com.webank.wedatasphere.linkis.engineplugin.elasticsearch.factory

import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconn.common.engineconn.EngineConn
import com.webank.wedatasphere.linkis.engineconn.core.executor.ExecutorManager
import com.webank.wedatasphere.linkis.engineconn.executor.entity.Executor
import com.webank.wedatasphere.linkis.engineplugin.elasticsearch.conf.ElasticSearchConfiguration
import com.webank.wedatasphere.linkis.engineplugin.elasticsearch.executer.ElasticSearchEngineConnExecutor
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.ExecutorFactory
import com.webank.wedatasphere.linkis.manager.label.entity.Label

abstract class ElasticSearchExecutorFactory extends ExecutorFactory {

  /**
   *
   * @param engineCreationContext
   * @param engineConn
   * @param labels
   * @return
   */
  override def createExecutor(engineCreationContext: EngineCreationContext, engineConn: EngineConn, labels: Array[Label[_]]): Executor = {
    val id = ExecutorManager.getInstance().generateId()
    val executor = new ElasticSearchEngineConnExecutor(ElasticSearchConfiguration.ENGINE_DEFAULT_LIMIT.getValue, id)
    val runTypeLabel = getDefaultEngineRunTypeLabel()
    executor.getExecutorLabels().add(runTypeLabel)
    executor
  }

}
