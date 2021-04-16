package com.webank.wedatasphere.linkis.engineplugin.elasticsearch.factory

import com.webank.wedatasphere.linkis.engineplugin.elasticsearch.executer.ElasticSearchExecutorOrder
import com.webank.wedatasphere.linkis.manager.label.entity.engine.{EngineRunTypeLabel, RunType}

class ElasticSearchJsonExecutorFactory extends ElasticSearchExecutorFactory {
  /**
   * Order of executors, the smallest one is the default
   *
   * @return
   */
  override def getOrder: Int = ElasticSearchExecutorOrder.JSON.id

  override def getDefaultEngineRunTypeLabel(): EngineRunTypeLabel = {
    val runTypeLabel = new EngineRunTypeLabel
    runTypeLabel.setRunType(RunType.ES_JSON.toString)
    runTypeLabel
  }

}
