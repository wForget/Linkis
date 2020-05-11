package com.webank.wedatasphere.linkis.entrance.execute

import com.webank.wedatasphere.linkis.entrance.executor.EsEngineExecutor
import com.webank.wedatasphere.linkis.entrance.executor.codeparser.CodeParser
import com.webank.wedatasphere.linkis.entrance.executor.esclient.{EsClient, EsClientFactory}
import com.webank.wedatasphere.linkis.protocol.engine.{JobProgressInfo, RequestTask}
import com.webank.wedatasphere.linkis.scheduler.executer.{ExecuteRequest, ExecuteResponse, SingleTaskInfoSupport, SingleTaskOperateSupport}
import com.webank.wedatasphere.linkis.server.JMap

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
class EsEntranceEngine(id: Long, properties: JMap[String, String], engineExecutors: Seq[EsEngineExecutor]) extends EntranceEngine(id) with SingleTaskOperateSupport with SingleTaskInfoSupport {

  private var client: EsClient = _

  override def execute(executeRequest: ExecuteRequest): ExecuteResponse =  {

  }

  override protected def callExecute(request: RequestTask): EngineExecuteAsynReturn = ???

  def init(): Unit = {
    // TODO initialize es client
    this.client = EsClientFactory.getRestClient(properties)
  }



  override def progress(): Float = ???

  override def getProgressInfo: Array[JobProgressInfo] = ???

  override def log(): String = ???



  override def kill(): Boolean = ???

  override def pause(): Boolean = ???

  override def resume(): Boolean = ???

  override def close(): Unit = ???

}
