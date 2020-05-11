package com.webank.wedatasphere.linkis.entrance.executor

import java.util.concurrent.CountDownLatch

import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.entrance.executor.esclient.EsClient
import com.webank.wedatasphere.linkis.scheduler.executer.ExecuteResponse
import com.webank.wedatasphere.linkis.server.JMap
import org.elasticsearch.client.{Cancellable, Response, ResponseListener}

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
abstract class EsEngineExecutor(client: EsClient, properties: JMap[String, String]) extends Logging {

  private var cancelable: Cancellable = _

  def execute(code: String): ExecuteResponse = {
    val countDown = new CountDownLatch(1);
    var executeResponse: ExecuteResponse = _
    cancelable = client.execute(code, properties, new ResponseListener {
      override def onSuccess(response: Response): Unit = {
        executeResponse = convertResponse(response)
        countDown.countDown()
      }
      override def onFailure(exception: Exception): Unit = {
        warn("EsEngineExecutor execute error: {}", exception)
        countDown.countDown()
      }
    })
    countDown.await()
    executeResponse
  }

  private def convertResponse(response: Response): ExecuteResponse = {
    // todo convert response to executeResponse
  }

  def close: Unit = cancelable match {
    case c: Cancellable => c.cancel()
    case _ =>
  }

  def kind: Kind

}