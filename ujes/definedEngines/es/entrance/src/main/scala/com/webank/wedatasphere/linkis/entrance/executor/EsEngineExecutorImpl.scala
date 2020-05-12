package com.webank.wedatasphere.linkis.entrance.executor

import java.util.concurrent.CountDownLatch

import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.entrance.exception.EsConvertResponseException
import com.webank.wedatasphere.linkis.entrance.executor.codeparser.CodeParser
import com.webank.wedatasphere.linkis.entrance.executor.esclient.EsClient
import com.webank.wedatasphere.linkis.scheduler.executer.{ErrorExecuteResponse, ExecuteResponse, SuccessExecuteResponse}
import com.webank.wedatasphere.linkis.server.JMap
import org.apache.http.entity.ContentType
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Cancellable, Response, ResponseListener}

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
class EsEngineExecutorImpl(runType:String, client: EsClient, properties: JMap[String, String]) extends EsEngineExecutor {

  private var cancelable: Cancellable = _
  private var codeParser: CodeParser = _

  override def open: Unit = {
    this.codeParser = CodeParser.codeParser(runType)
  }

  override def parse(code: String): Array[String] = {
    codeParser.parse(code)
  }

  override def executeLine(code: String, storePath: String, alias: String): ExecuteResponse = {
    val realCode = code.trim()
    info(s"es client begins to run jdbc code:\n ${realCode.trim}")
    val countDown = new CountDownLatch(1)
    var executeResponse: ExecuteResponse  = SuccessExecuteResponse()
    cancelable = client.execute(realCode, properties, new ResponseListener {
      override def onSuccess(response: Response): Unit = {
        executeResponse = convertResponse(response, storePath, alias)
        countDown.countDown()
      }
      override def onFailure(exception: Exception): Unit = {
        executeResponse = ErrorExecuteResponse("EsEngineExecutor execute fail. ", exception)
        countDown.countDown()
      }
    })
    countDown.await()
    executeResponse
  }

  private def convertResponse(response: Response, storePath: String, alias: String): ExecuteResponse =  Utils.tryCatch{
    // todo convert response to executeResponse
    if (response.getStatusLine.getStatusCode == 200) {
      val contentType = ContentType.get(response.getEntity).getMimeType.toLowerCase
      val content = EntityUtils.toByteArray(response.getEntity)

      SuccessExecuteResponse()
    } else {
      throw EsConvertResponseException("EsEngineExecutor convert response fail. response code: " + response.getStatusLine.getStatusCode)
    }
  } {
    case _: Throwable => ErrorExecuteResponse("EsEngineExecutor execute fail.", t)
  }

  override def close: Unit = cancelable match {
    case c: Cancellable => c.cancel()
    case _ =>
  }

}