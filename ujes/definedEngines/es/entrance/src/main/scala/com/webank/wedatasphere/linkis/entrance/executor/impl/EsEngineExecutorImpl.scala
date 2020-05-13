package com.webank.wedatasphere.linkis.entrance.executor.impl

import java.util.concurrent.CountDownLatch

import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.entrance.exception.EsConvertResponseException
import com.webank.wedatasphere.linkis.entrance.executor.codeparser.CodeParser
import com.webank.wedatasphere.linkis.entrance.executor.esclient.EsClient
import com.webank.wedatasphere.linkis.entrance.executor.{EsEngineExecutor, ResultSerialize}
import com.webank.wedatasphere.linkis.scheduler.executer.{ErrorExecuteResponse, ExecuteResponse, SuccessExecuteResponse}
import com.webank.wedatasphere.linkis.server.JMap
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
    runType.trim.toLowerCase match {
      case "esjson" || "json" => CodeParser.ESJSON_CODE_PARSER
      case "essql" || "sql" => CodeParser.ESSQL_CODE_PARSER
      case _ => CodeParser.ESJSON_CODE_PARSER
    }
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

  // convert response to executeResponse
  private def convertResponse(response: Response, storePath: String, alias: String): ExecuteResponse =  Utils.tryCatch{
    if (response.getStatusLine.getStatusCode == 200) {
      ResultSerialize.RESULT_SERIALIZE.serialize(response, storePath, alias)
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