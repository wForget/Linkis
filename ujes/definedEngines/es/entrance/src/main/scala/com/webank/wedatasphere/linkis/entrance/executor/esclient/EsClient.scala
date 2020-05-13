package com.webank.wedatasphere.linkis.entrance.executor.esclient

import com.webank.wedatasphere.linkis.entrance.conf.EsEntranceConfiguration._
import com.webank.wedatasphere.linkis.server.JMap
import org.elasticsearch.client.sniff.Sniffer
import org.elasticsearch.client.{Cancellable, Request, ResponseListener, RestClient}


/**
 *
 * @author wang_zh
 * @date 2020/5/6
 */

trait EsClientOperate {

  def execute(code: String, options: JMap[String, String], responseListener: ResponseListener): Cancellable

  def close(): Unit

}

abstract class EsClient(datasourceName:String, client: RestClient, sniffer: Sniffer) extends EsClientOperate {

  def getDatasourceName: String = datasourceName

  def getRestClient: RestClient = client

  def getSniffer: Sniffer = sniffer

  override def close(): Unit = {
    sniffer match {
      case s: Sniffer => s.close()
      case _ =>
    }
    client match {
      case c: RestClient => c.close()
      case _ =>
    }
  }

}

class EsClientImpl(datasourceName:String, client: RestClient, sniffer: Sniffer)
  extends EsClient(datasourceName, client, sniffer) {

  override def execute(code: String, options: JMap[String, String], responseListener: ResponseListener): Cancellable = {
    val request = createRequest(code, options)
    client.performRequestAsync(request, responseListener)
  }

  private def createRequest(code: String, options: JMap[String, String]): Request = {
    val endpoint = ES_HTTP_ENDPOINT.getValue(options)
    val method = ES_HTTP_METHOD.getValue(options)
    val request = new Request(method, endpoint)
    request.setJsonEntity(code)
    request
  }

}
