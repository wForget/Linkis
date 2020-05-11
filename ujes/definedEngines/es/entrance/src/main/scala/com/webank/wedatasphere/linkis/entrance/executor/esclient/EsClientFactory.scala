package com.webank.wedatasphere.linkis.entrance.executor.esclient

import java.util

import com.webank.wedatasphere.linkis.common.conf.CommonVars
import com.webank.wedatasphere.linkis.entrance.exception.EsParamsIllegalException
import com.webank.wedatasphere.linkis.server.JMap
import org.apache.commons.lang.StringUtils
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.sniff.Sniffer

import scala.collection.JavaConversions._

/**
 *
 * @author wang_zh
 * @date 2020/5/6
 */
object EsClientFactory {

  def getRestClient(options: JMap[String, String]): EsClient = {
    val key = getDatasourceName(options)
    if (StringUtils.isBlank(key)) {
      return defaultClient
    }

    if (!ES_CLIENT_MAP.contains(key)) {
      ES_CLIENT_MAP synchronized {
        if (!ES_CLIENT_MAP.contains(key)) {
          cacheClient(createRestClient(options))
        }
      }
    }

    ES_CLIENT_MAP.get(key)
  }

  private val ES_CLIENT_MAP: Map[String, EsClient] = new util.HashMap[String, RestClient]()

  private val DATASOURCE_NAME_KEY = "datasourceName"
  private def getDatasourceName(options: JMap[String, String]): String = {
    options.getOrDefault(DATASOURCE_NAME_KEY, "")
  }

  private def cacheClient(client: EsClient) = {
    ES_CLIENT_MAP.put(client.getDatasourceName, client)
  }

  private def createRestClient(options: JMap[String, String]): EsClient = {
    val clusterStr = options.get(ES_CLUSTER.key)
    if (StringUtils.isBlank(clusterStr)) {
      throw EsParamsIllegalException("cluster is blank!")
    }
    val cluster = getCluster(cluster)
    if (cluster.isEmpty) {
      throw EsParamsIllegalException("cluster is empty!")
    }
    val username = options.get(ES_USERNAME.key)
    val password = options.get(ES_PASSWORD.key)

    if (ES_AUTH_CACHE.getValue) {
      setAuthScope(cluster, username, password)
    }

    val httpHosts = cluster.map(item => new HttpHost(item._1, item._2))
    val builder = RestClient.builder(httpHosts: _*)
      .setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => {
        if(!ES_AUTH_CACHE.getValue) {
          httpClientBuilder.disableAuthCaching
        }
//        httpClientBuilder.setDefaultRequestConfig(RequestConfig.DEFAULT)
//        httpClientBuilder.setDefaultConnectionConfig(ConnectionConfig.DEFAULT)
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)

      })
      .setDefaultHeaders(defaultHeaders)

    val client = builder.build

    val sniffer = if (ES_SNIFFER_ENABLE.getValue) {
      Sniffer.builder(client).build
    } else null

    val datasourceName = getDatasourceName(options)
    new EsClientImpl(datasourceName, client, sniffer)
  }

  // default cluster
  private val defaultCluster:Array[(String, Int)] = getCluster(ES_CLUSTER.getValue)

  private val credentialsProvider: Option[CredentialsProvider] = {
    val credentialsProvider = new BasicCredentialsProvider()
    setAuthScope(defaultCluster, ES_USERNAME.getValue, ES_PASSWORD.getValue)
    credentialsProvider
  }

  private val defaultClient = {
    val client = createRestClient(new util.HashMap(CommonVars.properties))
    cacheClient(client)
    client
  }

  private val defaultHeaders: Array[BasicHeader] = {
    val headers = CommonVars.properties.entrySet()
      .filter(entry => entry.getKey != null && entry.getValue != null && entry.getKey.toString.startsWith(ES_HTTP_HEADER_PREFIX))
      .map(entry => new BasicHeader(entry.getKey.toString, entry.getValue.toString))
//    if (!ES_AUTH_CACHE.getValue) {
//      headers.add(new BasicHeader("http.protocol.handle-authentication", "false"))
//    }
    headers.toArray
  }

  // host1:port1,host2:port2 -> [(host1,port1),(host2,port2)]
  private def getCluster(clusterStr: String): Array[(String, Int)] = if (StringUtils.isNotBlank(clusterStr)) {
    clusterStr.split(",")
      .map(value => {
        val arr = value.split(":")
        (arr(0), arr(1).toInt)
      })
  } else Array()

  // set cluster auth
  private def setAuthScope(cluster: Array[(String, Int)], username: String, password: String): Option[CredentialsProvider] = if (cluster != null && !cluster.isEmpty
    && StringUtils.isNotBlank(username)
    && StringUtils.isNotBlank(password)) {
    cluster.foreach{
      case (host, port) => {
        credentialsProvider.setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM, AuthScope.ANY_SCHEME)
          , new UsernamePasswordCredentials(username, password))
      }
      case _ =>
    }
  }

}
