package com.webank.wedatasphere.linkis.rpc.nacos.sender

import com.alibaba.nacos.api.naming.NamingService
import com.alibaba.nacos.client.naming.NacosNamingService
import com.webank.wedatasphere.linkis.rpc.conf.RPCConfiguration
import com.webank.wedatasphere.linkis.server.utils.AopTargetUtils
import javax.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration

@Configuration
class NacosClientRefreshUtils {

  @Autowired
  private var namingService: NamingService = _
  private var clientLastRefreshTime = 0l

  val serviceRefreshInterval = RPCConfiguration.BDP_RPC_NACOS_SERVICE_REFRESH_INTERVAL.getValue.toLong
  private[eureka] def refreshEurekaClient(): Unit = if(System.currentTimeMillis - clientLastRefreshTime > serviceRefreshInterval) synchronized {
    if (System.currentTimeMillis - clientLastRefreshTime < serviceRefreshInterval) return
    clientLastRefreshTime = System.currentTimeMillis
    namingService match {
      case nacosNamingService: NacosNamingService =>
        // TODO
      case _ =>
    }
  }

  @PostConstruct
  def storeEurekaClientRefreshUtils(): Unit = {
    namingService = AopTargetUtils.getTarget(namingService).asInstanceOf[NamingService]
    NacosClientRefreshUtils.nacosClientRefreshUtils = this
  }

}

object NacosClientRefreshUtils {

  var nacosClientRefreshUtils: NacosClientRefreshUtils = _

  def refreshEurekaClient(): Unit = nacosClientRefreshUtils.refreshEurekaClient()

}