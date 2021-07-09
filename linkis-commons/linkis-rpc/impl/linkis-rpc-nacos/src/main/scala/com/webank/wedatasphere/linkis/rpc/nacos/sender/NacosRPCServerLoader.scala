package com.webank.wedatasphere.linkis.rpc.nacos.sender

import com.webank.wedatasphere.linkis.common.ServiceInstance
import com.webank.wedatasphere.linkis.rpc.conf.RPCConfiguration
import com.webank.wedatasphere.linkis.rpc.interceptor.AbstractRPCServerLoader

import scala.concurrent.duration.Duration

class NacosRPCServerLoader extends AbstractRPCServerLoader {

  override val refreshMaxWaitTime: Duration = _

  override def refreshAllServers(): Unit = RPCConfiguration.BDP_RPC_EUREKA_SERVICE_REFRESH_MAX_WAIT_TIME.getValue.toDuration

  override def getDWCServiceInstance(serviceInstance: SpringCloudServiceInstance): ServiceInstance = ???

}
