package com.webank.wedatasphere.linkis.entrance.execute
import java.util
import java.util.concurrent.atomic.AtomicLong

import com.webank.wedatasphere.linkis.entrance.conf.EntranceConfiguration
import com.webank.wedatasphere.linkis.protocol.config.{RequestQueryGlobalConfig, ResponseQueryConfig}
import com.webank.wedatasphere.linkis.protocol.constants.TaskConstant
import com.webank.wedatasphere.linkis.protocol.engine.{RequestEngine, RequestNewEngine, TimeoutRequestNewEngine}
import com.webank.wedatasphere.linkis.protocol.utils.TaskUtils
import com.webank.wedatasphere.linkis.rpc.Sender
import com.webank.wedatasphere.linkis.scheduler.executer.ExecutorState
import com.webank.wedatasphere.linkis.scheduler.queue.Job
import com.webank.wedatasphere.linkis.server.JMap

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
class EsEngineRequester extends EngineRequester {

  private val idGenerator = new AtomicLong(0)

  override def request(job: Job): Option[EntranceEngine] = job match {
    case entranceJob: EntranceJob => {
      val requestEngine = createRequestEngine(job);

      // TODO request resource manager

      val engine = new EsEntranceEngine(idGenerator.incrementAndGet(), requestEngine.properties)
      engine.setGroup(groupFactory.getOrCreateGroup(getGroupName(requestEngine.creator, requestEngine.user)))
      engine.setUser(requestEngine.user)
      engine.setCreator(requestEngine.creator)
      engine.updateState(ExecutorState.Starting, ExecutorState.Idle, null, null)
      engine.setJob(job)
      engine.init()
      engine
    }
    case _ => null
  }

  override protected def createRequestEngine(job: Job): RequestEngine = job match {
    case entranceJob: EntranceJob =>
      val sender = Sender.getSender(EntranceConfiguration.CLOUD_CONSOLE_CONFIGURATION_SPRING_APPLICATION_NAME.getValue)
      val requestQueryGlobalConfig = RequestQueryGlobalConfig(entranceJob.getUser)
      val responseQueryGlobalConfig = sender.ask(requestQueryGlobalConfig).asInstanceOf[ResponseQueryConfig]
      val keyAndValue = responseQueryGlobalConfig.getKeyAndValue

      // TODO get datasoure config

      val properties = if(entranceJob.getParams == null) new util.HashMap[String, String]
      else {
        val startupMap = TaskUtils.getStartupMap(entranceJob.getParams)
        val runtimeMap = TaskUtils.getRuntimeMap(entranceJob.getParams)
        val properties = new JMap[String, String]
        startupMap.foreach {case (k, v) => if(v != null) properties.put(k, v.toString)}
        runtimeMap.foreach {case (k, v) => if(v != null) properties.put(k, v.toString)}
        properties
      }

      val runType:String = entranceJob.getParams.get(TaskConstant.RUNTYPE).asInstanceOf[String]
      properties.put(TaskConstant.RUNTYPE, runType)

      properties.put(RequestEngine.REQUEST_ENTRANCE_INSTANCE, Sender.getThisServiceInstance.getApplicationName + "," + Sender.getThisServiceInstance.getInstance)
      RequestNewEngine(entranceJob.getCreator, entranceJob.getUser, properties)
    case _ => null
  }

}
