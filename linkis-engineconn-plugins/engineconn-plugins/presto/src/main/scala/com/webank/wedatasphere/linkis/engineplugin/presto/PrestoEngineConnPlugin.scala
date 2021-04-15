package com.webank.wedatasphere.linkis.engineplugin.presto

import java.util

import com.webank.wedatasphere.linkis.engineplugin.presto.PrestoEngineConnPlugin._
import com.webank.wedatasphere.linkis.engineplugin.presto.builder.PrestoProcessEngineConnLaunchBuilder
import com.webank.wedatasphere.linkis.engineplugin.presto.factory.{PrestoEngineConnFactory, PrestoEngineConnResourceFactory}
import com.webank.wedatasphere.linkis.manager.engineplugin.common.EngineConnPlugin
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.EngineConnFactory
import com.webank.wedatasphere.linkis.manager.engineplugin.common.launch.EngineConnLaunchBuilder
import com.webank.wedatasphere.linkis.manager.engineplugin.common.resource.EngineResourceFactory
import com.webank.wedatasphere.linkis.manager.label.entity.Label

class PrestoEngineConnPlugin extends EngineConnPlugin {

  private val defaultLabels: util.List[Label[_]] = new util.ArrayList[Label[_]]()

  override def init(params: util.Map[String, Any]): Unit = {

  }

  override def getEngineResourceFactory: EngineResourceFactory = ENGINE_RESOURCE_FACTORY

  override def getEngineConnLaunchBuilder: EngineConnLaunchBuilder = ENGINE_LAUNCH_BUILDER

  override def getEngineConnFactory: EngineConnFactory = ENGINE_FACTORY

  override def getDefaultLabels: util.List[Label[_]] = defaultLabels

}

private object PrestoEngineConnPlugin {

  val ENGINE_LAUNCH_BUILDER = new PrestoProcessEngineConnLaunchBuilder

  val ENGINE_RESOURCE_FACTORY = new PrestoEngineConnResourceFactory

  val ENGINE_FACTORY = new PrestoEngineConnFactory

}
