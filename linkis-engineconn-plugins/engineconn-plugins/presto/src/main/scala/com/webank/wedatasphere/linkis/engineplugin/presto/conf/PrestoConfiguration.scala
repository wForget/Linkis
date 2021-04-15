package com.webank.wedatasphere.linkis.engineplugin.presto.conf

import com.webank.wedatasphere.linkis.common.conf.CommonVars

object PrestoConfiguration {

  val ENGINE_DEFAULT_LIMIT = CommonVars("wds.linkis.jdbc.default.limit", 5000)

}
