package com.webank.wedatasphere.linkis.entrance.executor

import com.webank.wedatasphere.linkis.entrance.executor.esclient.EsClient
import com.webank.wedatasphere.linkis.server.JMap

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
class EsJsonEngineExecutor(client: EsClient, properties: JMap[String, String]) extends EsEngineExecutor {

  def kind: Kind = EsJsonKind()

}
