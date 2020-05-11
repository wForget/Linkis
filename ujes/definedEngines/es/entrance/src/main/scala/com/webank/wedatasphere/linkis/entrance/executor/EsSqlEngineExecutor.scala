package com.webank.wedatasphere.linkis.entrance.executor

import com.webank.wedatasphere.linkis.entrance.executor.esclient.EsClient

/**
 *
 * @author wang_zh
 * @date 2020/5/11
 */
class EsSqlEngineExecutor(client: EsClient)  extends EsEngineExecutor {

  def kind: Kind = EsSqlKind()

}
