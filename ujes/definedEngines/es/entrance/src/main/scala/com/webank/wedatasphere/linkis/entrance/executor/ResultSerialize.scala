package com.webank.wedatasphere.linkis.entrance.executor

/**
 *
 * @author wang_zh
 * @date 2020/5/12
 */
trait ResultSerialize {

  def serialize(contentBytes: Array[Byte], contentType: String, storePath: String, alias: String): Unit

}