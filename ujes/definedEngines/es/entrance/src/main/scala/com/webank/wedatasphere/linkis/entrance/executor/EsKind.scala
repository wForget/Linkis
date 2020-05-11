package com.webank.wedatasphere.linkis.entrance.executor

/**
 *
 * @author wang_zh
 * @date 2020/5/6
 */
trait Kind

case class EsJsonKind() extends Kind {
  override def toString: String = ESJSON_TYPE
}

case class EsSqlKind() extends Kind {
  override def toString: String = ESSQL_TYPE
}

object EsKind {

  val ESJSON_TYPE = "esjson"
  val ESSQL_TYPE = "essql"

  def getKind(runType: String): Kind = runType match {
    case ESJSON_TYPE => EsJsonKind()
    case ESSQL_TYPE => EsSqlKind()
    case _ => None
  }

}
