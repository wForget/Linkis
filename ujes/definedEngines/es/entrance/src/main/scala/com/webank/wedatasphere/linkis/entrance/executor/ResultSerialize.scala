package com.webank.wedatasphere.linkis.entrance.executor

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.entrance.executor.impl.ResultSerializeImpl
import com.webank.wedatasphere.linkis.storage.domain._
import org.elasticsearch.client.Response

/**
 *
 * @author wang_zh
 * @date 2020/5/12
 */
trait ResultSerialize extends Logging {

  def serialize(response: Response, storePath: String, alias: String): Unit

}


object ResultSerialize {

  val RESULT_SERIALIZE = new ResultSerializeImpl()

  val jsonMapper = new ObjectMapper()
  val yamlMapper = new YAMLMapper()
  val cborMapper = new ObjectMapper(new CBORFactory())
  val smileMapper = new ObjectMapper(new SmileFactory())
  val csvMapper = new CsvMapper()

  def getNodeDataType(node: JsonNode): DataType = node.getNodeType match {
    case JsonNodeType.ARRAY => (ArrayType, node.as)
    case JsonNodeType.BINARY => BinaryType
    case JsonNodeType.BOOLEAN => BooleanType
    case JsonNodeType.NULL => NullType
    case JsonNodeType.NUMBER => DecimalType
    case JsonNodeType.OBJECT => StructType
    case JsonNodeType.POJO => StructType
    case JsonNodeType.STRING => StringType
    case JsonNodeType.MISSING => StringType
    case _ => StringType
  }

  def getNodeTypeByEsType(estype: String): DataType = estype.toLowerCase match {
    case "long" || "integer" || "short" || "byte" || "double" || "float" || "half_float" || "scaled_float" => DecimalType
    case "text" || "keyword" => StringType
    case "date" => DateType
    case "binary" => BinaryType
    case _ => StringType
  }

  def getNodeValue(node: JsonNode): Any = node.getNodeType match {
    case JsonNodeType.NUMBER => node.asDouble()
    case JsonNodeType.NULL => null
    case _ => node.asText().replaceAll("\n|\t", " ")
  }

}