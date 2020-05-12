package com.webank.wedatasphere.linkis.entrance.executor

import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.webank.wedatasphere.linkis.storage.domain.{ArrayType, _}
import com.webank.wedatasphere.linkis.storage.resultset.table.TableRecord

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 *
 * @author wang_zh
 * @date 2020/5/12
 */
class ResultSerializeImpl extends ResultSerialize {

  override def serialize(contentBytes: Array[Byte], contentType: String, storePath: String, alias: String): Unit = {
    val jsonNode = contentType match {
      case "application/yaml" =>
        ResultSerialize.yamlMapper.readTree(contentBytes)
      case "application/cbor" =>
        ResultSerialize.cborMapper.readTree(contentBytes)
      case "application/smile" =>
        ResultSerialize.smileMapper.readTree(contentBytes)
      case "application/json" =>
        ResultSerialize.jsonMapper.readTree(contentBytes)
      case _ => null
    }

    if (jsonNode == null) {
      // todo write to text
      return
    }

    var isTable = false
    val columns = ArrayBuffer(Column("_index", StringType, ""),
      Column("_type", StringType, ""),
      Column("_id", StringType, ""),
      Column("_score", DoubleType, ""))
    val records = new ArrayBuffer[TableRecord]

    // es json runType response
    jsonNode.at("/hits/hits") match {
      case hits if hits.isArray => {
        isTable = true
        hits.map {
          case obj if obj.isObject => {
            val lineValues = new ArrayBuffer(columns.size)[Any]
            obj.fields().foreach(entry => {
              val key = entry.getKey
              val value = entry.getValue
              val index = columns.indexWhere(_.columnName.endsWith(key))
              if (index < 0) {
                columns += Column(key, getNodeDataType(value), "")
                lineValues += getNodeValue(value)
              } else {
                lineValues.set(index, getNodeValue(value))
              }
            })
            records += new TableRecord(lineValues.toArray)
          }
          case _ =>
        }
      }.toArray
      case _ =>
    }

    // TODO es sql runType response

    // todo write result

  }

}

object ResultSerialize {

  val jsonMapper = new ObjectMapper()
  val yamlMapper = new ObjectMapper(new YAMLFactory())
  val cborMapper = new ObjectMapper(new CBORFactory())
  val smileMapper = new ObjectMapper(new SmileFactory())

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

  def getNodeValue(node: JsonNode): Any = node.getNodeType match {
    case JsonNodeType.NUMBER => node.asDouble()
    case JsonNodeType.NULL => null
    case _ => node.asText().replaceAll("\n|\t", " ")
  }

}
