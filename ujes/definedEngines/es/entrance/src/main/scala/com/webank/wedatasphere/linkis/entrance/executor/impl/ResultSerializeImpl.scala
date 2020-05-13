package com.webank.wedatasphere.linkis.entrance.executor.impl

import java.nio.charset.Charset

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.entrance.conf.EsEntranceConfiguration
import com.webank.wedatasphere.linkis.entrance.exception.EsConvertResponseException
import com.webank.wedatasphere.linkis.entrance.executor.ResultSerialize
import com.webank.wedatasphere.linkis.entrance.executor.ResultSerialize._
import com.webank.wedatasphere.linkis.storage.LineRecord
import com.webank.wedatasphere.linkis.storage.domain._
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import com.webank.wedatasphere.linkis.storage.resultset.{ResultSetFactory, ResultSetWriter}
import org.apache.http.entity.ContentType
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Response

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author wang_zh
 * @date 2020/5/12
 */
class ResultSerializeImpl extends ResultSerialize {

  override def serialize(response: Response, storePath: String, alias: String): Unit = {
    val contentType = ContentType.get(response.getEntity).getMimeType.toLowerCase
    val charSet = ContentType.get(response.getEntity).getCharset match {
      case c: Charset => c
      case _ => Charset.forName("UTF-8")
    }
    val contentBytes = EntityUtils.toByteArray(response.getEntity)

    if (contentBytes == null || contentBytes.isEmpty) {
      throw EsConvertResponseException("EsEngineExecutor convert response fail, response content is empty.")
    }

    val jsonNode = Utils.tryCatch{ contentType match {
        case "application/yaml" =>
          yamlMapper.readTree(contentBytes)
        case "application/cbor" =>
          cborMapper.readTree(contentBytes)
        case "application/smile" =>
          smileMapper.readTree(contentBytes)
//        case "text/csv" =>
//          csvMapper.readTree(contentBytes)
//        case "text/tab-separated-values" =>
//          csvMapper.readTree(contentBytes)
//          val schema = csvMapper.schemaFor(classOf[Array[Byte]]).withColumnSeparator('\t')
//          val reader = csvMapper.readerFor(classOf[Array[Byte]]).`with`(schema)
//          reader.readValue(contentBytes)
        case _ =>
          jsonMapper.readTree(contentBytes)
      }
    } {
      case t: Throwable => {
        warn("deserialize response content error, {}", t)
        null
      }
    }

    if (jsonNode == null) {
      writeText(new String(contentBytes, charSet), storePath, alias)
      return
    }

    var isTable = false
    val columns = ArrayBuffer()
    val records = new ArrayBuffer[TableRecord]

    // es json runType response
    jsonNode.at("/hits/hits") match {
      case hits: ArrayNode => {
        isTable = true
        columns += Column("_index", StringType, "")
        columns += Column("_type", StringType, "")
        columns += Column("_id", StringType, "")
        columns += Column("_score", DoubleType, "")
        hits.map {
          case obj: ObjectNode => {
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
      }
      case _ =>
    }

    // es sql runType response
    jsonNode.at("/rows") match {
      case rows: ArrayNode => {
        isTable = true
        jsonNode.get("columns").asInstanceOf[ArrayNode]
          .foreach(node => {
            val name = node.get("name").asText()
            val estype = node.get("type").asText().trim
            columns += Column(name, getNodeTypeByEsType(estype), "")
          })
        rows.map {
          case row: ArrayNode => {
            val lineValues = new ArrayBuffer()[Any]
            row.foreach(lineValues += getNodeValue(_))
            records += new TableRecord(lineValues.toArray)
          }
          case _ =>
        }
      }.toArray
      case _ =>
    }

    // write result
    if (isTable) {
      writeTable(new TableMetaData(columns), records, storePath, alias)
    } else {
      writeText(new String(contentBytes, charSet), storePath, alias)
    }
  }

  def writeText(content: String, storePath: String, alias: String): Unit = {
    val resultSet = ResultSetFactory.getInstance.getResultSetByType(ResultSetFactory.TEXT_TYPE)
    val resultSetPath = resultSet.getResultSetPath(new FsPath(storePath), alias)
    val writer = ResultSetWriter.getResultSetWriter(resultSet, EsEntranceConfiguration.ENGINE_RESULT_SET_MAX_CACHE.getValue.toLong, resultSetPath)
    writer.addMetaData(null)
    content.split("\\n").foreach(writer.addRecord(new LineRecord(_)))
    writer.close()
  }

  def writeTable(metaData: TableMetaData, records: ArrayBuffer[TableRecord], storePath: String, alias: String): Unit = {
    val resultSet = ResultSetFactory.getInstance.getResultSetByType(ResultSetFactory.TABLE_TYPE)
    val resultSetPath = resultSet.getResultSetPath(new FsPath(storePath), alias)
    val writer = ResultSetWriter.getResultSetWriter(resultSet, EsEntranceConfiguration.ENGINE_RESULT_SET_MAX_CACHE.getValue.toLong, resultSetPath)
    writer.addMetaData(metaData)
    records.foreach(writer.addRecord)
    writer.close()
  }

}
