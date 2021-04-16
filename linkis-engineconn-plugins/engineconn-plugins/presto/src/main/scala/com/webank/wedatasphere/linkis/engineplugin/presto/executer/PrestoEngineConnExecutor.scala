package com.webank.wedatasphere.linkis.engineplugin.presto.executer

import java.net.URI
import java.sql.SQLException
import java.util
import java.util.{Collections, Locale, Objects, Optional, TimeZone}
import java.util.concurrent.TimeUnit

import com.facebook.presto.client.{ClientSession, QueryStatusInfo, StatementClient, StatementClientFactory}
import com.facebook.presto.spi.security.SelectedRole
import com.google.common.cache.{Cache, CacheBuilder}
import com.webank.wedatasphere.linkis.common.io.FsPath
import com.webank.wedatasphere.linkis.common.log.LogUtils
import com.webank.wedatasphere.linkis.common.utils.Utils
import com.webank.wedatasphere.linkis.engineconn.common.conf.{EngineConnConf, EngineConnConstant}
import com.webank.wedatasphere.linkis.engineconn.computation.executor.entity.EngineConnTask
import com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.{ConcurrentComputationExecutor, EngineExecutionContext}
import com.webank.wedatasphere.linkis.engineconn.computation.executor.parser.SQLCodeParser
import com.webank.wedatasphere.linkis.engineplugin.presto.conf.PrestoConfiguration._
import com.webank.wedatasphere.linkis.manager.common.entity.resource.NodeResource
import com.webank.wedatasphere.linkis.manager.label.entity.Label
import com.webank.wedatasphere.linkis.manager.label.entity.engine.{EngineRunTypeLabel, UserCreatorLabel}
import com.webank.wedatasphere.linkis.protocol.engine.JobProgressInfo
import com.webank.wedatasphere.linkis.rpc.Sender
import com.webank.wedatasphere.linkis.scheduler.executer.{AliasOutputExecuteResponse, ExecuteResponse, SuccessExecuteResponse}
import com.webank.wedatasphere.linkis.storage.domain.Column
import com.webank.wedatasphere.linkis.storage.resultset.{ResultSetFactory, ResultSetWriter}
import com.webank.wedatasphere.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import okhttp3.OkHttpClient
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.springframework.util.CollectionUtils

import scala.collection.JavaConverters._

class PrestoEngineConnExecutor(override val outputPrintLimit: Int, val id: Int) extends ConcurrentComputationExecutor(outputPrintLimit) {

  private var okHttpClient: OkHttpClient = _

  private val executorLabels: util.List[Label[_]] = new util.ArrayList[Label[_]](2)

  private val clientSessionCache: Cache[String, ClientSession] = CacheBuilder.newBuilder()
    .expireAfterAccess(EngineConnConf.ENGINE_TASK_EXPIRE_TIME.getValue, TimeUnit.MILLISECONDS)
    .maximumSize(EngineConnConstant.MAX_TASK_NUM).build()

  override def init: Unit = {
    setCodeParser(new SQLCodeParser)
    super.init
  }

  override def execute(engineConnTask: EngineConnTask): ExecuteResponse = {
    val user = getUserCreatorLabel().getUser
    clientSessionCache.put(engineConnTask.getTaskId, getClientSession(user, engineConnTask.getProperties))
    super.execute(engineConnTask)
  }

  override def executeLine(engineExecutorContext: EngineExecutionContext, code: String): ExecuteResponse = {
    val realCode = code.trim
    info(s"presto client begins to run psql code:\n $realCode")

    val clientSession = clientSessionCache.getIfPresent(engineExecutorContext.getJobId)
    val statement = StatementClientFactory.newStatementClient(okHttpClient, clientSession, realCode)

//    initialStatusUpdates(statement)
//
//    var response: ExecuteResponse = SuccessExecuteResponse()
//    if (statement.isRunning || (statement.isFinished && statement.finalStatusInfo().getError == null)) {
//      response = queryOutput(statement, storePath, alias)
//    }
//
//    verifyServerError(statement)
//
//    updateSession(statement)

//    response

    // TODO
    ???
  }

  override def executeCompletely(engineExecutorContext: EngineExecutionContext, code: String, completedLine: String): ExecuteResponse = null

  override def progress(): Float = 0.0f

  override def getProgressInfo: Array[JobProgressInfo] = Array.empty[JobProgressInfo]

  override def getExecutorLabels(): util.List[Label[_]] = executorLabels

  override def setExecutorLabels(labels: util.List[Label[_]]): Unit = {
    if (!CollectionUtils.isEmpty(labels)) {
      executorLabels.clear()
      executorLabels.addAll(labels)
    }
  }

  override def supportCallBackLogs(): Boolean = false

  override def requestExpectedResource(expectedResource: NodeResource): NodeResource = {
    // TODO
    ???
  }

  override def getCurrentNodeResource(): NodeResource = {
    // TODO
    ???
  }

  override def getId(): String = Sender.getThisServiceInstance.getInstance + s"_$id"

  override def getConcurrentLimit: Int = ENGINE_CONCURRENT_LIMIT.getValue

  private def getClientSession(user: String, taskParams : util.Map[String, Object]): ClientSession = {
    val configMap = new util.HashMap[String, String]()
    taskParams.asScala.foreach {
      case (key: String, value: Object) if value != null => configMap.put(key, String.valueOf(value))
      case _ =>
    }
    val httpUri: URI = URI.create(PRESTO_URL.getValue(configMap))
    val source: String = PRESTO_SOURCE.getValue(configMap)
    val catalog: String = PRESTO_CATALOG.getValue(configMap)
    val schema: String = PRESTO_SCHEMA.getValue(configMap)

    val properties: util.Map[String, String] = configMap.asScala
      .filter(tuple => tuple._1.startsWith("presto.session."))
      .map(tuple => (tuple._1.substring("presto.session.".length), tuple._2))
      .asJava

    val clientInfo: String = "Linkis"
    val transactionId: String = null
    val traceToken: util.Optional[String] = Optional.empty()
    val clientTags: util.Set[String] = Collections.emptySet()
    val timeZonId = TimeZone.getDefault.getID
    val locale: Locale = Locale.getDefault
    val resourceEstimates: util.Map[String, String] = Collections.emptyMap()
    val preparedStatements: util.Map[String, String] = Collections.emptyMap()
    val roles: java.util.Map[String, SelectedRole] = Collections.emptyMap()
    val extraCredentials: util.Map[String, String] = Collections.emptyMap()
    //0不设限
    val clientRequestTimeout: io.airlift.units.Duration = new io.airlift.units.Duration(0, TimeUnit.MILLISECONDS)

    new ClientSession(httpUri, user, source, traceToken, clientTags, clientInfo, catalog, schema, timeZonId, locale,
      resourceEstimates, properties, preparedStatements, roles, extraCredentials, transactionId, clientRequestTimeout)
  }

  private def getUserCreatorLabel(): UserCreatorLabel = {
    this.getExecutorLabels().asScala
      .find(l => l.isInstanceOf[UserCreatorLabel])
      .get
      .asInstanceOf[UserCreatorLabel]
  }

//  def initialStatusUpdates(statement: StatementClient): Unit = {
//    while (statement.isRunning
//      && (statement.currentData().getData == null || statement.currentStatusInfo().getUpdateType != null)) {
//      job.getProgressListener.foreach(_.onProgressUpdate(job, progress(), getProgressInfo))
//      statement.advance()
//    }
//  }
//
//  private def queryOutput(statement: StatementClient, storePath: String, alias: String): AliasOutputExecuteResponse = {
//    var columnCount = 0
//    var rows = 0
//    val resultSet = ResultSetFactory.getInstance.getResultSetByType(ResultSetFactory.TABLE_TYPE)
//    val resultSetPath = resultSet.getResultSetPath(new FsPath(storePath), alias)
//    val resultSetWriter = ResultSetWriter.getResultSetWriter(resultSet, PrestoConfiguration.ENTRANCE_RESULTS_MAX_CACHE.getValue.toLong, resultSetPath, job.getUser)
//    Utils.tryFinally({
//      var results: QueryStatusInfo = null
//      if (statement.isRunning) {
//        results = statement.currentStatusInfo()
//      } else {
//        results = statement.finalStatusInfo()
//      }
//      if (results.getColumns == null) {
//        throw new RuntimeException("presto columns is null.")
//      }
//      val columns = results.getColumns.asScala
//        .map(column => Column(column.getName, column.getType, "")).toArray[Column]
//      columnCount = columns.length
//      resultSetWriter.addMetaData(new TableMetaData(columns))
//      while (statement.isRunning) {
//        val data = statement.currentData().getData
//        if (data != null) for (row <- data.asScala) {
//          val rowArray = row.asScala.map(r => String.valueOf(r))
//          resultSetWriter.addRecord(new TableRecord(rowArray.toArray))
//          rows += 1
//        }
//        job.getProgressListener.foreach(_.onProgressUpdate(job, progress(), getProgressInfo))
//        statement.advance()
//      }
//    })(IOUtils.closeQuietly(resultSetWriter))
//
//    info(s"Fetched $columnCount col(s) : $rows row(s) in presto")
//    job.getLogListener.foreach(_.onLogUpdate(job, LogUtils.generateInfo(s"Fetched $columnCount col(s) : $rows row(s) in presto")))
//    val output = if (resultSetWriter != null) resultSetWriter.toString else null
//    AliasOutputExecuteResponse(alias, output)
//  }
//
//  // check presto error
//  private def verifyServerError(statement: StatementClient): Unit = {
//    job.getProgressListener.foreach(_.onProgressUpdate(job, progress(), getProgressInfo))
//    if (statement.isFinished) {
//      val info: QueryStatusInfo = statement.finalStatusInfo()
//      if (info.getError != null) {
//        val error = Objects.requireNonNull(info.getError);
//        val message: String = s"Presto execute failed (#${info.getId}): ${error.getMessage}"
//        var cause: Throwable = null
//        if (error.getFailureInfo != null) {
//          cause = error.getFailureInfo.toException
//        }
//        throw new SQLException(message, error.getSqlState, error.getErrorCode, cause)
//      }
//    } else if (statement.isClientAborted) {
//      warn(s"Presto statement is killed by ${job.getUser}")
//    } else if (statement.isClientError) {
//      throw PrestoClientException("Presto client error.")
//    } else {
//      throw PrestoStateInvalidException("Presto status error. Statement is not finished.")
//    }
//  }
//
//  private def updateSession(statement: StatementClient): Unit = {
//    var newSession = clientSession.get()
//    // update catalog and schema if present
//    if (statement.getSetCatalog.isPresent || statement.getSetSchema.isPresent) {
//      newSession = ClientSession.builder(newSession)
//        .withCatalog(statement.getSetCatalog.orElse(newSession.getCatalog))
//        .withSchema(statement.getSetSchema.orElse(newSession.getSchema))
//        .build
//    }
//
//    // update transaction ID if necessary
//    if (statement.isClearTransactionId) newSession = ClientSession.stripTransactionId(newSession)
//
//    var builder: ClientSession.Builder = ClientSession.builder(newSession)
//
//    if (statement.getStartedTransactionId != null) builder = builder.withTransactionId(statement.getStartedTransactionId)
//
//    // update session properties if present
//    if (!statement.getSetSessionProperties.isEmpty || !statement.getResetSessionProperties.isEmpty) {
//      val sessionProperties: util.Map[String, String] = new util.HashMap[String, String](newSession.getProperties)
//      sessionProperties.putAll(statement.getSetSessionProperties)
//      sessionProperties.keySet.removeAll(statement.getResetSessionProperties)
//      builder = builder.withProperties(sessionProperties)
//    }
//
//    // update session roles
//    if (!statement.getSetRoles.isEmpty) {
//      val roles: util.Map[String, SelectedRole] = new util.HashMap[String, SelectedRole](newSession.getRoles)
//      roles.putAll(statement.getSetRoles)
//      builder = builder.withRoles(roles)
//    }
//
//    // update prepared statements if present
//    if (!statement.getAddedPreparedStatements.isEmpty || !statement.getDeallocatedPreparedStatements.isEmpty) {
//      val preparedStatements: util.Map[String, String] = new util.HashMap[String, String](newSession.getPreparedStatements)
//      preparedStatements.putAll(statement.getAddedPreparedStatements)
//      preparedStatements.keySet.removeAll(statement.getDeallocatedPreparedStatements)
//      builder = builder.withPreparedStatements(preparedStatements)
//    }
//
//    clientSession.set(newSession)
//  }


}
