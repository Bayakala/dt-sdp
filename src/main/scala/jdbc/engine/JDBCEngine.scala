package sdp.jdbc.engine
import java.sql.PreparedStatement

import scala.collection.generic.CanBuildFrom
import akka.stream.scaladsl._
import scalikejdbc._
import scalikejdbc.streams._
import akka.NotUsed
import akka.stream._
import java.time._

import scala.concurrent.duration._
import protobuf.bytes.Converter._
import sdp.file.Streaming._

import scala.concurrent.ExecutionContextExecutor
import java.io.InputStream

import sdp.logging.LogSupport
import sdp.result.DBOResult._


object JDBCUpdateContext {
  type SQLTYPE = Int
  val SQL_EXEDDL= 1
  val SQL_UPDATE = 2
  val RETURN_GENERATED_KEYVALUE = true
  val RETURN_UPDATED_COUNT = false

  def fromProto(proto: sdp.grpc.services.ProtoJDBCUpdate) =
    new JDBCUpdateContext (
      dbName = Symbol(proto.dbName),
      statements = proto.statements,
      parameters =
        proto.parameters match {
          case None => Nil
          case Some(ssa) =>
            if (ssa.value == _root_.com.google.protobuf.ByteString.EMPTY)
              Nil
            else
              unmarshal[Seq[Seq[Any]]](ssa.value)
        },
      fetchSize = proto.fetchSize,
      queryTimeout = proto.queryTimeout,
      sqlType = proto.sqlType,
      batch = {
        if (proto.batch == None) false
        else proto.batch.get
      },
      returnGeneratedKey =
        proto.returnGeneratedKey match {
          case None => Nil
          case Some(oa) =>
            if (oa.value == _root_.com.google.protobuf.ByteString.EMPTY)
              Nil
            else
              unmarshal[Seq[Option[Any]]](oa.value)
        }
    )

}

case class JDBCQueryContext(
                                dbName: Symbol,
                                statement: String,
                                parameters: Seq[Any] = Nil,
                                fetchSize: Int = 100,
                                autoCommit: Boolean = false,
                                queryTimeout: Option[Int] = None) {
  def toProto = new sdp.grpc.services.ProtoJDBCQuery (
    dbName = this.dbName.toString.tail,
    statement = this.statement,
    parameters = { if (this.parameters == Nil) None
    else Some(sdp.grpc.services.ProtoAny(marshal(this.parameters))) },
    fetchSize = this.fetchSize,
    autoCommit = Some(this.autoCommit),
    queryTimeout = this.queryTimeout
  )
}

object JDBCQueryContext {
  def fromProto(proto: sdp.grpc.services.ProtoJDBCQuery) =
    new JDBCQueryContext(
      dbName = Symbol(proto.dbName),
      statement = proto.statement,
      parameters =
        proto.parameters match {
          case None => Nil
          case Some(sa) =>
            if (sa.value == _root_.com.google.protobuf.ByteString.EMPTY)
              Nil
            else
              unmarshal[Seq[Any]](sa.value)
        },
      fetchSize = proto.fetchSize,
      autoCommit = {
        if (proto.autoCommit == None) false
        else proto.autoCommit.get
      },
      queryTimeout = proto.queryTimeout
    )
}

case class JDBCUpdateContext (
                               dbName: Symbol,
                               statements: Seq[String] = Nil,
                               parameters: Seq[Seq[Any]] = Nil,
                               fetchSize: Int = 100,
                               queryTimeout: Option[Int] = None,
                               queryTags: Seq[String] = Nil,
                               sqlType: JDBCUpdateContext.SQLTYPE = JDBCUpdateContext.SQL_UPDATE,
                               batch: Boolean = false,
                               returnGeneratedKey: Seq[Option[Any]] = Nil,
                               // no return: None, return by index: Some(1), by name: Some("id")
                               preAction: Option[PreparedStatement => Unit] = None,
                               postAction: Option[PreparedStatement => Unit] = None)
  extends LogSupport {

  ctx =>

  def toProto = new sdp.grpc.services.ProtoJDBCUpdate(
    dbName = this.dbName.toString.tail,
    statements = this.statements,
    parameters = { if (this.parameters == Nil) None
    else Some(sdp.grpc.services.ProtoAny(marshal(this.parameters))) },
    fetchSize= this.fetchSize,
    queryTimeout = this.queryTimeout,
    sqlType = this.sqlType,
    batch = Some(this.batch),
    returnGeneratedKey = { if (this.returnGeneratedKey == Nil) None
    else Some(sdp.grpc.services.ProtoAny(marshal(this.returnGeneratedKey))) }
  )


  //helper functions

  def appendTag(tag: String): JDBCUpdateContext = ctx.copy(queryTags = ctx.queryTags :+ tag)

  def appendTags(tags: Seq[String]): JDBCUpdateContext = ctx.copy(queryTags = ctx.queryTags ++ tags)

  def setFetchSize(size: Int): JDBCUpdateContext = ctx.copy(fetchSize = size)

  def setQueryTimeout(time: Option[Int]): JDBCUpdateContext = ctx.copy(queryTimeout = time)

  def setPreAction(action: Option[PreparedStatement => Unit]): JDBCUpdateContext = {
    if (ctx.sqlType == JDBCUpdateContext.SQL_UPDATE &&
      !ctx.batch && ctx.statements.size == 1) {
      val nc = ctx.copy(preAction = action)
      log.info("setPreAction> set")
      nc
    }
    else {
      log.info("setPreAction> JDBCContex setting error: preAction not supported!")
      throw new IllegalStateException("JDBCContex setting error: preAction not supported!")
    }
  }

  def setPostAction(action: Option[PreparedStatement => Unit]): JDBCUpdateContext = {
    if (ctx.sqlType == JDBCUpdateContext.SQL_UPDATE &&
      !ctx.batch && ctx.statements.size == 1) {
      val nc = ctx.copy(postAction = action)
      log.info("setPostAction> set")
      nc
    }
    else {
      log.info("setPreAction> JDBCContex setting error: postAction not supported!")
      throw new IllegalStateException("JDBCContex setting error: postAction not supported!")
    }
  }

  def appendDDLCommand(_statement: String, _parameters: Any*): JDBCUpdateContext = {
    if (ctx.sqlType == JDBCUpdateContext.SQL_EXEDDL) {
      log.info(s"appendDDLCommand> appending: statement: ${_statement}, parameters: ${_parameters}")
      val nc = ctx.copy(
        statements = ctx.statements ++ Seq(_statement),
        parameters = ctx.parameters ++ Seq(Seq(_parameters))
      )
      log.info(s"appendDDLCommand> appended: statement: ${nc.statements}, parameters: ${nc.parameters}")
      nc
    } else {
      log.info(s"appendDDLCommand> JDBCContex setting error: option not supported!")
      throw new IllegalStateException("JDBCContex setting error: option not supported!")
    }
  }

  def appendUpdateCommand(_returnGeneratedKey: Boolean, _statement: String,_parameters: Any*): JDBCUpdateContext = {
    if (ctx.sqlType == JDBCUpdateContext.SQL_UPDATE && !ctx.batch) {
      log.info(s"appendUpdateCommand> appending: returnGeneratedKey: ${_returnGeneratedKey}, statement: ${_statement}, parameters: ${_parameters}")
      val nc = ctx.copy(
        statements = ctx.statements ++ Seq(_statement),
        parameters = ctx.parameters ++ Seq(_parameters),
        returnGeneratedKey = ctx.returnGeneratedKey ++ (if (_returnGeneratedKey) Seq(Some(1)) else Seq(None))
      )
      log.info(s"appendUpdateCommand> appended: statement: ${nc.statements}, parameters: ${nc.parameters}")
      nc
    } else {
      log.info(s"appendUpdateCommand> JDBCContex setting error: option not supported!")
      throw new IllegalStateException("JDBCContex setting error: option not supported!")
    }
  }

  def appendBatchParameters(_parameters: Any*): JDBCUpdateContext = {
    log.info(s"appendBatchParameters> appending:  parameters: ${_parameters}")
    if (ctx.sqlType != JDBCUpdateContext.SQL_UPDATE || !ctx.batch) {
      log.info(s"appendBatchParameters> JDBCContex setting error: batch parameters only supported for SQL_UPDATE and batch = true!")
      throw new IllegalStateException("JDBCContex setting error: batch parameters only supported for SQL_UPDATE and batch = true!")
    }
    var matchParams = true
    if (ctx.parameters != Nil)
      if (ctx.parameters.head.size != _parameters.size)
        matchParams = false
    if (matchParams) {
      val nc = ctx.copy(
        parameters = ctx.parameters ++ Seq(_parameters)
      )
      log.info(s"appendBatchParameters> appended: statement: ${nc.statements}, parameters: ${nc.parameters}")
      nc
    } else {
      log.info(s"appendBatchParameters> JDBCContex setting error: batch command parameters not match!")
      throw new IllegalStateException("JDBCContex setting error: batch command parameters not match!")
    }
  }


  def setBatchReturnGeneratedKeyOption(returnKey: Boolean): JDBCUpdateContext = {
    if (ctx.sqlType != JDBCUpdateContext.SQL_UPDATE || !ctx.batch)
      throw new IllegalStateException("JDBCContex setting error: only supported in batch update commands!")
    ctx.copy(
      returnGeneratedKey = if (returnKey) Seq(Some(1)) else Nil
    )
  }

  def setDDLCommand(_statement: String, _parameters: Any*): JDBCUpdateContext = {
    log.info(s"setDDLCommand> setting: statement: ${_statement}, parameters: ${_parameters}")
    val nc = ctx.copy(
      statements = Seq(_statement),
      parameters = Seq(_parameters),
      sqlType = JDBCUpdateContext.SQL_EXEDDL,
      batch = false
    )
    log.info(s"setDDLCommand> set: statement: ${nc.statements}, parameters: ${nc.parameters}")
    nc
  }

  def setUpdateCommand(_returnGeneratedKey: Boolean, _statement: String,_parameters: Any*): JDBCUpdateContext = {
    log.info(s"setUpdateCommand> setting: returnGeneratedKey: ${_returnGeneratedKey}, statement: ${_statement}, parameters: ${_parameters}")
    val nc = ctx.copy(
      statements = Seq(_statement),
      parameters = Seq(_parameters),
      returnGeneratedKey = if (_returnGeneratedKey) Seq(Some(1)) else Seq(None),
      sqlType = JDBCUpdateContext.SQL_UPDATE,
      batch = false
    )
    log.info(s"setUpdateCommand> set: statement: ${nc.statements}, parameters: ${nc.parameters}")
    nc
  }
  def setBatchCommand(_statement: String): JDBCUpdateContext = {
    log.info(s"setBatchCommand> appending: statement: ${_statement}")
    val nc = ctx.copy (
      statements = Seq(_statement),
      sqlType = JDBCUpdateContext.SQL_UPDATE,
      batch = true
    )
    log.info(s"setBatchCommand> set: statement: ${nc.statements}, parameters: ${nc.parameters}")
    nc
  }

}

object JDBCEngine extends LogSupport {
  import JDBCUpdateContext._

  type JDBCDate = LocalDate
  type JDBCDateTime = LocalDateTime
  type JDBCTime = LocalTime


  def jdbcSetDate(yyyy: Int, mm: Int, dd: Int) = LocalDate.of(yyyy,mm,dd)
  def jdbcSetTime(hh: Int, mm: Int, ss: Int, nn: Int) = LocalTime.of(hh,mm,ss,nn)
  def jdbcSetDateTime(date: JDBCDate, time: JDBCTime) =  LocalDateTime.of(date,time)
  def jdbcSetNow = LocalDateTime.now()

  def jdbcGetDate(sqlDate: java.sql.Date): java.time.LocalDate = sqlDate.toLocalDate
  def jdbcGetTime(sqlTime: java.sql.Time): java.time.LocalTime = sqlTime.toLocalTime
  def jdbcGetTimestamp(sqlTimestamp: java.sql.Timestamp): java.time.LocalDateTime =
    sqlTimestamp.toLocalDateTime


  type JDBCBlob = InputStream

  def fileToJDBCBlob(fileName: String, timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer) = FileToInputStream(fileName,timeOut)

  def jdbcBlobToFile(blob: JDBCBlob, fileName: String)(
    implicit mat: Materializer) =  InputStreamToFile(blob,fileName)


  private def noExtractor(message: String): WrappedResultSet => Nothing = { (rs: WrappedResultSet) =>
    throw new IllegalStateException(message)
  }

  def jdbcAkkaStream[A](ctx: JDBCQueryContext,extractor: WrappedResultSet => A)
                       (implicit ec: ExecutionContextExecutor): Source[A,NotUsed] = {
    val publisher: DatabasePublisher[A] = NamedDB(ctx.dbName) readOnlyStream {
      val rawSql = new SQLToCollectionImpl[A, NoExtractor](ctx.statement, ctx.parameters)(noExtractor(""))
      ctx.queryTimeout.foreach(rawSql.queryTimeout(_))
      val sql: SQL[A, HasExtractor] = rawSql.map(extractor)
      sql.iterator
        .withDBSessionForceAdjuster(session => {
          session.connection.setAutoCommit(ctx.autoCommit)
          session.fetchSize(ctx.fetchSize)
        })
    }
    log.info(s"jdbcAkkaStream> Source: db: ${ctx.dbName}, statement: ${ctx.statement}, parameters: ${ctx.parameters}")
    Source.fromPublisher[A](publisher)
  }


  def jdbcQueryResult[C[_] <: TraversableOnce[_], A](ctx: JDBCQueryContext,
                                                     extractor: WrappedResultSet => A)(
                                                      implicit cbf: CanBuildFrom[Nothing, A, C[A]]): DBOResult[C[A]] = {
    val rawSql = new SQLToCollectionImpl[A, NoExtractor](ctx.statement, ctx.parameters)(noExtractor(""))
    ctx.queryTimeout.foreach(rawSql.queryTimeout(_))
    rawSql.fetchSize(ctx.fetchSize)
    try {
      implicit val session = NamedAutoSession(ctx.dbName)
      log.info(s"jdbcQueryResult> Source: db: ${ctx.dbName}, statement: ${ctx.statement}, parameters: ${ctx.parameters}")
      val sql: SQL[A, HasExtractor] = rawSql.map(extractor)
      val res = sql.collection.apply[C]()
      //     optionToDBOResult((if(res.isEmpty) None else Some(res)): Option[C[A]])
      valueToDBOResult(res)
    } catch {
      case e: Exception =>
        log.error(s"jdbcQueryResult> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"dbcQueryResult> jdbcQueryResult> Error: ${e.getMessage}"))

    }

  }

  def jdbcExecuteDDL(ctx: JDBCUpdateContext)(implicit ec: ExecutionContextExecutor): DBOResult[String] = {
    if (ctx.sqlType != SQL_EXEDDL) {
      log.info(s"jdbcExecuteDDL> JDBCContex setting error: sqlType must be 'SQL_EXEDDL'!")
      Left(new IllegalStateException("jdbcExecuteDDL> JDBCContex setting error: sqlType must be 'SQL_EXEDDL'!"))
    }
    else {
      log.info(s"jdbcExecuteDDL> Source: db: ${ctx.dbName}, statement: ${ctx.statements}, parameters: ${ctx.parameters}")
      try {
        NamedDB(ctx.dbName) localTx { implicit session =>
          ctx.statements.foreach { stm =>
            val ddl = new SQLExecution(statement = stm, parameters = Nil)(
              before = WrappedResultSet => {})(
              after = WrappedResultSet => {})

            ddl.apply()
          }
          "SQL_EXEDDL executed succesfully."
        }
      }
      catch { case e: Exception =>
        log.error(s"jdbcExecuteDDL> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"jdbcExecuteDDL> Error: ${e.getMessage}"))
      }
    }
  }

  def jdbcBatchUpdate[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {
    if (ctx.statements == Nil) {
      log.info(s"jdbcBatchUpdate> JDBCContex setting error: statements empty!")
      Left(new IllegalStateException("jdbcBatchUpdate> JDBCContex setting error: statements empty!"))
    }
    if (ctx.sqlType != SQL_UPDATE) {
      log.info(s"jdbcBatchUpdate> JDBCContex setting error: sqlType must be 'SQL_UPDATE'!")
      Left(new IllegalStateException("jdbcBatchUpdate> JDBCContex setting error: sqlType must be 'SQL_UPDATE'!"))
    }
    else {
      if (ctx.batch) {
        if (noReturnKey(ctx)) {
          log.info(s"jdbcBatchUpdate> batch updating no return: db: ${ctx.dbName}, statement: ${ctx.statements}, parameters: ${ctx.parameters}")
          val usql = SQL(ctx.statements.head)
            .tags(ctx.queryTags: _*)
            .batch(ctx.parameters: _*)
          try {
            NamedDB(ctx.dbName) localTx { implicit session =>
              ctx.queryTimeout.foreach(session.queryTimeout(_))
              usql.apply[Seq]()
              Right(Seq.empty[Long].to[C])
            }
          }
          catch {
            case e: Exception =>
              log.error(s"jdbcBatchUpdate> runtime error: ${e.getMessage}")
              Left(new RuntimeException(s"jdbcBatchUpdate> Error: ${e.getMessage}"))
          }
        } else {
          log.info(s"jdbcBatchUpdate> batch updating return genkey: db: ${ctx.dbName}, statement: ${ctx.statements}, parameters: ${ctx.parameters}")
          val usql = new SQLBatchWithGeneratedKey(ctx.statements.head, ctx.parameters, ctx.queryTags)(None)
          try {
            NamedDB(ctx.dbName) localTx { implicit session =>
              ctx.queryTimeout.foreach(session.queryTimeout(_))
              val res = usql.apply[C]()
              Right(res)
            }
          }
          catch {
            case e: Exception =>
              log.error(s"jdbcBatchUpdate> runtime error: ${e.getMessage}")
              Left(new RuntimeException(s"jdbcBatchUpdate> Error: ${e.getMessage}"))
          }
        }

      } else {
        log.info(s"jdbcBatchUpdate> JDBCContex setting error: must set batch = true !")
        Left(new IllegalStateException("jdbcBatchUpdate> JDBCContex setting error: must set batch = true !"))
      }
    }
  }
  private def singleTxUpdateWithReturnKey[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {
    val Some(key) :: xs = ctx.returnGeneratedKey
    val params: Seq[Any] = ctx.parameters match {
      case Nil => Nil
      case p@_ => p.head
    }
    log.info(s"singleTxUpdateWithReturnKey> updating: db: ${ctx.dbName}, statement: ${ctx.statements}, parameters: ${ctx.parameters}")
    val usql = new SQLUpdateWithGeneratedKey(ctx.statements.head, params, ctx.queryTags)(key)
    try {
      NamedDB(ctx.dbName) localTx { implicit session =>
        session.fetchSize(ctx.fetchSize)
        ctx.queryTimeout.foreach(session.queryTimeout(_))
        val result = usql.apply()
        Right(Seq(result).to[C])
      }
    }
    catch {
      case e: Exception =>
        log.error(s"singleTxUpdateWithReturnKey> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"singleTxUpdateWithReturnKey> Error: ${e.getMessage}"))
    }
  }

  private def singleTxUpdateNoReturnKey[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {
    val params: Seq[Any] = ctx.parameters match {
      case Nil => Nil
      case p@_ => p.head
    }
    val before = ctx.preAction match {
      case None => pstm: PreparedStatement => {}
      case Some(f) => f
    }
    val after = ctx.postAction match {
      case None => pstm: PreparedStatement => {}
      case Some(f) => f
    }
    log.info(s"singleTxUpdateNoReturnKey> updating: db: ${ctx.dbName}, statement: ${ctx.statements}, parameters: ${ctx.parameters}")
    val usql = new SQLUpdate(ctx.statements.head,params,ctx.queryTags)(before)(after)
    try {
      NamedDB(ctx.dbName) localTx {implicit session =>
        session.fetchSize(ctx.fetchSize)
        ctx.queryTimeout.foreach(session.queryTimeout(_))
        val result = usql.apply()
        Right(Seq(result.toLong).to[C])
      }
    }
    catch {
      case e: Exception =>
        log.error(s"singleTxUpdateNoReturnKey> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"singleTxUpdateNoReturnKey> Error: ${e.getMessage}"))
    }
  }

  private def singleTxUpdate[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {
    if (noReturnKey(ctx))
      singleTxUpdateNoReturnKey(ctx)
    else
      singleTxUpdateWithReturnKey(ctx)
  }

  private def noReturnKey(ctx: JDBCUpdateContext): Boolean = {
    if (ctx.returnGeneratedKey != Nil) {
      val k :: xs = ctx.returnGeneratedKey
      k match {
        case None => true
        case Some(k) => false
      }
    } else true
  }

  def noActon: PreparedStatement=>Unit = pstm => {}

  def multiTxUpdates[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {

    val keys: Seq[Option[Any]] = ctx.returnGeneratedKey match {
      case Nil => Seq.fill(ctx.statements.size)(None)
      case k@_ => k
    }
    val sqlcmd = ctx.statements zip ctx.parameters zip keys
    log.info(s"multiTxUpdates> updating: db: ${ctx.dbName}, SQL Commands: ${sqlcmd}")
    try {
      NamedDB(ctx.dbName) localTx { implicit session =>
        session.fetchSize(ctx.fetchSize)
        ctx.queryTimeout.foreach(session.queryTimeout(_))
        val results = sqlcmd.map { case ((stm, param), key) =>
          key match {
            case None =>
              new SQLUpdate(stm, param, Nil)(noActon)(noActon).apply().toLong
            case Some(k) =>
              new SQLUpdateWithGeneratedKey(stm, param, Nil)(k).apply().toLong
          }
        }
        Right(results.to[C])
      }
    }
    catch {
      case e: Exception =>
        log.error(s"multiTxUpdates> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"multiTxUpdates> Error: ${e.getMessage}"))
    }
  }


  def jdbcTxUpdates[C[_] <: TraversableOnce[_]](ctx: JDBCUpdateContext)(
    implicit ec: ExecutionContextExecutor,
    cbf: CanBuildFrom[Nothing, Long, C[Long]]): DBOResult[C[Long]] = {
    if (ctx.statements == Nil) {
      log.info(s"jdbcTxUpdates> JDBCContex setting error: statements empty!")
      Left(new IllegalStateException("JjdbcTxUpdates> DBCContex setting error: statements empty!"))
    }
    if (ctx.sqlType != SQL_UPDATE) {
      log.info(s"jdbcTxUpdates> JDBCContex setting error: sqlType must be 'SQL_UPDATE'!")
      Left(new IllegalStateException("jdbcTxUpdates> JDBCContex setting error: sqlType must be 'SQL_UPDATE'!"))
    }
    else {
      if (!ctx.batch) {
        if (ctx.statements.size == 1)
          singleTxUpdate(ctx)
        else
          multiTxUpdates(ctx)
      } else {
        log.info(s"jdbcTxUpdates> JDBCContex setting error: must set batch = false !")
        Left(new IllegalStateException("jdbcTxUpdates> JDBCContex setting error: must set batch = false !"))
      }
    }
  }

  case class JDBCActionStream[R](dbName: Symbol, parallelism: Int = 1, processInOrder: Boolean = true,
                                 statement: String, prepareParams: R => Seq[Any]) extends LogSupport {
    jas =>
    def setDBName(db: Symbol): JDBCActionStream[R] = jas.copy(dbName = db)
    def setParallelism(parLevel: Int): JDBCActionStream[R] = jas.copy(parallelism = parLevel)
    def setProcessOrder(ordered: Boolean): JDBCActionStream[R] = jas.copy(processInOrder = ordered)

    private def perform(r: R)(implicit ec: ExecutionContextExecutor) = {
      import scala.concurrent._
      val params = prepareParams(r)
      log.info(s"JDBCActionStream.perform>  db: ${dbName}, statement: ${statement}, parameters: ${params}")
      Future {
        NamedDB(dbName) autoCommit { session =>
          session.execute(statement, params: _*)
        }
        r
      }
    }
    def performOnRow(implicit ec: ExecutionContextExecutor): Flow[R, R, NotUsed] =
      if (processInOrder)
        Flow[R].mapAsync(parallelism)(perform)
      else
        Flow[R].mapAsyncUnordered(parallelism)(perform)

  }

  object JDBCActionStream {
    def apply[R](_dbName: Symbol, _statement: String, params: R => Seq[Any]): JDBCActionStream[R] =
      new JDBCActionStream[R](dbName = _dbName, statement=_statement, prepareParams = params)
  }

}