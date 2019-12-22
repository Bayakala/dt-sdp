package sdp.cql.engine

import akka.NotUsed
import akka.stream.alpakka.cassandra.scaladsl._
import akka.stream.scaladsl._
import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import protobuf.bytes.Converter._
import sdp.logging.LogSupport
import sdp.result.DBOResult._

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.concurrent._

object CQLContext {
  // Consistency Levels
  type CONSISTENCY_LEVEL = Int
  val ANY: CONSISTENCY_LEVEL          =                                        0x0000
  val ONE: CONSISTENCY_LEVEL          =                                        0x0001
  val TWO: CONSISTENCY_LEVEL          =                                        0x0002
  val THREE: CONSISTENCY_LEVEL        =                                        0x0003
  val QUORUM : CONSISTENCY_LEVEL      =                                        0x0004
  val ALL: CONSISTENCY_LEVEL          =                                        0x0005
  val LOCAL_QUORUM: CONSISTENCY_LEVEL =                                        0x0006
  val EACH_QUORUM: CONSISTENCY_LEVEL  =                                        0x0007
  val LOCAL_ONE: CONSISTENCY_LEVEL    =                                        0x000A
  val LOCAL_SERIAL: CONSISTENCY_LEVEL =                                        0x000B
  val SERIAL: CONSISTENCY_LEVEL       =                                        0x000C

  def apply(): CQLUpdateContext = CQLUpdateContext(statements = Nil)

  def consistencyLevel: CONSISTENCY_LEVEL => ConsistencyLevel = consistency => {
    consistency match {
      case ALL => ConsistencyLevel.ALL
      case ONE => ConsistencyLevel.ONE
      case TWO => ConsistencyLevel.TWO
      case THREE => ConsistencyLevel.THREE
      case ANY => ConsistencyLevel.ANY
      case EACH_QUORUM => ConsistencyLevel.EACH_QUORUM
      case LOCAL_ONE => ConsistencyLevel.LOCAL_ONE
      case QUORUM => ConsistencyLevel.QUORUM
      case SERIAL => ConsistencyLevel.SERIAL
      case LOCAL_SERIAL => ConsistencyLevel.LOCAL_SERIAL

    }
  }

}
case class CQLQueryContext(
                            statement: String,
                            parameters: Seq[Object] = Nil,
                            consistency: Option[CQLContext.CONSISTENCY_LEVEL] = None,
                            fetchSize: Int = 100
                          ) { ctx =>
  def setConsistencyLevel(_consistency: CQLContext.CONSISTENCY_LEVEL): CQLQueryContext =
    ctx.copy(consistency = Some(_consistency))
  def setFetchSize(pageSize: Int): CQLQueryContext =
    ctx.copy(fetchSize = pageSize)
  def setParameters(param: Seq[Object]): CQLQueryContext =
    ctx.copy(parameters = param)

  def toProto = new sdp.grpc.services.ProtoCQLQuery(
    statement = this.statement,
    parameters = { if (this.parameters == Nil) None
    else Some(sdp.grpc.services.ProtoAny(marshal(this.parameters))) },
    consistency = this.consistency,
    fetchSize = this.fetchSize
  )
}
object CQLQueryContext {
  def apply[M](stmt: String, param: Seq[Object]): CQLQueryContext = new CQLQueryContext(statement = stmt, parameters = param)
  def fromProto(proto: sdp.grpc.services.ProtoCQLQuery) =
    new CQLQueryContext(
      statement = proto.statement,
      parameters =
        proto.parameters match {
          case None => Nil
          case Some(so) =>
            if (so.value == _root_.com.google.protobuf.ByteString.EMPTY)
              Nil
            else
              unmarshal[Seq[Object]](so.value)
        },
      consistency = proto.consistency,
      fetchSize = proto.fetchSize
    )
}

case class CQLUpdateContext(
                             statements: Seq[String],
                             parameters: Seq[Seq[Object]] = Nil,
                             consistency: Option[CQLContext.CONSISTENCY_LEVEL] = None,
                             batch: Boolean = false
                           ) extends LogSupport { ctx =>
  def setBatch(bat: Boolean) = ctx.copy(batch = bat)
  def setConsistencyLevel(_consistency: CQLContext.CONSISTENCY_LEVEL): CQLUpdateContext =
    ctx.copy(consistency = Some(_consistency))
  def setCommand(_statement: String, _parameters: Object*): CQLUpdateContext = {
    log.info(s"setCommand> setting: statement: ${_statement}, parameters: ${_parameters}")
    val nc = ctx.copy(statements = Seq(_statement), parameters = Seq(_parameters))
    log.info(s"setCommand> set: statements: ${nc.statements}, parameters: ${nc.parameters}")
    nc
  }
  def appendCommand(_statement: String, _parameters: Object*): CQLUpdateContext = {
    log.info(s"appendCommand> appending: statement: ${_statement}, parameters: ${_parameters}")
    val nc = ctx.copy(statements = ctx.statements :+ _statement,
      parameters = ctx.parameters ++ Seq(_parameters))
    log.info(s"appendCommand> appended: statements: ${nc.statements}, parameters: ${nc.parameters}")
    nc
  }
  def toProto = new sdp.grpc.services.ProtoCQLUpdate(
    statements = this.statements,
    parameters = { if (this.parameters == Nil) None
    else Some(sdp.grpc.services.ProtoAny(marshal(this.parameters))) },
    consistency = this.consistency,
    batch = Some(this.batch)
  )
}

object CQLUpdateContext {
  def fromProto(proto: sdp.grpc.services.ProtoCQLUpdate) =
    new CQLUpdateContext(
      statements = proto.statements,
      parameters =
        proto.parameters match {
          case None => Nil
          case Some(so) =>
            if (so.value == _root_.com.google.protobuf.ByteString.EMPTY)
              Nil
            else
              unmarshal[Seq[Seq[Object]]](so.value)
        },
      consistency = proto.consistency,
      batch = if(proto.batch == None) false else proto.batch.get
    )
}

object CQLEngine extends LogSupport {
  import CQLContext._
  import CQLHelpers._

  import scala.concurrent.Await
  import scala.concurrent.duration._

  def fetchResult[C[_] <: TraversableOnce[_],A](ctx: CQLQueryContext, pageSize: Int = 100
                                                ,extractor: Row => A)(
                                                 implicit session: Session, cbf: CanBuildFrom[Nothing, A, C[A]]): DBOResult[C[A]]= {

    val prepStmt = session.prepare(ctx.statement)

    var boundStmt =  prepStmt.bind()
    var params: Seq[Object] = Nil
    if (ctx.parameters != Nil) {
      try {
        params = processParameters(ctx.parameters)
        boundStmt = prepStmt.bind(params: _*)
      }
      catch {
        case e: Exception =>
          log.error(s"fetchResult> prepStmt.bind error: ${e.getMessage}")
          Left(new RuntimeException(s"fetchResult> prepStmt.bind Error: ${e.getMessage}"))
      }
    }
    log.info(s"fetchResult>  statement: ${prepStmt.getQueryString}, parameters: ${params}")

    try {
      ctx.consistency.foreach { consistency =>
        boundStmt.setConsistencyLevel(consistencyLevel(consistency))
      }

      val resultSet = session.execute(boundStmt.setFetchSize(pageSize))
      val rows = resultSet.asScala.view.map(extractor).to[C]
      valueToDBOResult(rows)
      /*
      val ores = if(rows.isEmpty) None else Some(rows)
      optionToDBOResult(ores: Option[C[A]]) */
    }
    catch {
      case e: Exception =>
        log.error(s"fetchResult> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"fetchResult> Error: ${e.getMessage}"))
    }
  }

  def fetchResultPage[C[_] <: TraversableOnce[_],A](ctx: CQLQueryContext, pageSize: Int = 100
                                                    ,extractor: Row => A)(
                                                     implicit session: Session, cbf: CanBuildFrom[Nothing, A, C[A]]): (ResultSet, C[A])= {

    val prepStmt = session.prepare(ctx.statement)

    var boundStmt =  prepStmt.bind()
    var params: Seq[Object] = Nil
    if (ctx.parameters != Nil) {
      params = processParameters(ctx.parameters)
      boundStmt = prepStmt.bind(params:_*)
    }
    log.info(s"fetchResultPage>  statement: ${prepStmt.getQueryString}, parameters: ${params}")

    ctx.consistency.foreach {consistency =>
      boundStmt.setConsistencyLevel(consistencyLevel(consistency))}

    val resultSet = session.execute(boundStmt.setFetchSize(pageSize))
    (resultSet,(resultSet.asScala.view.map(extractor)).to[C])
  }

  def fetchMorePages[C[_] <: TraversableOnce[_],A](resultSet: ResultSet, timeOut: Duration)(
    extractor: Row => A)(implicit cbf: CanBuildFrom[Nothing, A, C[A]]): (ResultSet,Option[C[A]]) =
    if (resultSet.isFullyFetched) {
      (resultSet, None)
    } else {
      try {
        val result = Await.result(resultSet.fetchMoreResults(), timeOut)
        (result, Some((result.asScala.view.map(extractor)).to[C]))
      } catch { case e: Throwable => (resultSet, None) }
    }

  def cqlExecute(ctx: CQLUpdateContext)(
    implicit session: Session, ec: ExecutionContext): DBOResult[Boolean] = {

    var invalidBat = false
    if ( ctx.batch ) {
      if (ctx.parameters == Nil)
        invalidBat = true
      else if (ctx.parameters.size < 2)
        invalidBat = true;
    }
    if (!ctx.batch || invalidBat) {
      if(invalidBat)
        log.warn(s"cqlExecute> batch update must have at least 2 sets of parameters! change to single-command.")

      if (ctx.statements.size == 1) {
        var param: Seq[Object] = Nil
        if (ctx.parameters != Nil) param =  ctx.parameters.head
        log.info(s"cqlExecute>  single-command: statement: ${ctx.statements.head} parameters: ${param}")
        cqlSingleUpdate(ctx.consistency, ctx.statements.head, param)
      }
      else {
        var params: Seq[Seq[Object]] = Nil
        if (ctx.parameters == Nil)
          params = Seq.fill(ctx.statements.length)(Nil)
        else {
          if (ctx.statements.size > ctx.parameters.size) {
            log.warn(s"cqlExecute> fewer parameters than statements! pad with 'Nil'.")
            val nils = Seq.fill(ctx.statements.size - ctx.parameters.size)(Nil)
            params = ctx.parameters ++ nils

          }
          else
            params = ctx.parameters
        }

        val commands: Seq[(String, Seq[Object])] = ctx.statements zip params
        log.info(s"cqlExecute>  multi-commands: ${commands}")
        /*
                //using sequence to flip List[Future[Boolean]] => Future[List[Boolean]]
                //therefore, make sure no command replies on prev command effect
                val lstCmds: List[Future[Boolean]] = commands.map { case (stmt,param) =>
                  cqlSingleUpdate(ctx.consistency, stmt, param)
                }.toList

                val futList = Future.sequence(lstCmds).map(_ => true)   //must map to execute
        */
        /*
                //using traverse to have some degree of parallelism = max(runtimes)
                //therefore, make sure no command replies on prev command effect
                val futList = Future.traverse(commands) { case (stmt,param)  =>
                  cqlSingleUpdate(ctx.consistency, stmt, param)
                }.map(_ => true)

                Await.result(futList, 3 seconds)
                Future.successful(true)
        */
        // run sync directly
        try {
          commands.foreach { case (stm, pars) =>
            cqlExecuteSync(ctx.consistency, stm, pars)
          }
          Right(true)
        }
        catch {
          case e: Exception =>
            log.error(s"cqlExecute> runtime error: ${e.getMessage}")
            Left(new RuntimeException(s"cqlExecute> Error: ${e.getMessage}"))
        }
      }
    }
    else
      cqlBatchUpdate(ctx)
  }
  def cqlSingleUpdate(cons: Option[CQLContext.CONSISTENCY_LEVEL],stmt: String, params: Seq[Object])(
    implicit session: Session, ec: ExecutionContext): DBOResult[Boolean] = {

    val prepStmt = session.prepare(stmt)

    var boundStmt = prepStmt.bind()
    var pars: Seq[Object] = Nil
    if (params != Nil) {
      try {
        pars = processParameters(params)
        boundStmt = prepStmt.bind(pars: _*)
      }
      catch {
        case e: Exception =>
          log.error(s"cqlSingleUpdate> prepStmt.bind error: ${e.getMessage}")
          Left(new RuntimeException(s"cqlSingleUpdate> prepStmt.bind Error: ${e.getMessage}"))
      }
    }
    log.info(s"cqlSingleUpdate>  statement: ${prepStmt.getQueryString}, parameters: ${pars}")

    try {
      cons.foreach { consistency =>
        boundStmt.setConsistencyLevel(consistencyLevel(consistency))
      }
      val res = session.execute(boundStmt) //executeAsync(boundStmt).map(_.wasApplied())
      Right(res.wasApplied())
    }
    catch {
      case e: Exception =>
        log.error(s"cqlExecute> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"cqlExecute> Error: ${e.getMessage}"))
    }
  }

  def cqlExecuteSync(cons: Option[CQLContext.CONSISTENCY_LEVEL],stmt: String, params: Seq[Object])(
    implicit session: Session, ec: ExecutionContext): Boolean = {

    val prepStmt = session.prepare(stmt)

    var boundStmt = prepStmt.bind()
    var pars: Seq[Object] = Nil
    if (params != Nil) {
      pars = processParameters(params)
      boundStmt = prepStmt.bind(pars: _*)
    }
    log.info(s"cqlExecuteSync>  statement: ${prepStmt.getQueryString}, parameters: ${pars}")

    cons.foreach { consistency =>
      boundStmt.setConsistencyLevel(consistencyLevel(consistency))
    }
    session.execute(boundStmt).wasApplied()

  }

  def cqlBatchUpdate(ctx: CQLUpdateContext)(
    implicit session: Session, ec: ExecutionContext): DBOResult[Boolean] = {
    var params: Seq[Seq[Object]] = Nil
    if (ctx.parameters == Nil)
      params = Seq.fill(ctx.statements.length)(Nil)
    else
      params = ctx.parameters
    log.info(s"cqlBatchUpdate>  statement: ${ctx.statements.head}, parameters: ${params}")

    val prepStmt = session.prepare(ctx.statements.head)

    var batch = new BatchStatement()
    try {
      params.foreach { p =>
        log.info(s"cqlBatchUpdate>  batch with raw parameter: ${p}")
        val pars = processParameters(p)
        log.info(s"cqlMultiUpdate>  batch with cooked parameters: ${pars}")
        batch.add(prepStmt.bind(pars: _*))
      }
      ctx.consistency.foreach { consistency =>
        batch.setConsistencyLevel(consistencyLevel(consistency))
      }
      val res = session.execute(batch) //session.executeAsync(batch).map(_.wasApplied())
      Right(res.wasApplied())
    }
    catch {
      case e: Exception =>
        log.error(s"cqlBatchUpdate> runtime error: ${e.getMessage}")
        Left(new RuntimeException(s"cqlBatchUpdate> Error: ${e.getMessage}"))
    }

  }

  def cassandraStream[A](ctx: CQLQueryContext,extractor: Row => A)
                        (implicit session: Session, ec: ExecutionContextExecutor): Source[A,NotUsed] = {

    val prepStmt = session.prepare(ctx.statement)
    var boundStmt =  prepStmt.bind()
    val params = processParameters(ctx.parameters)
    boundStmt = prepStmt.bind(params:_*)
    ctx.consistency.foreach {consistency =>
      boundStmt.setConsistencyLevel(consistencyLevel(consistency))}

    log.info(s"cassandraStream>  statement: ${prepStmt.getQueryString}, parameters: ${params}")
    CassandraSource(boundStmt.setFetchSize(ctx.fetchSize)).map(extractor)
  }

  case class CassandraActionStream[R](parallelism: Int = 1, processInOrder: Boolean = true,
                                      statement: String, prepareParams: R => Seq[Object],
                                      consistency: Option[CQLContext.CONSISTENCY_LEVEL] = None){ cas =>
    def setParallelism(parLevel: Int): CassandraActionStream[R] = cas.copy(parallelism=parLevel)
    def setProcessOrder(ordered: Boolean): CassandraActionStream[R] = cas.copy(processInOrder = ordered)
    def setConsistencyLevel(_consistency: CQLContext.CONSISTENCY_LEVEL): CassandraActionStream[R] =
      cas.copy(consistency = Some(_consistency))

    def perform(r: R)(implicit session: Session, ec: ExecutionContext) = {
      var prepStmt = session.prepare(statement)
      var boundStmt =  prepStmt.bind()
      val params = processParameters(prepareParams(r))
      boundStmt = prepStmt.bind(params: _*)
      consistency.foreach { cons =>
        boundStmt.setConsistencyLevel(CQLContext.consistencyLevel(cons))
      }
      log.info(s"CassandraActionStream.perform>  statement: ${prepStmt.getQueryString}, parameters: ${params}")
      session.executeAsync(boundStmt).map(_ => r)
    }

    def performOnRow(implicit session: Session, ec: ExecutionContext): Flow[R, R, NotUsed] =
      if (processInOrder)
        Flow[R].mapAsync(parallelism)(perform)
      else
        Flow[R].mapAsyncUnordered(parallelism)(perform)

    def unloggedBatch[K](statementBinder: (
      R, PreparedStatement) => BoundStatement,partitionKey: R => K)(
                          implicit session: Session, ec: ExecutionContext): Flow[R, R, NotUsed] = {
      val preparedStatement = session.prepare(statement)
      log.info(s"CassandraActionStream.unloggedBatch>  statement: ${preparedStatement.getQueryString}")
      CassandraFlow.createUnloggedBatchWithPassThrough[R, K](
        parallelism,
        preparedStatement,
        statementBinder,
        partitionKey)
    }

  }
  object CassandraActionStream {
    def apply[R](_statement: String, params: R => Seq[Object]): CassandraActionStream[R] =
      new CassandraActionStream[R]( statement=_statement, prepareParams = params)
  }


}

object CQLHelpers extends LogSupport {
  import java.io._
  import java.nio.ByteBuffer
  import java.nio.file._
  import java.time.Instant

  import akka.stream._
  import akka.stream.scaladsl._
  import com.datastax.driver.core.LocalDate
  import com.datastax.driver.extras.codecs.jdk8.InstantCodec

  implicit def listenableFutureToFuture[T](
                                            listenableFuture: ListenableFuture[T]): Future[T] = {
    val promise = Promise[T]()
    Futures.addCallback(listenableFuture, new FutureCallback[T] {
      def onFailure(error: Throwable): Unit = {
        promise.failure(error)
        ()
      }
      def onSuccess(result: T): Unit = {
        promise.success(result)
        ()
      }
    })
    promise.future
  }

  case class CQLDate(year: Int, month: Int, day: Int)
  case object CQLTodayDate
  case class CQLDateTime(year: Int, Month: Int,
                         day: Int, hour: Int, minute: Int, second: Int, millisec: Int = 0)
  case object CQLDateTimeNow

  def cqlGetDate(dateToConvert: java.util.Date): java.time.LocalDate =
    dateToConvert.toInstant()
      .atZone(java.time.ZoneId.systemDefault())
      .toLocalDate()

  def cqlGetTime(dateToConvert: java.util.Date): java.time.LocalTime =
    dateToConvert.toInstant()
      .atZone(java.time.ZoneId.systemDefault())
      .toLocalTime()

  def cqlGetTimestamp(dateToConvert: java.util.Date): java.time.LocalDateTime=
    new java.sql.Timestamp(
      dateToConvert.getTime()
    ).toLocalDateTime()

  def processParameters(params: Seq[Object]): Seq[Object] = {
    import java.time.{Clock, ZoneId}
    log.info(s"[processParameters] input: ${params}")
    val outParams = params.map { obj =>
      obj match {
        case CQLDate(yy, mm, dd) => LocalDate.fromYearMonthDay(yy, mm, dd)
        case CQLTodayDate =>
          val today = java.time.LocalDate.now()
          LocalDate.fromYearMonthDay(today.getYear, today.getMonth.getValue, today.getDayOfMonth)
        case CQLDateTimeNow => Instant.now(Clock.system(ZoneId.of("EST", ZoneId.SHORT_IDS)))
        case CQLDateTime(yy, mm, dd, hr, ms, sc, mi) =>
          Instant.parse(f"$yy%4d-$mm%2d-$dd%2dT$hr%2d:$ms%2d:$sc%2d$mi%3d")
        case p@_ => p
      }
    }
    log.info(s"[processParameters] output: ${params}")
    outParams
  }
  class ByteBufferInputStream(buf: ByteBuffer) extends InputStream {
    override def read: Int = {
      if (!buf.hasRemaining) return -1
      buf.get
    }

    override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
      val length: Int = Math.min(len, buf.remaining)
      buf.get(bytes, off, length)
      length
    }
  }
  object ByteBufferInputStream {
    def apply(buf: ByteBuffer): ByteBufferInputStream = {
      new ByteBufferInputStream(buf)
    }
  }
  class FixsizedByteBufferOutputStream(buf: ByteBuffer) extends OutputStream {

    override def write(b: Int): Unit = {
      buf.put(b.toByte)
    }

    override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
      buf.put(bytes, off, len)
    }
  }
  object FixsizedByteBufferOutputStream {
    def apply(buf: ByteBuffer) = new FixsizedByteBufferOutputStream(buf)
  }
  class ExpandingByteBufferOutputStream(var buf: ByteBuffer, onHeap: Boolean) extends OutputStream {

    private val increasing = ExpandingByteBufferOutputStream.DEFAULT_INCREASING_FACTOR

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      val position = buf.position
      val limit = buf.limit
      val newTotal: Long = position + len
      if(newTotal > limit){
        var capacity = (buf.capacity * increasing)
        while(capacity <= newTotal){
          capacity = (capacity*increasing)
        }
        increase(capacity.toInt)
      }

      buf.put(b, 0, len)
    }

    override def write(b: Int): Unit= {
      if (!buf.hasRemaining) increase((buf.capacity * increasing).toInt)
      buf.put(b.toByte)
    }
    protected def increase(newCapacity: Int): Unit = {
      buf.limit(buf.position)
      buf.rewind
      val newBuffer =
        if (onHeap) ByteBuffer.allocate(newCapacity)
        else  ByteBuffer.allocateDirect(newCapacity)
      newBuffer.put(buf)
      buf.clear
      buf = newBuffer
    }
    def size: Long = buf.position
    def capacity: Long = buf.capacity
    def byteBuffer: ByteBuffer = buf
  }
  object ExpandingByteBufferOutputStream {
    val DEFAULT_INCREASING_FACTOR = 1.5f
    def apply(size: Int, increasingBy: Float, onHeap: Boolean) = {
      if (increasingBy <= 1) throw new IllegalArgumentException("Increasing Factor must be greater than 1.0")
      val buffer: ByteBuffer =
        if (onHeap) ByteBuffer.allocate(size)
        else ByteBuffer.allocateDirect(size)
      new ExpandingByteBufferOutputStream(buffer,onHeap)
    }
    def apply(size: Int): ExpandingByteBufferOutputStream = {
      apply(size, ExpandingByteBufferOutputStream.DEFAULT_INCREASING_FACTOR, false)
    }

    def apply(size: Int, onHeap: Boolean): ExpandingByteBufferOutputStream = {
      apply(size, ExpandingByteBufferOutputStream.DEFAULT_INCREASING_FACTOR, onHeap)
    }

    def apply(size: Int, increasingBy: Float): ExpandingByteBufferOutputStream = {
      apply(size, increasingBy, false)
    }

  }
  def cqlFileToBytes(fileName: String): ByteBuffer = {
    val fis = new FileInputStream(fileName)
    val b = new Array[Byte](fis.available + 1)
    val length = b.length
    fis.read(b)
    ByteBuffer.wrap(b)
  }
  def cqlBytesToFile(bytes: ByteBuffer, fileName: String)(
    implicit mat: Materializer): Future[IOResult] = {
    val source = StreamConverters.fromInputStream(() => ByteBufferInputStream(bytes))
    source.runWith(FileIO.toPath(Paths.get(fileName)))
  }
  def cqlDateTimeString(date: java.util.Date, fmt: String): String = {
    val outputFormat = new java.text.SimpleDateFormat(fmt)
    outputFormat.format(date)
  }
  def useJava8DateTime(cluster: Cluster) = {
    //for jdk8 datetime format
    cluster.getConfiguration().getCodecRegistry()
      .register(InstantCodec.instance)
  }
}