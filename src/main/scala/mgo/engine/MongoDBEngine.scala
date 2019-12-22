package sdp.mongo.engine

import java.text.SimpleDateFormat
import java.util.Calendar

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.mongodb.scaladsl._
import akka.stream.scaladsl.{Flow, Source}
import org.bson.conversions.Bson
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.{BsonArray, BsonBinary}
import org.mongodb.scala.model._
import org.mongodb.scala.{MongoClient, _}
import protobuf.bytes.Converter._
import sdp.file.Streaming._
import sdp.logging.LogSupport

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._

object MGOClasses {
  type MGO_ACTION_TYPE = Int
  val MGO_QUERY        = 0
  val MGO_UPDATE       = 1
  val MGO_ADMIN        = 2

  /*  org.mongodb.scala.FindObservable
    import com.mongodb.async.client.FindIterable
    val resultDocType = FindIterable[Document]
    val resultOption = FindObservable(resultDocType)
      .maxScan(...)
    .limit(...)
    .sort(...)
    .project(...) */

  type FOD_TYPE       = Int
  val FOD_FIRST       = 0  //def first(): SingleObservable[TResult], return the first item
  val FOD_FILTER      = 1  //def filter(filter: Bson): FindObservable[TResult]
  val FOD_LIMIT       = 2  //def limit(limit: Int): FindObservable[TResult]
  val FOD_SKIP        = 3  //def skip(skip: Int): FindObservable[TResult]
  val FOD_PROJECTION  = 4  //def projection(projection: Bson): FindObservable[TResult]
  //Sets a document describing the fields to return for all matching documents
  val FOD_SORT        = 5  //def sort(sort: Bson): FindObservable[TResult]
  val FOD_PARTIAL     = 6  //def partial(partial: Boolean): FindObservable[TResult]
  //Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error)
  val FOD_CURSORTYPE  = 7  //def cursorType(cursorType: CursorType): FindObservable[TResult]
  //Sets the cursor type
  val FOD_HINT        = 8  //def hint(hint: Bson): FindObservable[TResult]
  //Sets the hint for which index to use. A null value means no hint is set
  val FOD_MAX         = 9  //def max(max: Bson): FindObservable[TResult]
  //Sets the exclusive upper bound for a specific index. A null value means no max is set
  val FOD_MIN         = 10 //def min(min: Bson): FindObservable[TResult]
  //Sets the minimum inclusive lower bound for a specific index. A null value means no max is set
  val FOD_RETURNKEY   = 11 //def returnKey(returnKey: Boolean): FindObservable[TResult]
  //Sets the returnKey. If true the find operation will return only the index keys in the resulting documents
  val FOD_SHOWRECORDID=12  //def showRecordId(showRecordId: Boolean): FindObservable[TResult]
  //Sets the showRecordId. Set to true to add a field `\$recordId` to the returned documents

  case class ResultOptions(
                            optType: FOD_TYPE,
                            bson: Option[Bson] = None,
                            value: Int = 0 ){
    def toProto = new sdp.grpc.services.ProtoMGOResultOption(
      optType = this.optType,
      bsonParam = this.bson.map {b => sdp.grpc.services.ProtoMGOBson(marshal(b))},
      valueParam = this.value
    )
    def toFindObservable: FindObservable[Document] => FindObservable[Document] = find => {
      optType match {
        case  FOD_FIRST        => find
        case  FOD_FILTER       => find.filter(bson.get)
        case  FOD_LIMIT        => find.limit(value)
        case  FOD_SKIP         => find.skip(value)
        case  FOD_PROJECTION   => find.projection(bson.get)
        case  FOD_SORT         => find.sort(bson.get)
        case  FOD_PARTIAL      => find.partial(value != 0)
        case  FOD_CURSORTYPE   => find
        case  FOD_HINT         => find.hint(bson.get)
        case  FOD_MAX          => find.max(bson.get)
        case  FOD_MIN          => find.min(bson.get)
        case  FOD_RETURNKEY    => find.returnKey(value != 0)
        case  FOD_SHOWRECORDID => find.showRecordId(value != 0)

      }
    }
  }
  object ResultOptions {
    def fromProto(msg: sdp.grpc.services.ProtoMGOResultOption) = new ResultOptions(
      optType = msg.optType,
      bson = msg.bsonParam.map(b => unmarshal[Bson](b.bson)),
      value = msg.valueParam
    )

  }

  trait MGOCommands

  object MGOCommands {

    case class Count(filter: Option[Bson] = None, options: Option[Any] = None) extends MGOCommands

    case class Distict(fieldName: String, filter: Option[Bson] = None) extends MGOCommands

    /*  org.mongodb.scala.FindObservable
    import com.mongodb.async.client.FindIterable
    val resultDocType = FindIterable[Document]
    val resultOption = FindObservable(resultDocType)
      .maxScan(...)
    .limit(...)
    .sort(...)
    .project(...) */
    case class Find(filter: Option[Bson] = None,
                    andThen: Seq[ResultOptions] = Seq.empty[ResultOptions],
                    firstOnly: Boolean = false) extends MGOCommands

    case class Aggregate(pipeLine: Seq[Bson]) extends MGOCommands

    case class MapReduce(mapFunction: String, reduceFunction: String) extends MGOCommands

    case class Insert(newdocs: Seq[Document], options: Option[Any] = None) extends MGOCommands

    case class Delete(filter: Bson, options: Option[Any] = None, onlyOne: Boolean = false) extends MGOCommands

    case class Replace(filter: Bson, replacement: Document, options: Option[Any] = None) extends MGOCommands

    case class Update(filter: Bson, update: Bson, options: Option[Any] = None, onlyOne: Boolean = false) extends MGOCommands


    case class BulkWrite(commands: List[WriteModel[Document]], options: Option[Any] = None) extends MGOCommands

  }

  object MGOAdmins {

    case class DropCollection(collName: String) extends MGOCommands

    case class CreateCollection(collName: String, options: Option[Any] = None) extends MGOCommands

    case class ListCollection(dbName: String) extends MGOCommands

    case class CreateView(viewName: String, viewOn: String, pipeline: Seq[Bson], options: Option[Any] = None) extends MGOCommands

    case class CreateIndex(key: Bson, options: Option[Any] = None) extends MGOCommands

    case class DropIndexByName(indexName: String, options: Option[Any] = None) extends MGOCommands

    case class DropIndexByKey(key: Bson, options: Option[Any] = None) extends MGOCommands

    case class DropAllIndexes(options: Option[Any] = None) extends MGOCommands

  }

  case class MGOContext(
                         dbName: String,
                         collName: String,
                         actionType: MGO_ACTION_TYPE = MGO_QUERY,
                         action: Option[MGOCommands] = None,
                         actionOptions: Option[Any] = None,
                         actionTargets: Seq[String] = Nil
                       ) {
    ctx =>
    def setDbName(name: String): MGOContext = ctx.copy(dbName = name)

    def setCollName(name: String): MGOContext = ctx.copy(collName = name)

    def setActionType(at: MGO_ACTION_TYPE): MGOContext = ctx.copy(actionType = at)

    def setCommand(cmd: MGOCommands): MGOContext  = ctx.copy(action = Some(cmd))

    def toSomeProto = MGOProtoConversion.ctxToProto(this)

  }

  object MGOContext {
    def apply(db: String, coll: String) = new MGOContext(db, coll)
    def fromProto(proto: sdp.grpc.services.ProtoMGOContext): MGOContext =
      MGOProtoConversion.ctxFromProto(proto)
  }

  case class MGOBatContext(contexts: Seq[MGOContext], tx: Boolean = false) {
    ctxs =>
    def setTx(txopt: Boolean): MGOBatContext = ctxs.copy(tx = txopt)
    def appendContext(ctx: MGOContext): MGOBatContext =
      ctxs.copy(contexts = contexts :+ ctx)
  }

  object MGOBatContext {
    def apply(ctxs: Seq[MGOContext], tx: Boolean = false) = new MGOBatContext(ctxs,tx)
  }

  type MGODate = java.util.Date
  def mgoDate(yyyy: Int, mm: Int, dd: Int): MGODate = {
    val ca = Calendar.getInstance()
    ca.set(yyyy,mm,dd)
    ca.getTime()
  }
  def mgoDateTime(yyyy: Int, mm: Int, dd: Int, hr: Int, min: Int, sec: Int): MGODate = {
    val ca = Calendar.getInstance()
    ca.set(yyyy,mm,dd,hr,min,sec)
    ca.getTime()
  }
  def mgoDateTimeNow: MGODate = {
    val ca = Calendar.getInstance()
    ca.getTime
  }


  def mgoDateToString(dt: MGODate, formatString: String): String = {
    val fmt= new SimpleDateFormat(formatString)
    fmt.format(dt)
  }

  type MGOBlob = BsonBinary
  type MGOArray = BsonArray

  def fileToMGOBlob(fileName: String, timeOut: FiniteDuration = 60 seconds)(
    implicit mat: Materializer) = FileToByteArray(fileName,timeOut)

  def mgoBlobToFile(blob: MGOBlob, fileName: String)(
    implicit mat: Materializer) =  ByteArrayToFile(blob.getData,fileName)

  def mgoGetStringOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getString(fieldName))
    else None
  }
  def mgoGetIntOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getInteger(fieldName))
    else None
  }
  def mgoGetLonggOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getLong(fieldName))
    else None
  }
  def mgoGetDoubleOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getDouble(fieldName))
    else None
  }
  def mgoGetBoolOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getBoolean(fieldName))
    else None
  }
  def mgoGetDateOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      Some(doc.getDate(fieldName))
    else None
  }
  def mgoGetBlobOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      doc.get(fieldName).asInstanceOf[Option[MGOBlob]]
    else None
  }
  def mgoGetArrayOrNone(doc: Document, fieldName: String) = {
    if (doc.keySet.contains(fieldName))
      doc.get(fieldName).asInstanceOf[Option[MGOArray]]
    else None
  }

  def mgoArrayToDocumentList(arr: MGOArray): scala.collection.immutable.List[org.bson.BsonDocument] = {
    (arr.getValues.asScala.toList)
      .asInstanceOf[scala.collection.immutable.List[org.bson.BsonDocument]]
  }

  type MGOFilterResult = FindObservable[Document] => FindObservable[Document]
}

object MGOEngine extends LogSupport {

  import MGOClasses._
  import MGOAdmins._
  import MGOCommands._
  import sdp.result.DBOResult._


  object TxUpdateMode {
    private def mgoTxUpdate(ctxs: MGOBatContext, observable: SingleObservable[ClientSession])(
      implicit client: MongoClient, ec: ExecutionContext): SingleObservable[ClientSession] = {
      log.info(s"mgoTxUpdate> calling ...")
      observable.map(clientSession => {

        val transactionOptions =
          TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY).build()

        clientSession.startTransaction(transactionOptions)
        /*
                val fut = Future.traverse(ctxs.contexts) { ctx =>
                  mgoUpdateObservable[Completed](ctx).map(identity).toFuture()
                }
                Await.ready(fut, 3 seconds) */

        ctxs.contexts.foreach { ctx =>
          mgoUpdateObservable[Completed](ctx).map(identity).toFuture()
        }
        clientSession
      })
    }

    private def commitAndRetry(observable: SingleObservable[Completed]): SingleObservable[Completed] = {
      log.info(s"commitAndRetry> calling ...")
      observable.recoverWith({
        case e: MongoException if e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL) => {
          log.warn("commitAndRetry> UnknownTransactionCommitResult, retrying commit operation ...")
          commitAndRetry(observable)
        }
        case e: Exception => {
          log.error(s"commitAndRetry> Exception during commit ...: $e")
          throw e
        }
      })
    }

    private def runTransactionAndRetry(observable: SingleObservable[Completed]): SingleObservable[Completed] = {
      log.info(s"runTransactionAndRetry> calling ...")
      observable.recoverWith({
        case e: MongoException if e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) => {
          log.warn("runTransactionAndRetry> TransientTransactionError, aborting transaction and retrying ...")
          runTransactionAndRetry(observable)
        }
      })
    }

    def mgoTxBatch(ctxs: MGOBatContext)(
      implicit client: MongoClient, ec: ExecutionContext): DBOResult[Completed] = {

      log.info(s"mgoTxBatch>  MGOBatContext: ${ctxs}")

      val updateObservable: Observable[ClientSession] = mgoTxUpdate(ctxs, client.startSession())
      val commitTransactionObservable: SingleObservable[Completed] =
        updateObservable.flatMap(clientSession => clientSession.commitTransaction())
      val commitAndRetryObservable: SingleObservable[Completed] = commitAndRetry(commitTransactionObservable)

      runTransactionAndRetry(commitAndRetryObservable)

      valueToDBOResult(Completed())

    }
  }


  def mgoUpdateBatch(ctxs: MGOBatContext)(implicit client: MongoClient, ec: ExecutionContext): DBOResult[Completed] = {
    log.info(s"mgoUpdateBatch>  MGOBatContext: ${ctxs}")
    if (ctxs.tx) {
      TxUpdateMode.mgoTxBatch(ctxs)
    } else {
      /*
              val fut = Future.traverse(ctxs.contexts) { ctx =>
                mgoUpdate[Completed](ctx).map(identity) }

              Await.ready(fut, 3 seconds)
              Future.successful(new Completed) */
      ctxs.contexts.foreach { ctx =>
        mgoUpdate[Completed](ctx).map(identity) }

      valueToDBOResult(Completed())
    }

  }

  def mongoStream(ctx: MGOContext)(
    implicit client: MongoClient, ec: ExecutionContextExecutor): Source[Document, NotUsed] = {
    log.info(s"mongoStream>  MGOContext: ${ctx}")

    def toResultOption(rts: Seq[ResultOptions]): FindObservable[Document] => FindObservable[Document] = findObj =>
      rts.foldRight(findObj)((a,b) => a.toFindObservable(b))

    val db = client.getDatabase(ctx.dbName)
    val coll = db.getCollection(ctx.collName)
    if ( ctx.action == None) {
      log.error(s"mongoStream> uery action cannot be null!")
      throw new IllegalArgumentException("query action cannot be null!")
    }
    try {
      ctx.action.get match {
        case Find(None, Nil, false) => //FindObservable
          MongoSource(coll.find())
        case Find(None, Nil, true) => //FindObservable
          MongoSource(coll.find().first())
        case Find(Some(filter), Nil, false) => //FindObservable
          MongoSource(coll.find(filter))
        case Find(Some(filter), Nil, true) => //FindObservable
          MongoSource(coll.find(filter).first())
        case Find(None, sro, _) => //FindObservable
          val next = toResultOption(sro)
          MongoSource(next(coll.find[Document]()))
        case Find(Some(filter), sro, _) => //FindObservable
          val next = toResultOption(sro)
          MongoSource(next(coll.find[Document](filter)))
        case _ =>
          log.error(s"mongoStream> unsupported streaming query [${ctx.action.get}]")
          throw new RuntimeException(s"mongoStream> unsupported streaming query [${ctx.action.get}]")

      }
    }
    catch { case e: Exception =>
      log.error(s"mongoStream> runtime error: ${e.getMessage}")
      throw new RuntimeException(s"mongoStream> Error: ${e.getMessage}")
    }

  }


  // T => FindIterable  e.g List[Document]
  def mgoQuery[T](ctx: MGOContext, Converter: Option[Document => Any] = None)(implicit client: MongoClient): DBOResult[T] = {
    log.info(s"mgoQuery>  MGOContext: ${ctx}")

    val db = client.getDatabase(ctx.dbName)
    val coll = db.getCollection(ctx.collName)

    def toResultOption(rts: Seq[ResultOptions]): FindObservable[Document] => FindObservable[Document] = findObj =>
      rts.foldRight(findObj)((a,b) => a.toFindObservable(b))


    if ( ctx.action == None) {
      log.error(s"mgoQuery> uery action cannot be null!")
      Left(new IllegalArgumentException("query action cannot be null!"))
    }
    try {
      ctx.action.get match {
        /* count */
        case Count(Some(filter), Some(opt)) => //SingleObservable
          coll.countDocuments(filter, opt.asInstanceOf[CountOptions])
            .toFuture().asInstanceOf[Future[T]]
        case Count(Some(filter), None) => //SingleObservable
          coll.countDocuments(filter).toFuture()
            .asInstanceOf[Future[T]]
        case Count(None, None) => //SingleObservable
          coll.countDocuments().toFuture()
            .asInstanceOf[Future[T]]
        /* distinct */
        case Distict(field, Some(filter)) => //DistinctObservable
          coll.distinct(field, filter).toFuture()
            .asInstanceOf[Future[T]]
        case Distict(field, None) => //DistinctObservable
          coll.distinct((field)).toFuture()
            .asInstanceOf[Future[T]]
        /* find */
        case Find(None, Nil, false) => //FindObservable
          if (Converter == None) coll.find().toFuture().asInstanceOf[Future[T]]
          else coll.find().map(Converter.get).toFuture().asInstanceOf[Future[T]]
        case Find(None, Nil, true) => //FindObservable
          if (Converter == None) coll.find().first().head().asInstanceOf[Future[T]]
          else coll.find().first().map(Converter.get).head().asInstanceOf[Future[T]]
        case Find(Some(filter), Nil, false) => //FindObservable
          if (Converter == None) coll.find(filter).toFuture().asInstanceOf[Future[T]]
          else coll.find(filter).map(Converter.get).toFuture().asInstanceOf[Future[T]]
        case Find(Some(filter), Nil, true) => //FindObservable
          if (Converter == None) coll.find(filter).first().head().asInstanceOf[Future[T]]
          else coll.find(filter).first().map(Converter.get).head().asInstanceOf[Future[T]]
        case Find(None, sro, _) => //FindObservable
          val next = toResultOption(sro)
          if (Converter == None) next(coll.find[Document]()).toFuture().asInstanceOf[Future[T]]
          else next(coll.find[Document]()).map(Converter.get).toFuture().asInstanceOf[Future[T]]
        case Find(Some(filter), sro, _) => //FindObservable
          val next = toResultOption(sro)
          if (Converter == None) next(coll.find[Document](filter)).toFuture().asInstanceOf[Future[T]]
          else next(coll.find[Document](filter)).map(Converter.get).toFuture().asInstanceOf[Future[T]]
        /* aggregate AggregateObservable*/
        case Aggregate(pline) => coll.aggregate(pline).toFuture().asInstanceOf[Future[T]]
        /* mapReduce MapReduceObservable*/
        case MapReduce(mf, rf) => coll.mapReduce(mf, rf).toFuture().asInstanceOf[Future[T]]
        /* list collection */
        case ListCollection(dbName) => //ListConllectionObservable
          client.getDatabase(dbName).listCollections().toFuture().asInstanceOf[Future[T]]

      }
    }
    catch { case e: Exception =>
      log.error(s"mgoQuery> runtime error: ${e.getMessage}")
      Left(new RuntimeException(s"mgoQuery> Error: ${e.getMessage}"))
    }
  }
  //T => Completed, result.UpdateResult, result.DeleteResult
  def mgoUpdate[T](ctx: MGOContext)(implicit client: MongoClient): DBOResult[T] =
    try {
      mgoUpdateObservable[T](ctx).toFuture()
    }
    catch { case e: Exception =>
      log.error(s"mgoUpdate> runtime error: ${e.getMessage}")
      Left(new RuntimeException(s"mgoUpdate> Error: ${e.getMessage}"))
    }

  def mgoUpdateObservable[T](ctx: MGOContext)(implicit client: MongoClient): SingleObservable[T] = {
    log.info(s"mgoUpdateObservable>  MGOContext: ${ctx}")

    val db = client.getDatabase(ctx.dbName)
    val coll = db.getCollection(ctx.collName)
    if ( ctx.action == None) {
      log.error(s"mgoUpdateObservable> uery action cannot be null!")
      throw new IllegalArgumentException("mgoUpdateObservable> query action cannot be null!")
    }
    try {
      ctx.action.get match {
        /* insert */
        case Insert(docs, Some(opt)) => //SingleObservable[Completed]
          if (docs.size > 1)
            coll.insertMany(docs, opt.asInstanceOf[InsertManyOptions]).asInstanceOf[SingleObservable[T]]
          else coll.insertOne(docs.head, opt.asInstanceOf[InsertOneOptions]).asInstanceOf[SingleObservable[T]]
        case Insert(docs, None) => //SingleObservable
          if (docs.size > 1) coll.insertMany(docs).asInstanceOf[SingleObservable[T]]
          else coll.insertOne(docs.head).asInstanceOf[SingleObservable[T]]
        /* delete */
        case Delete(filter, None, onlyOne) => //SingleObservable
          if (onlyOne) coll.deleteOne(filter).asInstanceOf[SingleObservable[T]]
          else coll.deleteMany(filter).asInstanceOf[SingleObservable[T]]
        case Delete(filter, Some(opt), onlyOne) => //SingleObservable
          if (onlyOne) coll.deleteOne(filter, opt.asInstanceOf[DeleteOptions]).asInstanceOf[SingleObservable[T]]
          else coll.deleteMany(filter, opt.asInstanceOf[DeleteOptions]).asInstanceOf[SingleObservable[T]]
        /* replace */
        case Replace(filter, replacement, None) => //SingleObservable
          coll.replaceOne(filter, replacement).asInstanceOf[SingleObservable[T]]
        case Replace(filter, replacement, Some(opt)) => //SingleObservable
          coll.replaceOne(filter, replacement, opt.asInstanceOf[ReplaceOptions]).asInstanceOf[SingleObservable[T]]
        /* update */
        case Update(filter, update, None, onlyOne) => //SingleObservable
          if (onlyOne) coll.updateOne(filter, update).asInstanceOf[SingleObservable[T]]
          else coll.updateMany(filter, update).asInstanceOf[SingleObservable[T]]
        case Update(filter, update, Some(opt), onlyOne) => //SingleObservable
          if (onlyOne) coll.updateOne(filter, update, opt.asInstanceOf[UpdateOptions]).asInstanceOf[SingleObservable[T]]
          else coll.updateMany(filter, update, opt.asInstanceOf[UpdateOptions]).asInstanceOf[SingleObservable[T]]
        /* bulkWrite */
        case BulkWrite(commands, None) => //SingleObservable
          coll.bulkWrite(commands).asInstanceOf[SingleObservable[T]]
        case BulkWrite(commands, Some(opt)) => //SingleObservable
          coll.bulkWrite(commands, opt.asInstanceOf[BulkWriteOptions]).asInstanceOf[SingleObservable[T]]
      }
    }
    catch { case e: Exception =>
      log.error(s"mgoUpdateObservable> runtime error: ${e.getMessage}")
      throw new RuntimeException(s"mgoUpdateObservable> Error: ${e.getMessage}")
    }
  }

  def mgoAdmin(ctx: MGOContext)(implicit client: MongoClient): DBOResult[Completed] = {
    log.info(s"mgoAdmin>  MGOContext: ${ctx}")

    val db = client.getDatabase(ctx.dbName)
    val coll = db.getCollection(ctx.collName)
    if ( ctx.action == None) {
      log.error(s"mgoAdmin> uery action cannot be null!")
      Left(new IllegalArgumentException("mgoAdmin> query action cannot be null!"))
    }
    try {
      ctx.action.get match {
        /* drop collection */
        case DropCollection(collName) => //SingleObservable
          val coll = db.getCollection(collName)
          coll.drop().toFuture()
        /* create collection */
        case CreateCollection(collName, None) => //SingleObservable
          db.createCollection(collName).toFuture()
        case CreateCollection(collName, Some(opt)) => //SingleObservable
          db.createCollection(collName, opt.asInstanceOf[CreateCollectionOptions]).toFuture()
        /* list collection
      case ListCollection(dbName) =>   //ListConllectionObservable
        client.getDatabase(dbName).listCollections().toFuture().asInstanceOf[Future[T]]
        */
        /* create view */
        case CreateView(viewName, viewOn, pline, None) => //SingleObservable
          db.createView(viewName, viewOn, pline).toFuture()
        case CreateView(viewName, viewOn, pline, Some(opt)) => //SingleObservable
          db.createView(viewName, viewOn, pline, opt.asInstanceOf[CreateViewOptions]).toFuture()
        /* create index */
        case CreateIndex(key, None) => //SingleObservable
          coll.createIndex(key).toFuture().asInstanceOf[Future[Completed]] //   asInstanceOf[SingleObservable[Completed]]
        case CreateIndex(key, Some(opt)) => //SingleObservable
          coll.createIndex(key, opt.asInstanceOf[IndexOptions]).asInstanceOf[Future[Completed]] // asInstanceOf[SingleObservable[Completed]]
        /* drop index */
        case DropIndexByName(indexName, None) => //SingleObservable
          coll.dropIndex(indexName).toFuture()
        case DropIndexByName(indexName, Some(opt)) => //SingleObservable
          coll.dropIndex(indexName, opt.asInstanceOf[DropIndexOptions]).toFuture()
        case DropIndexByKey(key, None) => //SingleObservable
          coll.dropIndex(key).toFuture()
        case DropIndexByKey(key, Some(opt)) => //SingleObservable
          coll.dropIndex(key, opt.asInstanceOf[DropIndexOptions]).toFuture()
        case DropAllIndexes(None) => //SingleObservable
          coll.dropIndexes().toFuture()
        case DropAllIndexes(Some(opt)) => //SingleObservable
          coll.dropIndexes(opt.asInstanceOf[DropIndexOptions]).toFuture()
      }
    }
    catch { case e: Exception =>
      log.error(s"mgoAdmin> runtime error: ${e.getMessage}")
      throw new RuntimeException(s"mgoAdmin> Error: ${e.getMessage}")
    }

  }

  /*
      def mgoExecute[T](ctx: MGOContext)(implicit client: MongoClient): Future[T] = {
      val db = client.getDatabase(ctx.dbName)
      val coll = db.getCollection(ctx.collName)
      ctx.action match {
        /* count */
        case Count(Some(filter), Some(opt)) =>   //SingleObservable
          coll.countDocuments(filter, opt.asInstanceOf[CountOptions])
            .toFuture().asInstanceOf[Future[T]]
        case Count(Some(filter), None) =>        //SingleObservable
          coll.countDocuments(filter).toFuture()
            .asInstanceOf[Future[T]]
        case Count(None, None) =>                //SingleObservable
          coll.countDocuments().toFuture()
            .asInstanceOf[Future[T]]
        /* distinct */
        case Distict(field, Some(filter)) =>     //DistinctObservable
          coll.distinct(field, filter).toFuture()
            .asInstanceOf[Future[T]]
        case Distict(field, None) =>             //DistinctObservable
          coll.distinct((field)).toFuture()
            .asInstanceOf[Future[T]]
        /* find */
        case Find(None, None, optConv, false) =>  //FindObservable
          if (optConv == None) coll.find().toFuture().asInstanceOf[Future[T]]
          else coll.find().map(optConv.get).toFuture().asInstanceOf[Future[T]]
        case Find(None, None, optConv, true) =>   //FindObservable
          if (optConv == None) coll.find().first().head().asInstanceOf[Future[T]]
          else coll.find().first().map(optConv.get).head().asInstanceOf[Future[T]]
        case Find(Some(filter), None, optConv, false) =>   //FindObservable
          if (optConv == None) coll.find(filter).toFuture().asInstanceOf[Future[T]]
          else coll.find(filter).map(optConv.get).toFuture().asInstanceOf[Future[T]]
        case Find(Some(filter), None, optConv, true) =>   //FindObservable
          if (optConv == None) coll.find(filter).first().head().asInstanceOf[Future[T]]
          else coll.find(filter).first().map(optConv.get).head().asInstanceOf[Future[T]]
        case Find(None, Some(next), optConv, _) =>   //FindObservable
          if (optConv == None) next(coll.find[Document]()).toFuture().asInstanceOf[Future[T]]
          else next(coll.find[Document]()).map(optConv.get).toFuture().asInstanceOf[Future[T]]
        case Find(Some(filter), Some(next), optConv, _) =>  //FindObservable
          if (optConv == None) next(coll.find[Document](filter)).toFuture().asInstanceOf[Future[T]]
          else next(coll.find[Document](filter)).map(optConv.get).toFuture().asInstanceOf[Future[T]]
        /* aggregate AggregateObservable*/
        case Aggregate(pline) => coll.aggregate(pline).toFuture().asInstanceOf[Future[T]]
        /* mapReduce MapReduceObservable*/
        case MapReduce(mf, rf) => coll.mapReduce(mf, rf).toFuture().asInstanceOf[Future[T]]
        /* insert */
        case Insert(docs, Some(opt)) =>                  //SingleObservable[Completed]
          if (docs.size > 1) coll.insertMany(docs, opt.asInstanceOf[InsertManyOptions]).toFuture()
            .asInstanceOf[Future[T]]
          else coll.insertOne(docs.head, opt.asInstanceOf[InsertOneOptions]).toFuture()
            .asInstanceOf[Future[T]]
        case Insert(docs, None) =>                       //SingleObservable
          if (docs.size > 1) coll.insertMany(docs).toFuture().asInstanceOf[Future[T]]
          else coll.insertOne(docs.head).toFuture().asInstanceOf[Future[T]]
        /* delete */
        case Delete(filter, None, onlyOne) =>            //SingleObservable
          if (onlyOne) coll.deleteOne(filter).toFuture().asInstanceOf[Future[T]]
          else coll.deleteMany(filter).toFuture().asInstanceOf[Future[T]]
        case Delete(filter, Some(opt), onlyOne) =>       //SingleObservable
          if (onlyOne) coll.deleteOne(filter, opt.asInstanceOf[DeleteOptions]).toFuture().asInstanceOf[Future[T]]
          else coll.deleteMany(filter, opt.asInstanceOf[DeleteOptions]).toFuture().asInstanceOf[Future[T]]
        /* replace */
        case Replace(filter, replacement, None) =>        //SingleObservable
          coll.replaceOne(filter, replacement).toFuture().asInstanceOf[Future[T]]
        case Replace(filter, replacement, Some(opt)) =>    //SingleObservable
          coll.replaceOne(filter, replacement, opt.asInstanceOf[UpdateOptions]).toFuture().asInstanceOf[Future[T]]
        /* update */
        case Update(filter, update, None, onlyOne) =>      //SingleObservable
          if (onlyOne) coll.updateOne(filter, update).toFuture().asInstanceOf[Future[T]]
          else coll.updateMany(filter, update).toFuture().asInstanceOf[Future[T]]
        case Update(filter, update, Some(opt), onlyOne) => //SingleObservable
          if (onlyOne) coll.updateOne(filter, update, opt.asInstanceOf[UpdateOptions]).toFuture().asInstanceOf[Future[T]]
          else coll.updateMany(filter, update, opt.asInstanceOf[UpdateOptions]).toFuture().asInstanceOf[Future[T]]
        /* bulkWrite */
        case BulkWrite(commands, None) =>                  //SingleObservable
          coll.bulkWrite(commands).toFuture().asInstanceOf[Future[T]]
        case BulkWrite(commands, Some(opt)) =>             //SingleObservable
          coll.bulkWrite(commands, opt.asInstanceOf[BulkWriteOptions]).toFuture().asInstanceOf[Future[T]]

        /* drop collection */
        case DropCollection(collName) =>                   //SingleObservable
          val coll = db.getCollection(collName)
          coll.drop().toFuture().asInstanceOf[Future[T]]
        /* create collection */
        case CreateCollection(collName, None) =>           //SingleObservable
          db.createCollection(collName).toFuture().asInstanceOf[Future[T]]
        case CreateCollection(collName, Some(opt)) =>      //SingleObservable
          db.createCollection(collName, opt.asInstanceOf[CreateCollectionOptions]).toFuture().asInstanceOf[Future[T]]
        /* list collection */
        case ListCollection(dbName) =>   //ListConllectionObservable
          client.getDatabase(dbName).listCollections().toFuture().asInstanceOf[Future[T]]
        /* create view */
        case CreateView(viewName, viewOn, pline, None) =>       //SingleObservable
          db.createView(viewName, viewOn, pline).toFuture().asInstanceOf[Future[T]]
        case CreateView(viewName, viewOn, pline, Some(opt)) =>  //SingleObservable
          db.createView(viewName, viewOn, pline, opt.asInstanceOf[CreateViewOptions]).toFuture().asInstanceOf[Future[T]]
        /* create index */
        case CreateIndex(key, None) =>                     //SingleObservable
          coll.createIndex(key).toFuture().asInstanceOf[Future[T]]
        case CreateIndex(key, Some(opt)) =>                //SingleObservable
          coll.createIndex(key, opt.asInstanceOf[IndexOptions]).toFuture().asInstanceOf[Future[T]]
        /* drop index */
        case DropIndexByName(indexName, None) =>           //SingleObservable
          coll.dropIndex(indexName).toFuture().asInstanceOf[Future[T]]
        case DropIndexByName(indexName, Some(opt)) =>      //SingleObservable
          coll.dropIndex(indexName, opt.asInstanceOf[DropIndexOptions]).toFuture().asInstanceOf[Future[T]]
        case DropIndexByKey(key, None) =>                  //SingleObservable
          coll.dropIndex(key).toFuture().asInstanceOf[Future[T]]
        case DropIndexByKey(key, Some(opt)) =>             //SingleObservable
          coll.dropIndex(key, opt.asInstanceOf[DropIndexOptions]).toFuture().asInstanceOf[Future[T]]
        case DropAllIndexes(None) =>                       //SingleObservable
          coll.dropIndexes().toFuture().asInstanceOf[Future[T]]
        case DropAllIndexes(Some(opt)) =>                  //SingleObservable
          coll.dropIndexes(opt.asInstanceOf[DropIndexOptions]).toFuture().asInstanceOf[Future[T]]
      }
    }
  */


}


object MongoActionStream {

  import MGOClasses._

  case class StreamingInsert[A](dbName: String,
                                collName: String,
                                converter: A => Document,
                                parallelism: Int = 1
                               ) extends MGOCommands

  case class StreamingDelete[A](dbName: String,
                                collName: String,
                                toFilter: A => Bson,
                                parallelism: Int = 1,
                                justOne: Boolean = false
                               ) extends MGOCommands

  case class StreamingUpdate[A](dbName: String,
                                collName: String,
                                toFilter: A => Bson,
                                toUpdate: A => Bson,
                                parallelism: Int = 1,
                                justOne: Boolean = false
                               ) extends MGOCommands


  case class InsertAction[A](ctx: StreamingInsert[A])(
    implicit mongoClient: MongoClient) {

    val database = mongoClient.getDatabase(ctx.dbName)
    val collection = database.getCollection(ctx.collName)

    def performOnRow(implicit ec: ExecutionContext): Flow[A, Document, NotUsed] =
      Flow[A].map(ctx.converter)
        .mapAsync(ctx.parallelism)(doc => collection.insertOne(doc).toFuture().map(_ => doc))
  }

  case class UpdateAction[A](ctx: StreamingUpdate[A])(
    implicit mongoClient: MongoClient) {

    val database = mongoClient.getDatabase(ctx.dbName)
    val collection = database.getCollection(ctx.collName)

    def performOnRow(implicit ec: ExecutionContext): Flow[A, A, NotUsed] =
      if (ctx.justOne) {
        Flow[A]
          .mapAsync(ctx.parallelism)(a =>
            collection.updateOne(ctx.toFilter(a), ctx.toUpdate(a)).toFuture().map(_ => a))
      } else
        Flow[A]
          .mapAsync(ctx.parallelism)(a =>
            collection.updateMany(ctx.toFilter(a), ctx.toUpdate(a)).toFuture().map(_ => a))
  }


  case class DeleteAction[A](ctx: StreamingDelete[A])(
    implicit mongoClient: MongoClient) {

    val database = mongoClient.getDatabase(ctx.dbName)
    val collection = database.getCollection(ctx.collName)

    def performOnRow(implicit ec: ExecutionContext): Flow[A, A, NotUsed] =
      if (ctx.justOne) {
        Flow[A]
          .mapAsync(ctx.parallelism)(a =>
            collection.deleteOne(ctx.toFilter(a)).toFuture().map(_ => a))
      } else
        Flow[A]
          .mapAsync(ctx.parallelism)(a =>
            collection.deleteMany(ctx.toFilter(a)).toFuture().map(_ => a))
  }

}

object MGOHelpers {

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), 10 seconds)

    def headResult() = Await.result(observable.head(), 10 seconds)

    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }

    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }

  def getResult[T](fut: Future[T], timeOut: Duration = 1 second): T = {
    Await.result(fut, timeOut)
  }

  def getResults[T](fut: Future[Iterable[T]], timeOut: Duration = 1 second): Iterable[T] = {
    Await.result(fut, timeOut)
  }

  import monix.eval.Task
  import monix.execution.Scheduler.Implicits.global

  final class FutureToTask[A](x: => Future[A]) {
    def asTask: Task[A] = Task.deferFuture[A](x)
  }

  final class TaskToFuture[A](x: => Task[A]) {
    def asFuture: Future[A] = x.runAsync
  }

}