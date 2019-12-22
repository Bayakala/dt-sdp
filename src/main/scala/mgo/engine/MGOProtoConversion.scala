package sdp.mongo.engine
import org.mongodb.scala.bson.collection.immutable.Document
import org.bson.conversions.Bson
import sdp.grpc.services._
import protobuf.bytes.Converter._
import MGOClasses._
import MGOAdmins._
import MGOCommands._
import org.bson.BsonDocument
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.FindObservable

object MGOProtoConversion {

  type MGO_COMMAND_TYPE = Int
  val MGO_COMMAND_FIND            = 0
  val MGO_COMMAND_COUNT           = 20
  val MGO_COMMAND_DISTICT         = 21
  val MGO_COMMAND_DOCUMENTSTREAM  = 1
  val MGO_COMMAND_AGGREGATE       = 2
  val MGO_COMMAND_INSERT          = 3
  val MGO_COMMAND_DELETE          = 4
  val MGO_COMMAND_REPLACE         = 5
  val MGO_COMMAND_UPDATE          = 6


  val MGO_ADMIN_DROPCOLLECTION    = 8
  val MGO_ADMIN_CREATECOLLECTION  = 9
  val MGO_ADMIN_LISTCOLLECTION    = 10
  val MGO_ADMIN_CREATEVIEW        = 11
  val MGO_ADMIN_CREATEINDEX       = 12
  val MGO_ADMIN_DROPINDEXBYNAME   = 13
  val MGO_ADMIN_DROPINDEXBYKEY    = 14
  val MGO_ADMIN_DROPALLINDEXES    = 15


  case class AdminContext(
                           tarName: String = "",
                           bsonParam: Seq[Bson] = Nil,
                           options: Option[Any] = None,
                           objName: String = ""
                         ){
    def toProto = sdp.grpc.services.ProtoMGOAdmin(
      tarName = this.tarName,
      bsonParam = this.bsonParam.map {b => sdp.grpc.services.ProtoMGOBson(marshal(b))},
      objName = this.objName,
      options = this.options.map(b => ProtoAny(marshal(b)))

    )
  }

  object AdminContext {
    def fromProto(msg: sdp.grpc.services.ProtoMGOAdmin) = new AdminContext(
      tarName = msg.tarName,
      bsonParam = msg.bsonParam.map(b => unmarshal[Bson](b.bson)),
      objName = msg.objName,
      options = msg.options.map(b => unmarshal[Any](b.value))
    )
  }

  case class Context(
                      dbName: String = "",
                      collName: String = "",
                      commandType: MGO_COMMAND_TYPE,
                      bsonParam: Seq[Bson] = Nil,
                      resultOptions: Seq[ResultOptions] = Nil,
                      options: Option[Any] = None,
                      documents: Seq[Document] = Nil,
                      targets: Seq[String] = Nil,
                      only: Boolean = false,
                      adminOptions: Option[AdminContext] = None
                    ){

    def toProto = new sdp.grpc.services.ProtoMGOContext(
      dbName = this.dbName,
      collName = this.collName,
      commandType = this.commandType,
      bsonParam = this.bsonParam.map(bsonToProto),
      resultOptions = this.resultOptions.map(_.toProto),
      options = { if(this.options == None)
        None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
      else
        Some(ProtoAny(marshal(this.options.get))) },
      documents = this.documents.map(d => sdp.grpc.services.ProtoMGODocument(marshal(d))),
      targets = this.targets,
      only = Some(this.only),
      adminOptions = this.adminOptions.map(_.toProto)
    )

  }

  object MGODocument {
    def fromProto(msg: sdp.grpc.services.ProtoMGODocument): Document =
      unmarshal[Document](msg.document)
    def toProto(doc: Document): sdp.grpc.services.ProtoMGODocument =
      new ProtoMGODocument(marshal(doc))
  }

  object MGOProtoMsg {
    def fromProto(msg: sdp.grpc.services.ProtoMGOContext) = new Context(
      dbName = msg.dbName,
      collName = msg.collName,
      commandType = msg.commandType,
      bsonParam = msg.bsonParam.map(protoToBson),
      resultOptions = msg.resultOptions.map(r => ResultOptions.fromProto(r)),
      options = msg.options.map(a => unmarshal[Any](a.value)),
      documents = msg.documents.map(doc => unmarshal[Document](doc.document)),
      targets = msg.targets,
      adminOptions = msg.adminOptions.map(ado => AdminContext.fromProto(ado))
    )
  }

  def bsonToProto(bson: Bson) =
    ProtoMGOBson(marshal(bson.toBsonDocument(
      classOf[org.mongodb.scala.bson.collection.immutable.Document],DEFAULT_CODEC_REGISTRY)))

  def protoToBson(proto: ProtoMGOBson): Bson = new Bson {
    val bsdoc = unmarshal[BsonDocument](proto.bson)
    override def toBsonDocument[TDocument](documentClass: Class[TDocument], codecRegistry: CodecRegistry): BsonDocument = bsdoc
  }

  def ctxFromProto(proto: ProtoMGOContext): MGOContext = proto.commandType match {
    case MGO_COMMAND_FIND => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_QUERY,
        action = Some(Find())
      )
      def toResultOption(rts: Seq[ProtoMGOResultOption]): FindObservable[Document] => FindObservable[Document] = findObj =>
        rts.foldRight(findObj)((a,b) => ResultOptions.fromProto(a).toFindObservable(b))

      (proto.bsonParam, proto.resultOptions, proto.only) match {
        case (Nil, Nil, None) => ctx
        case (Nil, Nil, Some(b)) => ctx.setCommand(Find(firstOnly = b))
        case (bp,Nil,None) => ctx.setCommand(
          Find(filter = Some(protoToBson(bp.head))))
        case (bp,Nil,Some(b)) => ctx.setCommand(
          Find(filter = Some(protoToBson(bp.head)), firstOnly = b))
        case (bp,fo,None) => {
          ctx.setCommand(
            Find(filter = Some(protoToBson(bp.head)),
              andThen = fo.map(ResultOptions.fromProto)
            ))
        }
        case (bp,fo,Some(b)) => {
          ctx.setCommand(
            Find(filter = Some(protoToBson(bp.head)),
              andThen = fo.map(ResultOptions.fromProto),
              firstOnly = b))
        }
        case _ => ctx
      }
    }
    case MGO_COMMAND_COUNT => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_QUERY,
        action = Some(Count())
      )
      (proto.bsonParam, proto.options) match {
        case (Nil, None) => ctx
        case (bp, None) => ctx.setCommand(
          Count(filter = Some(protoToBson(bp.head)))
        )
        case (Nil,Some(o)) => ctx.setCommand(
          Count(options = Some(unmarshal[Any](o.value)))
        )
        case _ => ctx
      }
    }
    case MGO_COMMAND_DISTICT => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_QUERY,
        action = Some(Distict(fieldName = proto.targets.head))
      )
      (proto.bsonParam) match {
        case Nil => ctx
        case bp: Seq[ProtoMGOBson] => ctx.setCommand(
          Distict(fieldName = proto.targets.head,filter = Some(protoToBson(bp.head)))
        )
        case _ => ctx
      }
    }
    case MGO_COMMAND_AGGREGATE => {
      new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_QUERY,
        action = Some(Aggregate(proto.bsonParam.map(p => protoToBson(p))))
      )
    }
    case MGO_ADMIN_LISTCOLLECTION => {
      new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_QUERY,
        action = Some(ListCollection(proto.dbName)))
    }
    case MGO_COMMAND_INSERT => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_UPDATE,
        action = Some(Insert(
          newdocs = proto.documents.map(doc => unmarshal[Document](doc.document))))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(Insert(
          newdocs = proto.documents.map(doc => unmarshal[Document](doc.document)),
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_COMMAND_DELETE => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_UPDATE,
        action = Some(Delete(
          filter = protoToBson(proto.bsonParam.head)))
      )
      (proto.options, proto.only) match {
        case (None,None) => ctx
        case (None,Some(b)) => ctx.setCommand(Delete(
          filter = protoToBson(proto.bsonParam.head),
          onlyOne = b))
        case (Some(o),None) => ctx.setCommand(Delete(
          filter = protoToBson(proto.bsonParam.head),
          options = Some(unmarshal[Any](o.value)))
        )
        case (Some(o),Some(b)) => ctx.setCommand(Delete(
          filter = protoToBson(proto.bsonParam.head),
          options = Some(unmarshal[Any](o.value)),
          onlyOne = b)
        )
      }
    }
    case MGO_COMMAND_REPLACE => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_UPDATE,
        action = Some(Replace(
          filter = protoToBson(proto.bsonParam.head),
          replacement = unmarshal[Document](proto.documents.head.document)))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(Replace(
          filter = protoToBson(proto.bsonParam.head),
          replacement = unmarshal[Document](proto.documents.head.document),
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_COMMAND_UPDATE => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_UPDATE,
        action = Some(Update(
          filter = protoToBson(proto.bsonParam.head),
          update = protoToBson(proto.bsonParam.tail.head)))
      )
      (proto.options, proto.only) match {
        case (None,None) => ctx
        case (None,Some(b)) => ctx.setCommand(Update(
          filter = protoToBson(proto.bsonParam.head),
          update = protoToBson(proto.bsonParam.tail.head),
          onlyOne = b))
        case (Some(o),None) => ctx.setCommand(Update(
          filter = protoToBson(proto.bsonParam.head),
          update = protoToBson(proto.bsonParam.tail.head),
          options = Some(unmarshal[Any](o.value)))
        )
        case (Some(o),Some(b)) => ctx.setCommand(Update(
          filter = protoToBson(proto.bsonParam.head),
          update = protoToBson(proto.bsonParam.tail.head),
          options = Some(unmarshal[Any](o.value)),
          onlyOne = b)
        )
      }
    }
    case MGO_ADMIN_DROPCOLLECTION =>
      new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(DropCollection(proto.collName))
      )
    case MGO_ADMIN_CREATECOLLECTION => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(CreateCollection(proto.collName))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(CreateCollection(proto.collName,
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_ADMIN_CREATEVIEW => {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(CreateView(viewName = proto.targets.head,
          viewOn = proto.targets.tail.head,
          pipeline = proto.bsonParam.map(p => protoToBson(p))))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(CreateView(viewName = proto.targets.head,
          viewOn = proto.targets.tail.head,
          pipeline = proto.bsonParam.map(p => protoToBson(p)),
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_ADMIN_CREATEINDEX=> {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(CreateIndex(key = protoToBson(proto.bsonParam.head)))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(CreateIndex(key = protoToBson(proto.bsonParam.head),
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_ADMIN_DROPINDEXBYNAME=> {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(DropIndexByName(indexName = proto.targets.head))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(DropIndexByName(indexName = proto.targets.head,
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_ADMIN_DROPINDEXBYKEY=> {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(DropIndexByKey(key = protoToBson(proto.bsonParam.head)))
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(DropIndexByKey(key = protoToBson(proto.bsonParam.head),
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }
    case MGO_ADMIN_DROPALLINDEXES=> {
      var ctx = new MGOContext(
        dbName = proto.dbName,
        collName = proto.collName,
        actionType = MGO_ADMIN,
        action = Some(DropAllIndexes())
      )
      proto.options match {
        case None => ctx
        case Some(o) => ctx.setCommand(DropAllIndexes(
          options = Some(unmarshal[Any](o.value)))
        )
      }
    }

  }

  def ctxToProto(ctx: MGOContext): Option[sdp.grpc.services.ProtoMGOContext] = ctx.action match {
    case None => None
    case Some(act) => act match {
      case Count(filter, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_COUNT,
          bsonParam = { if (filter == None) Seq.empty[ProtoMGOBson]
          else Seq(bsonToProto(filter.get))},
          options = { if(options == None) None  //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))
      case Distict(fieldName, filter) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_DISTICT,
          bsonParam = { if (filter == None) Seq.empty[ProtoMGOBson]
          else Seq(bsonToProto(filter.get))},
          targets = Seq(fieldName)

        ))

      case Find(filter, andThen, firstOnly) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_FIND,
          bsonParam = { if (filter == None) Seq.empty[ProtoMGOBson]
          else Seq(bsonToProto(filter.get))},
          resultOptions = andThen.map(_.toProto)
        ))

      case Aggregate(pipeLine) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_AGGREGATE,
          bsonParam = pipeLine.map(bsonToProto)
        ))

      case Insert(newdocs, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_INSERT,
          documents = newdocs.map(d => ProtoMGODocument(marshal(d))),
          options = { if(options == None) None      //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))

      case Delete(filter, options, onlyOne) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_DELETE,
          bsonParam = Seq(bsonToProto(filter)),
          options = { if(options == None) None  //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) },
          only = Some(onlyOne)
        ))

      case Replace(filter, replacement, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_REPLACE,
          bsonParam = Seq(bsonToProto(filter)),
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) },
          documents = Seq(ProtoMGODocument(marshal(replacement)))
        ))

      case Update(filter, update, options, onlyOne) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_COMMAND_UPDATE,
          bsonParam = Seq(bsonToProto(filter),bsonToProto(update)),
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) },
          only = Some(onlyOne)
        ))


      case DropCollection(coll) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = coll,
          commandType = MGO_ADMIN_DROPCOLLECTION
        ))

      case CreateCollection(coll, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = coll,
          commandType = MGO_ADMIN_CREATECOLLECTION,
          options = { if(options == None) None  //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))

      case ListCollection(dbName) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          commandType = MGO_ADMIN_LISTCOLLECTION
        ))

      case CreateView(viewName, viewOn, pipeline, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_ADMIN_CREATEVIEW,
          bsonParam = pipeline.map(bsonToProto),
          options = { if(options == None) None  //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) },
          targets = Seq(viewName,viewOn)
        ))

      case CreateIndex(key, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_ADMIN_CREATEINDEX,
          bsonParam = Seq(bsonToProto(key)),
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))


      case DropIndexByName(indexName, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_ADMIN_DROPINDEXBYNAME,
          targets = Seq(indexName),
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))

      case DropIndexByKey(key, options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_ADMIN_DROPINDEXBYKEY,
          bsonParam = Seq(bsonToProto(key)),
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))


      case DropAllIndexes(options) =>
        Some(new sdp.grpc.services.ProtoMGOContext(
          dbName = ctx.dbName,
          collName = ctx.collName,
          commandType = MGO_ADMIN_DROPALLINDEXES,
          options = { if(options == None) None //Some(ProtoAny(com.google.protobuf.ByteString.EMPTY))
          else Some(ProtoAny(marshal(options.get))) }
        ))

    }
  }

}