package sdp.grpc.mongo.server

import sdp.mongo.engine._
import MGOClasses._
import MGOEngine._
import akka.NotUsed
import akka.stream.scaladsl.Flow
import sdp.logging.LogSupport
import sdp.grpc.services._
import org.mongodb.scala._
import scala.concurrent._
import akka.stream.ActorMaterializer
import sdp.mongo.engine.MGOProtoConversion.MGODocument
import common._


class MGOStreamingServices(implicit ec: ExecutionContextExecutor,
                            mat: ActorMaterializer, client: MongoClient)
  extends MgoGrpcAkkaStream.MGOServices with LogSupport {
  override def greeting: Flow[HelloMsg, HelloMsg, NotUsed] = {
    Flow[HelloMsg]
      .map {r => HelloMsg(r.hello+", mongo ...")}
  }
}

