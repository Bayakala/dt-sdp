package sdp.grpc.cassandra.server

import akka.NotUsed
import akka.stream.scaladsl._

import protobuf.bytes.Converter._
import com.datastax.driver.core._

import scala.concurrent.ExecutionContextExecutor
import sdp.grpc.services._
import sdp.cql.engine._
import CQLEngine._
import CQLHelpers._
import sdp.logging.LogSupport
import scala.concurrent._
import scala.concurrent.duration._
import akka.stream.ActorMaterializer
import common._


class CQLStreamingServices(implicit ec: ExecutionContextExecutor,
                           mat: ActorMaterializer,  session: Session)
  extends CqlGrpcAkkaStream.CQLServices with LogSupport{

  override def greeting: Flow[HelloMsg, HelloMsg, NotUsed] = {
    Flow[HelloMsg]
      .map {r => HelloMsg(r.hello + " cassandra ...")}
  }

}