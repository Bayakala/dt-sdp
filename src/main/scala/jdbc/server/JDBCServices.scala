package sdp.grpc.jdbc.server

import akka.NotUsed
import akka.stream.scaladsl.{Source,Flow}
import sdp.grpc.services._
import sdp.jdbc.engine._
import JDBCEngine._
import scalikejdbc.WrappedResultSet
import scala.concurrent.ExecutionContextExecutor
import sdp.logging.LogSupport
import common._
import scala.concurrent._

import sdp.grpc.cassandra.client._

class JDBCStreamingServices(implicit ec: ExecutionContextExecutor)
       extends JdbcGrpcAkkaStream.JDBCServices with LogSupport {

  override def greeting: Flow[HelloMsg, HelloMsg, NotUsed] = {
    Flow[HelloMsg]
      .mapAsyncUnordered(4) {r =>
        Future {

          HelloMsg(r.hello + " cassandra ...")
        }}
  }
}

