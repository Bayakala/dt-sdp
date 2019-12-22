package sdp.grpc.jdbc.client

import sdp.grpc.services._
import sdp.logging.LogSupport
import io.grpc._
import common._
import sdp.grpc.services._
import akka.stream.scaladsl._
import akka.NotUsed

class JDBCClient extends LogSupport {

  val channel = ManagedChannelBuilder
    .forAddress("localhost", 50053)
    .usePlaintext()
    .build()

  val stub = JdbcGrpcAkkaStream.stub(channel)

  def sayHello: Source[HelloMsg, NotUsed] = {
    val row = HelloMsg("hello ")
    val rows = List.fill[HelloMsg](100)(row)
    Source
      .fromIterator(() => rows.iterator)
      .via(stub.greeting)
  }
}