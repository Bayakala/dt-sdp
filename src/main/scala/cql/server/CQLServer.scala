package sdp.grpc.cassandra.server

import sdp.logging.LogSupport
import com.datastax.driver.core._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.grpc.Server
import io.grpc.ServerBuilder
import sdp.grpc.services._
import sdp.cql.engine._
import CQLHelpers._

class gRPCServer(server: Server) extends LogSupport {


  def start(): Unit = {
    server.start()
    log.info(s"Server started, listening on ${server.getPort}")
    sys.addShutdownHook {
      // Use stderr here since the logger may has been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      stop()
      System.err.println("*** server shut down")
    }
    ()
  }

  def stop(): Unit = {
    server.shutdown()
  }

  /**
    * Await termination on the main thread since the grpc library uses daemon threads.
    */
  def blockUntilShutdown(): Unit = {
    server.awaitTermination()
  }
}

object CQLServer extends App {
  implicit val cqlsys = ActorSystem("cqlSystem")
  implicit val mat = ActorMaterializer()
  implicit val ec = cqlsys.dispatcher

  val cluster = new Cluster
  .Builder()
    .addContactPoints("localhost")
    .withPort(9042)
    .build()

  useJava8DateTime(cluster)
  implicit val session = cluster.connect()

  val server = new gRPCServer(
    ServerBuilder
      .forPort(50052)
      .addService(
        CqlGrpcAkkaStream.bindService(
          new CQLStreamingServices
        )
      ).build()
  )
  server.start()
  //  server.blockUntilShutdown()
  scala.io.StdIn.readLine()
  session.close()
  cluster.close()
  mat.shutdown()
  cqlsys.terminate()
}