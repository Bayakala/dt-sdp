package sdp.grpc.jdbc.server

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.grpc.Server
import io.grpc.ServerBuilder
import sdp.grpc.services._
import sdp.logging.LogSupport

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

object JDBCServer extends App {
  import sdp.jdbc.config._

  implicit val system = ActorSystem("JDBCServer")
  implicit val mat = ActorMaterializer.create(system)
  implicit val ec = system.dispatcher

  ConfigDBsWithEnv("dev").setup('h2)
  ConfigDBsWithEnv("dev").loadGlobalSettings()

  val server = new gRPCServer(
    ServerBuilder
      .forPort(50053)
      .addService(
        JdbcGrpcAkkaStream.bindService(
          new JDBCStreamingServices
        )
      ).build()
  )
  server.start()
  //  server.blockUntilShutdown()
  scala.io.StdIn.readLine()
  ConfigDBsWithEnv("dev").close('h2)
  mat.shutdown()
  system.terminate()


}
