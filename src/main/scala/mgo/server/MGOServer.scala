package sdp.grpc.mongo.server


import io.grpc.Server
import io.grpc.ServerBuilder
import sdp.grpc.services._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import sdp.logging.LogSupport
import org.mongodb.scala._
import scala.collection.JavaConverters._

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

object MGOServer extends App {
  implicit val mgosys = ActorSystem("mgoSystem")
  implicit val mat = ActorMaterializer()
  implicit val ec = mgosys.dispatcher


  val clientSettings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings {b =>
      b.hosts(List(new ServerAddress("localhost:27017")).asJava)
    }.build()

  implicit val client: MongoClient = MongoClient(clientSettings)

  val server = new gRPCServer(
    ServerBuilder
      .forPort(50051)
      .addService(
        MgoGrpcAkkaStream.bindService(
          new MGOStreamingServices
        )
      ).build()
  )
  server.start()
  //  server.blockUntilShutdown()
  scala.io.StdIn.readLine()
  mat.shutdown()
  mgosys.terminate()
}
