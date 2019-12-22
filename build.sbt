import scalapb.compiler.Version.scalapbVersion
import scalapb.compiler.Version.grpcJavaVersion

name := "dt-sdp"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += Resolver.bintrayRepo("beyondthelines", "maven")

scalacOptions += "-Ypartial-unification"

libraryDependencies := Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
  "io.grpc" % "grpc-netty" % grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,
  // for GRPC Akkastream
  "beyondthelines"         %% "grpcakkastreamruntime" % "0.0.5",
  // for scalikejdbc
  "org.scalikejdbc" %% "scalikejdbc"       % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-test"   % "3.2.1"   % "test",
  "org.scalikejdbc" %% "scalikejdbc-config"  % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-streams" % "3.2.1",
  "org.scalikejdbc" %% "scalikejdbc-joda-time" % "3.2.1",
  "com.h2database"  %  "h2"                % "1.4.199",
  "mysql" % "mysql-connector-java" % "6.0.6",
  "org.postgresql" % "postgresql" % "42.2.0",
  "commons-dbcp" % "commons-dbcp" % "1.4",
  "org.apache.tomcat" % "tomcat-jdbc" % "9.0.2",
  "com.zaxxer" % "HikariCP" % "2.7.4",
  "com.jolbox" % "bonecp" % "0.8.0.RELEASE",
  "com.typesafe.slick" %% "slick" % "3.3.2",
  //for cassandra 3.4.0
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0",
  "com.datastax.cassandra" % "cassandra-driver-extras" % "3.6.0",
  "com.typesafe.akka" %% "akka-stream" % "2.5.13",
  "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "1.0.1",
  //for mongodb 4.0
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "0.20",
  //other dependencies
  "co.fs2" %% "fs2-core" % "0.9.7",
  "ch.qos.logback"  %  "logback-classic"   % "1.2.3",
  "org.typelevel" %% "cats-core" % "0.9.0",
  "io.monix" %% "monix-execution" % "3.0.0-RC1",
  "io.monix" %% "monix-eval" % "3.0.0-RC1"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value,
  // generate the akka stream files
  grpc.akkastreams.generators.GrpcAkkaStreamGenerator() -> (sourceManaged in Compile).value
)
