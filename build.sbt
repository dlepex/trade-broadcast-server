name := "trade-broadcast-server"

version := "1.0"

scalaVersion := "2.12.1"
 
val akkaVersion = "2.4.16"
val nettyVersion = "4.1.8.Final"
val jacksonVersion = "2.8.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,


  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7", // log4j2 is better


  "io.netty" % "netty-all" % nettyVersion, // we don't need all for just tcp

  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,

  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test")
