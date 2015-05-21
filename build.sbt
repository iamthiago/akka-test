name := "akka-test"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.10",
  "com.typesafe.akka" % "akka-slf4j_2.11" % "2.3.10",
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.1"
)