package com.akka.test.kafka

import java.util.Properties
import java.util.concurrent.CountDownLatch

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import akka.event.Logging
import akka.routing.{FromConfig, RoundRobinPool}
import kafka.common.{FailedToSendMessageException, NoBrokersForPartitionException}
import kafka.producer.{KeyedMessage, Producer, ProducerClosedException, ProducerConfig}
import org.apache.kafka.common.KafkaException

import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by thiago on 5/15/15.
 */
case class Message(msg: Array[Byte])

object Test extends App {

  val latch = new CountDownLatch(10)

  class KafkaActor extends Actor {

    val log = Logging(context.system, this)

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
        case _: ActorInitializationException => Stop
        case _: FailedToSendMessageException => Restart
        case _: ProducerClosedException => Restart
        case _: NoBrokersForPartitionException => Escalate
        case _: KafkaException => Escalate
        case _: Exception => Escalate
      }

    override def postStop(): Unit = producer.close()

    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("request.required.acks", "-1")
    props.put("producer.type", "sync")
    props.put("compression.codec", "gzip")

    private val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

    def receive = {
      case Message(msg) =>
        for (i <- 1 to 100) producer.send(new KeyedMessage[String, Array[Byte]]("my-topic", msg))
        latch.countDown()

      case _ => log.error("Got a msg that I don't understand")
    }
  }

  val system = ActorSystem("KafkaSystem")
  val kafkaActor = system.actorOf(Props[KafkaActor].withRouter(FromConfig()), name = "kafka-actor")

  val startTime = System.currentTimeMillis()
  val random = new Random()

  for (i <- 1 to 10) kafkaActor ! Message(Array.fill[Byte](100000)((random.nextInt() % 26 + 'a').toByte))

  latch.await()

  val finishTime = System.currentTimeMillis()
  val millis = Duration.create(finishTime - startTime, "millis")

  println("time in " + millis)
  println("time in " + millis.toSeconds + " seconds")
}