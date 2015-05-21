package com.akka.test.kafka

import java.util.Properties
import java.util.concurrent.CountDownLatch

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import akka.routing.RoundRobinPool
import kafka.common.{FailedToSendMessageException, NoBrokersForPartitionException}
import kafka.producer.{KeyedMessage, Producer, ProducerClosedException, ProducerConfig}
import org.apache.kafka.common.KafkaException

import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by thiago on 5/15/15.
 */
object Test extends App {

  val latch = new CountDownLatch(10)

  class KafkaActor extends Actor {

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
        case _: ActorInitializationException => Stop
        case _: FailedToSendMessageException => Restart
        case _: ProducerClosedException => Restart
        case _: NoBrokersForPartitionException => Escalate
        case _: KafkaException => Escalate
        case _: Exception => Escalate
      }

    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("request.required.acks", "-1")
    props.put("producer.type", "sync")
    props.put("compression.codec", "gzip")

    private val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

    override def postStop(): Unit = producer.close()

    val random = new Random()

    def receive = {
      case x: Any =>
        /*val startTime = System.currentTimeMillis()*/
        val bytes = Array.fill[Byte](100000)((random.nextInt() % 26 + 'a').toByte)

        for (i <- 1 to 100) {
          producer.send(new KeyedMessage[String, Array[Byte]]("my-topic", bytes))
        }

        latch.countDown()

        /*val finishTime = System.currentTimeMillis()
        val millis = Duration.create(finishTime - startTime, "millis")
        println("time in " + millis)
        println("time in " + millis.toSeconds + " seconds")*/
    }
  }

  val system = ActorSystem("KafkaSystem")

  val kafkaActor = system.actorOf(Props[KafkaActor].withRouter(RoundRobinPool(4)), name = "helloactor")

  val startTime = System.currentTimeMillis()

  for (i <- 1 to 10) {
    kafkaActor ! "bla"
  }

  latch.await()

  val finishTime = System.currentTimeMillis()
  val millis = Duration.create(finishTime - startTime, "millis")
  println("time in " + millis)
  println("time in " + millis.toSeconds + " seconds")

}