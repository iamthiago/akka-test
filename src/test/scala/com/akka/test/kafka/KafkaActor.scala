package com.akka.test.kafka

import java.util.Properties

import akka.actor.SupervisorStrategy.{Escalate, Restart, Stop}
import akka.actor._
import kafka.common.{FailedToSendMessageException, NoBrokersForPartitionException}
import kafka.producer.{KeyedMessage, Producer, ProducerClosedException, ProducerConfig}
import org.apache.kafka.common.KafkaException

import scala.concurrent.duration._

/**
 * Created by thiago on 5/15/15.
 */
class KafkaActor(config: ProducerConfig) extends Actor {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: ActorInitializationException => Stop
      case _: FailedToSendMessageException => Restart
      case _: ProducerClosedException => Restart
      case _: NoBrokersForPartitionException => Escalate
      case _: KafkaException => Escalate
      case _: Exception => Escalate
    }

  private val producer = new Producer[String, String](config)

  override def postStop(): Unit = producer.close()

  def receive = {
    case x: Any => producer.send(new KeyedMessage[String, String]("test", "thiago"))
  }
}

object Test extends App {
  val props = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")

  val system = ActorSystem("KafkaSystem")
  val kafkaActor = system.actorOf(Props(new KafkaActor(new ProducerConfig(props))), name = "helloactor")

  for (i <- 1 to 10) {
    kafkaActor ! "bla"
  }
}