package com.akka.test.basic

import akka.actor.{Actor, ActorSystem, Props}

/**
 * Created by thiago on 5/15/15.
 */
class HelloActor(msg: String) extends Actor {
  override def receive: Receive = {
    case _ => println(msg)
  }
}

object Main extends App {
  val system = ActorSystem("HelloSystem")
  val helloActor = system.actorOf(Props(new HelloActor("Bla")), name = "helloactor")
  helloActor ! "hello"
  helloActor ! "world"
}