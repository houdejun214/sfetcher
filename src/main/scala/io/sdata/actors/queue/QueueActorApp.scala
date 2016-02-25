package io.sdata.actors.queue

import akka.actor.{ActorRef, Props, ActorSystem}

/**
  * Created by dejun on 20/2/16.
  */
object QueueActorApp extends App{

  val system = ActorSystem("QueueActorApp")
  val ref: ActorRef = system.actorOf(Props.create(classOf[QueueActor]))
  ref ! SendMessage("test1")
//  ref ! SendMessage("test2")
//  ref ! SendMessage("test3")
//  ref ! SendMessage("test4")
//  ref ! SendMessage("test5")

}
