package com.sfetcher.modules

import akka.actor._
import com.google.inject.Injector

import scala.reflect.ClassTag

/**
 * Created by dejun on 9/2/16.
 */
trait ActorInject {
  protected def injector: Injector

  protected def injectActor[A <: Actor](implicit factory: ActorRefFactory,
                                        tag: ClassTag[A]): ActorRef =
    factory.actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass))


  protected def injectActor[A <: Actor](dispatcher: String)(implicit factory: ActorRefFactory,
                                                                tag: ClassTag[A]): ActorRef =
    factory.actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass)
      .withDispatcher(dispatcher))
}
