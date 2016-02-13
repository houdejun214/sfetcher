package io.sdata.modules

import akka.actor._
import com.google.inject.Injector

import scala.reflect.ClassTag

/**
 * Created by dejun on 9/2/16.
 */
trait ActorInject {
  protected def injector: Injector

  protected def injectActor[A <: Actor](implicit factory: ActorRefFactory, tag: ClassTag[A]): ActorRef =
    factory.actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass))

  protected def injectActor[A <: Actor](name: String)(implicit factory: ActorRefFactory, tag: ClassTag[A]): ActorRef =
    factory.actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass), name)

  //  protected def injectActor(create: => Actor)(implicit factory: ActorRefFactory): ActorRef =
  //    factory.actorOf(Props(create))
  //
  //  protected def injectActor(create: => Actor, name: String)(implicit factory: ActorRefFactory): ActorRef =
  //    factory.actorOf(Props(create), name)

  protected def injectTopActor[A <: Actor](implicit tag: ClassTag[A]): ActorRef =
    injector.getInstance(classOf[ActorSystem]).actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass))

  protected def injectTopActor[A <: Actor](name: String)(implicit tag: ClassTag[A]): ActorRef =
    injector.getInstance(classOf[ActorSystem]).actorOf(Props(classOf[GuiceTypedActorProducer[A]], injector, tag.runtimeClass), name)
}
