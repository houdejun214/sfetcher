package io.sdata.modules

import akka.actor.{IndirectActorProducer, Actor}
import com.google.inject.Injector


class GuiceTypedActorProducer[A <: Actor](injector: Injector, clazz: Class[A]) extends IndirectActorProducer {
  def actorClass = clazz
  def produce() = injector.getBinding(clazz).getProvider.get()
}