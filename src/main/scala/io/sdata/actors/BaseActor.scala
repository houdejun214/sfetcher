package io.sdata.actors

import akka.actor.Actor
import akka.event.Logging
import com.google.inject.Injector

/**
 * Created by dejun on 10/2/16.
 */
abstract class BaseActor(inject: Injector) extends Actor {

  protected def injector: Injector = inject

  protected val log = Logging(context.system, this)

  override def preStart() = {
    log.debug("Starting")
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
  }
}
