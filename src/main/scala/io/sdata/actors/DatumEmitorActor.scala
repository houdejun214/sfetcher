package io.sdata.actors

import akka.actor.Actor
import com.google.inject.{Injector, Inject}
import io.sdata.core.CrawlContext
import io.sdata.modules.ActorInject

/**
 * Created by dejun on 10/2/16.
 */
object DatumEmitorActor{

}
class DatumEmitorActor @Inject()(inject: Injector,
                                 crawlContext:CrawlContext
                                  ) extends Actor with ActorInject{
  def injector: Injector = inject

  override def receive: Receive = {
    case _=>Unit
  }
}
