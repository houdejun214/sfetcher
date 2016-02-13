package io.sdata

import akka.actor.ActorSystem
import com.google.inject.Guice
import io.sdata.modules.AkkaModule
import net.codingwell.scalaguice.InjectorExtensions._

/**
 * Created by dejun on 3/2/16.
 */
trait AbstractApp extends App {

  override def delayedInit(body: => Unit) = {

    super.delayedInit({
      val injector = Guice.createInjector(
        new AkkaModule()
      )
      val system = injector.instance[ActorSystem]

      //val counter = system.actorOf(GuiceAkkaExtension(system).props(CountingActor.name))

    })
    // evaluates the initialization code of C
    super.delayedInit(body)
  }

}
