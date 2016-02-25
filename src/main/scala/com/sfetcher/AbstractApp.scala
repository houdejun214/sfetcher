package com.sfetcher

import akka.actor.ActorSystem
import com.google.inject.Guice
import com.sfetcher.modules.AkkaModule

/**
 * Created by dejun on 3/2/16.
 */
trait AbstractApp extends App {

  import net.codingwell.scalaguice.InjectorExtensions._

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